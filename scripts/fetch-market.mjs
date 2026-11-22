#!/usr/bin/env node
/**
 * GoldPlan 行情抓取脚本。
 *
 * 只抓行情，不读取、也不写入任何持仓数量或金额：
 * - 场外基金：天天基金（东方财富）最近一期已公布净值
 * - 场内 ETF：东方财富实时/收盘行情
 *
 * 输出：
 * - data/latest.json          每个标的最新一条行情
 * - data/history/YYYY-MM.json 当月按「标的 + 日期」去重的历史行情
 *
 * 任一数据源连续失败时会重试；所有标的都失败时以非 0 退出，workflow 不会提交空文件。
 */

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..')
const DATA_DIR = join(ROOT, 'data')
const HISTORY_DIR = join(DATA_DIR, 'history')

const TIMEOUT_MS = 20_000
const ATTEMPTS = 3
const CONCURRENCY = 4

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'

/** 场外基金：代码 + 名称（名称仅用于可读性，不含任何金额） */
const OTC_FUNDS = [
  ['019005', '国投瑞银白银期货(LOF)C'],
  ['001595', '天弘中证银行ETF联接C'],
  ['010770', '天弘中证农业主题ETF联接C'],
  ['015896', '天弘中证细分化工产业主题ETF联接A'],
  ['001593', '天弘创业板ETF联接C'],
  ['006479', '广发纳斯达克100ETF联接(QDII)C'],
  ['009267', '广发双债添利债券E'],
  ['005875', '易方达中盘成长混合'],
  ['006327', '易方达中概互联网ETF联接(QDII)A(人民币份额)'],
  ['161032', '富国中证煤炭指数(LOF)A'],
  ['024689', '富国国证通用航空产业ETF联接C'],
  ['004010', '华泰柏瑞鼎利灵活配置混合A'],
  ['009644', '东方阿尔法优势产业混合A'],
  ['501312', '华宝海外科技股票(QDII-FOF-LOF)A'],
  ['217017', '招商上证消费80ETF联接A'],
  ['012414', '招商中证白酒指数C'],
  ['002611', '博时黄金ETF联接C'],
  ['166002', '中欧新蓝筹灵活配置混合A'],
  ['003095', '中欧医疗健康混合A'],
  ['020483', '中欧中证芯片产业指数C'],
  ['021200', '华夏中证装备产业ETF联接A'],
  ['008088', '华夏房地产ETF联接A'],
  ['019431', '永赢睿信混合A'],
  ['018075', '长盛航天海工装备灵活配置混合C'],
  ['018167', '国泰有色金属矿业ETF联接A'],
  ['013841', '银华集成电路混合C'],
]

/**
 * 场内 ETF：真实代码 + 东方财富 secid。
 * aliases 用于兼容看板种子里的旧占位代码。
 */
const ETFS = [
  {
    symbol: '159612',
    name: '标普500ETF国泰',
    secid: '0.159612',
    aliases: ['ETF-SP500'],
  },
  {
    symbol: '159766',
    name: '旅游ETF富国',
    secid: '0.159766',
    aliases: ['ETF-TOUR'],
  },
]

/** 基准：沪深 300ETF，只用于收益趋势对比，不参与持仓匹配 */
const BENCHMARKS = [
  {
    symbol: '510300',
    name: '沪深300ETF华泰柏瑞',
    secid: '1.510300',
    benchmark: true,
  },
]

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function fetchJson(url, headers = {}) {
  let lastError = null

  for (let attempt = 1; attempt <= ATTEMPTS; attempt += 1) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS)
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'User-Agent': UA,
          Accept: 'application/json,text/plain,*/*',
          'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
          ...headers,
        },
      })

      if (!response.ok) {
        const body = await response.text().catch(() => '')
        const error = new Error(`HTTP ${response.status} ${body.slice(0, 100)}`)
        error.retryable =
          response.status === 408 || response.status === 429 || response.status === 403 || response.status >= 500
        throw error
      }

      const text = await response.text()
      if (/^\s*</.test(text)) throw new Error('返回了 HTML 而不是 JSON（可能被拦截）')
      return JSON.parse(text)
    } catch (error) {
      lastError = error
      const retryable =
        error?.name === 'AbortError' ||
        (typeof error?.retryable === 'boolean' ? error.retryable : true)
      if (!retryable || attempt === ATTEMPTS) break
      await sleep(800 * 2 ** (attempt - 1) + Math.floor(Math.random() * 400))
    } finally {
      clearTimeout(timer)
    }
  }

  throw lastError ?? new Error('请求失败')
}

function beijingDate(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date)
}

function round(value, digits = 4) {
  const factor = 10 ** digits
  return Math.round(value * factor) / factor
}

async function fetchFund([symbol, name]) {
  const url = `https://api.fund.eastmoney.com/f10/lsjz?fundCode=${symbol}&pageIndex=1&pageSize=2`
  const data = await fetchJson(url, { Referer: 'https://fundf10.eastmoney.com/' })
  const list = data?.Data?.LSJZList
  const latest = Array.isArray(list) ? list[0] : null
  if (!latest) throw new Error('没有返回净值记录')

  const date = String(latest.FSRQ ?? '')
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`净值日期异常：${date || '空'}`)

  const price = Number(latest.DWJZ)
  if (!Number.isFinite(price) || price <= 0) throw new Error(`净值异常：${latest.DWJZ}`)

  let changePct = Number(latest.JZZZL)
  if (!Number.isFinite(changePct)) {
    const prev = Number(list[1]?.DWJZ)
    changePct = Number.isFinite(prev) && prev > 0 ? (price / prev - 1) * 100 : 0
  }

  return {
    symbol,
    name,
    instrument: 'otc-fund',
    market: '场外基金',
    date,
    price: round(price, 4),
    changePct: round(changePct, 4),
    source: '天天基金（东方财富）',
  }
}

async function fetchEtf(target) {
  const url = `https://push2.eastmoney.com/api/qt/stock/get?secid=${target.secid}&fields=f43,f57,f58,f59,f60,f86,f170`
  const data = await fetchJson(url)
  const quote = data?.data
  if (!quote) throw new Error('没有返回行情数据')

  const decimals = Number.isFinite(Number(quote.f59)) ? Number(quote.f59) : 2
  const scale = 10 ** decimals
  const price = Number(quote.f43) / scale
  const prevClose = Number(quote.f60) / scale
  if (!Number.isFinite(price) || price <= 0) throw new Error(`价格异常：${quote.f43}`)

  let changePct = Number(quote.f170) / 100
  if (!Number.isFinite(changePct)) {
    changePct = Number.isFinite(prevClose) && prevClose > 0 ? (price / prevClose - 1) * 100 : 0
  }

  const stamp = Number(quote.f86)
  const date = Number.isFinite(stamp) && stamp > 0 ? beijingDate(new Date(stamp * 1000)) : beijingDate()

  return {
    symbol: target.symbol,
    name: target.name,
    instrument: 'etf',
    market: 'A股ETF',
    aliases: target.aliases,
    benchmark: target.benchmark === true,
    date,
    price: round(price, 4),
    prevClose: round(prevClose, 4),
    changePct: round(changePct, 4),
    source: '东方财富行情',
  }
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length)
  let cursor = 0

  const run = async () => {
    while (cursor < items.length) {
      const index = cursor
      cursor += 1
      results[index] = await worker(items[index], index)
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run))
  return results
}

function readJson(path) {
  try {
    if (!existsSync(path)) return null
    return JSON.parse(readFileSync(path, 'utf8'))
  } catch {
    return null
  }
}

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true })
  const temp = `${path}.tmp`
  writeFileSync(temp, `${JSON.stringify(value, null, 2)}\n`, 'utf8')
  renameSync(temp, path)
}

async function main() {
  const tasks = [
    ...OTC_FUNDS.map((fund) => ({ kind: 'fund', fund })),
    ...ETFS.map((etf) => ({ kind: 'etf', etf })),
    ...BENCHMARKS.map((etf) => ({ kind: 'etf', etf })),
  ]

  const results = await mapLimit(tasks, CONCURRENCY, async (task) => {
    const label = task.kind === 'fund' ? task.fund[0] : task.etf.symbol
    try {
      const item = task.kind === 'fund' ? await fetchFund(task.fund) : await fetchEtf(task.etf)
      return { ok: true, item }
    } catch (error) {
      return {
        ok: false,
        symbol: label,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  })

  const succeeded = results.filter((r) => r.ok).map((r) => r.item)
  const failed = results.filter((r) => !r.ok)

  if (failed.length > 0) {
    console.warn(`失败 ${failed.length} 个标的：`)
    for (const item of failed) console.warn(`- ${item.symbol}: ${item.error}`)
  }

  if (succeeded.length === 0) {
    console.error('所有标的都抓取失败，退出且不写入任何文件')
    process.exitCode = 1
    return
  }

  const generatedAt = new Date().toISOString()
  const universe = new Set(tasks.map((task) => (task.kind === 'fund' ? task.fund[0] : task.etf.symbol)))

  const latestPath = join(DATA_DIR, 'latest.json')
  const previous = readJson(latestPath)
  const merged = new Map()

  if (previous && Array.isArray(previous.items)) {
    for (const item of previous.items) {
      if (item && universe.has(item.symbol)) merged.set(item.symbol, item)
    }
  }
  for (const item of succeeded) merged.set(item.symbol, item)

  const items = [...merged.values()].sort((a, b) => a.symbol.localeCompare(b.symbol))
  writeJson(latestPath, { generatedAt, items })

  const month = beijingDate().slice(0, 7)
  const monthPath = join(HISTORY_DIR, `${month}.json`)
  const previousMonth = readJson(monthPath)
  const monthly = new Map()

  if (previousMonth && Array.isArray(previousMonth.items)) {
    for (const item of previousMonth.items) {
      if (item?.symbol && item?.date) monthly.set(`${item.symbol}:${item.date}`, item)
    }
  }
  for (const item of items) monthly.set(`${item.symbol}:${item.date}`, item)

  const monthItems = [...monthly.values()].sort(
    (a, b) => b.date.localeCompare(a.date) || a.symbol.localeCompare(b.symbol),
  )
  writeJson(monthPath, { month, generatedAt, items: monthItems })

  console.log(
    `完成：成功 ${succeeded.length} 个，失败 ${failed.length} 个；latest ${items.length} 条，${month} 历史 ${monthItems.length} 条`,
  )
  console.log(`生成时间：${generatedAt}（${beijingDate()} Asia/Shanghai）`)
}

main().catch((error) => {
  console.error('抓取脚本异常退出：', error)
  process.exitCode = 1
})
