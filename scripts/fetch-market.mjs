#!/usr/bin/env node
/**
 * GoldPlan 行情抓取脚本。
 *
 * 只抓行情，不读取、也不写入任何持仓数量或金额：
 * - 标的清单来自仓库根目录 symbols.json，加新标的只需改那里
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
const SYMBOL_FILE = join(ROOT, 'symbols.json')

const TIMEOUT_MS = 20_000
const ATTEMPTS = 3
const CONCURRENCY = 4

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'

/**
 * 读取仓库根目录的 symbols.json。
 * 任何格式问题都在抓取前直接报错，避免静默产出空数据。
 */
function loadSymbols() {
  if (!existsSync(SYMBOL_FILE)) {
    throw new Error(`找不到 ${SYMBOL_FILE}，请把 symbols.json 放在仓库根目录`)
  }

  let raw
  try {
    raw = readFileSync(SYMBOL_FILE, 'utf8')
  } catch (error) {
    throw new Error(`无法读取 symbols.json：${error.message}`)
  }

  let parsed
  try {
    parsed = JSON.parse(raw)
  } catch (error) {
    throw new Error(`symbols.json 不是合法 JSON：${error.message}`)
  }

  if (!Array.isArray(parsed)) throw new Error('symbols.json 顶层必须是数组')
  if (parsed.length === 0) throw new Error('symbols.json 不能为空数组')

  const seen = new Set()
  const aliasSeen = new Map()

  return parsed.map((entry, index) => {
    const where = `第 ${index + 1} 项`
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error(`${where} 必须是对象`)
    }

    const symbol = typeof entry.symbol === 'string' ? entry.symbol.trim() : ''
    if (!/^\d{6}$/.test(symbol)) {
      throw new Error(`${where} 的代码格式不对：${symbol || '空'}（需要 6 位数字）`)
    }
    if (seen.has(symbol)) throw new Error(`代码 ${symbol} 重复出现`)
    seen.add(symbol)

    if (entry.type !== 'fund' && entry.type !== 'etf') {
      throw new Error(`${where}（${symbol}）的 type 必须是 "fund" 或 "etf"`)
    }

    const name = typeof entry.name === 'string' ? entry.name.trim() : ''
    if (!name) throw new Error(`${where}（${symbol}）缺少 name`)

    const market =
      typeof entry.market === 'string' && entry.market.trim()
        ? entry.market.trim()
        : entry.type === 'fund'
          ? '场外基金'
          : 'A股ETF'

    let aliases
    if (entry.aliases !== undefined) {
      if (
        !Array.isArray(entry.aliases) ||
        entry.aliases.some((alias) => typeof alias !== 'string' || !alias.trim())
      ) {
        throw new Error(`${where}（${symbol}）的 aliases 必须是非空字符串数组`)
      }
      aliases = entry.aliases.map((alias) => alias.trim())
      for (const alias of aliases) {
        const owner = aliasSeen.get(alias)
        if (owner && owner !== symbol) {
          throw new Error(`别名 ${alias} 同时属于 ${owner} 和 ${symbol}`)
        }
        aliasSeen.set(alias, symbol)
      }
    }

    let secid
    if (entry.type === 'etf') {
      if (entry.secid !== undefined) {
        if (typeof entry.secid !== 'string' || !/^[01]\.\d{6}$/.test(entry.secid)) {
          throw new Error(
            `${where}（${symbol}）的 secid 格式不对：${String(entry.secid)}（示例 "0.159612" / "1.510300"）`,
          )
        }
        secid = entry.secid
      } else {
        // 未填 secid 时按代码首位推断：5/6 开头为沪市，其余为深市
        secid = `${/^[56]/.test(symbol) ? '1' : '0'}.${symbol}`
      }
    }

    if (entry.benchmark !== undefined && typeof entry.benchmark !== 'boolean') {
      throw new Error(`${where}（${symbol}）的 benchmark 必须是 true 或 false`)
    }

    return {
      symbol,
      name,
      type: entry.type,
      market,
      aliases,
      secid,
      benchmark: entry.benchmark === true,
    }
  })
}

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

async function fetchFund(target) {
  const { symbol, name, market } = target
  const url = `https://api.fund.eastmoney.com/f10/lsjz?fundCode=${symbol}&pageIndex=1&pageSize=2`
  const data = await fetchJson(url, { Referer: 'https://fundf10.eastmoney.com/' })
  const list = data?.Data?.LSJZList
  const latest = Array.isArray(list) ? list[0] : null
  if (!latest) {
    const hint = data?.ErrCode || data?.ErrMsg ? `（接口返回 ${data.ErrCode ?? ''} ${data.ErrMsg ?? ''}）` : ''
    throw new Error(`代码 ${symbol} 没有净值记录，可能不存在或已停售${hint}`)
  }

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
    market: market || '场外基金',
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
  if (!quote || quote.f43 === undefined || quote.f43 === null) {
    throw new Error(`代码 ${target.symbol} 没有行情数据（secid ${target.secid}），可能不存在或已退市`)
  }

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
    market: target.market || 'A股ETF',
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
  // 清单有问题时在抓取前就报错，避免静默产出空数据
  let symbols
  try {
    symbols = loadSymbols()
  } catch (error) {
    console.error(`symbols.json 配置错误：${error instanceof Error ? error.message : String(error)}`)
    process.exitCode = 1
    return
  }
  const tasks = symbols.map((target) => ({ kind: target.type, target }))

  const results = await mapLimit(tasks, CONCURRENCY, async (task) => {
    const label = task.target.symbol
    try {
      const item = task.kind === 'fund' ? await fetchFund(task.target) : await fetchEtf(task.target)
      return { ok: true, item }
    } catch (error) {
      return {
        ok: false,
        symbol: label,
        name: task.target.name,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  })

  const succeeded = results.filter((r) => r.ok).map((r) => r.item)
  const failed = results.filter((r) => !r.ok)

  if (failed.length > 0) {
    console.warn(`失败 ${failed.length} 个标的（已跳过，其余继续）：`)
    for (const item of failed) console.warn(`- ${item.symbol} ${item.name}: ${item.error}`)
    console.warn('如果代码是新加的，请检查 symbols.json 中的代码是否正确后重试')
  }

  if (succeeded.length === 0) {
    console.error('所有标的都抓取失败，退出且不写入任何文件')
    process.exitCode = 1
    return
  }

  const generatedAt = new Date().toISOString()
  const universe = new Set(symbols.map((s) => s.symbol))

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
