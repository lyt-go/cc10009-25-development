import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/crawljob'
const FAILURE_COLUMNS = ['作业编号', '失败URL', '退避级别', '已重试次数', '尝试编号', '节点编号', '尝试状态', '清单状态', '处理来源', '冲突标记', '作业冻结', '更新时间']
const columns = [
  "作业编号",
  "目标站点",
  "起始URL",
  "爬取深度",
  "URL去重策略",
  "页面解析器",
  "下载超时秒",
  "最大页面数"
]
const actions = [
  "提交作业审批",
  "启动作业",
  "停止作业"
]
const statuses = [
  "草稿",
  "作业审批中",
  "执行中",
  "已完成",
  "已停止"
]
const stats = [
  {
    "label": "作业定义总数",
    "value": 0
  },
  {
    "label": "执行中作业",
    "value": 0
  },
  {
    "label": "已完成作业",
    "value": 0
  }
]

type Row = Record<string, string | number | null | boolean>

export default function CrawljobView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [failureRows, setFailureRows] = useState<Row[]>([])
  const [jobNo, setJobNo] = useState('')
  const [notice, setNotice] = useState('')
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('作业定义列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '抓取作业列表读取失败')
    }
  }, [filters])

  const reloadFailures = useCallback(async () => {
    setErrorMessage('')
    try {
      const query = new URLSearchParams(jobNo ? { job_no: jobNo } : {}).toString()
      const response = await request(`${ENDPOINT}/failures/list?${query}`)
      if (!response.ok) {
        throw new Error('作业失败清单读取失败')
      }
      const payload = await response.json()
      setFailureRows((payload.items ?? []).filter((item: Row) => item['快照批次'] === null))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '作业失败清单读取失败')
    }
  }, [jobNo])

  useEffect(() => {
    void reload()
  }, [reload])

  useEffect(() => {
    void reloadFailures()
  }, [reloadFailures])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('抓取作业动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '抓取作业操作失败')
    }
  }

  async function syncFailures() {
    setNotice('')
    setErrorMessage('')
    if (!jobNo) {
      setErrorMessage('请先填写要同步失败清单的作业编号')
      return
    }
    try {
      const response = await request(`${ENDPOINT}/failures/sync`, {
        method: 'POST',
        body: JSON.stringify({ values: { 作业编号: jobNo } }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message || '重试进度同步失败')
      }
      setNotice(`${payload.message}；最终失败 ${payload.entry['最终失败数']} 条、已恢复 ${payload.entry['已恢复数']} 条、重试中 ${payload.entry['重试中数']} 条，作业状态未被改写`)
      await reloadFailures()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '重试进度同步失败')
    }
  }

  return (
    <section className="page" data-module="crawljob">
      <header className="page-head">
        <div>
          <h2>抓取作业管理</h2>
          <p className="page-desc">定义单次抓取作业的范围和参数，并消费重试队列进度同步本作业失败清单。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('作业定义登记入口尚未接入审批流')}>
            登记作业定义
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出抓取作业清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无抓取作业数据，可先登记作业定义</td>
            </tr>
          )}
        </tbody>
      </table>

      <div className="panel">
        <h3>采集执行侧失败清单（重试进度回写）</h3>
        <p className="page-desc">最终失败只能来自退避耗尽条目；作业停止后同步只更新清单，不会把作业带回执行中。</p>
        <div className="filter-bar">
          <label className="filter-item">
            <span>作业编号</span>
            <input value={jobNo} placeholder="如 CRAW-0003" onChange={(event) => setJobNo(event.target.value)} />
          </label>
          <button className="btn primary" type="button" onClick={() => void syncFailures()}>拉取重试进度并同步</button>
          <button className="btn" type="button" onClick={() => void reloadFailures()}>刷新清单</button>
        </div>
        <table className="data-table">
          <thead>
            <tr>
              {FAILURE_COLUMNS.map((column) => <th key={column}>{column}</th>)}
            </tr>
          </thead>
          <tbody>
            {failureRows.map((row) => (
              <tr key={String(row.id)}>
                {FAILURE_COLUMNS.map((column) => (
                  <td key={column}>{row[column] === null || row[column] === '' ? '—' : String(row[column])}</td>
                ))}
              </tr>
            ))}
            {failureRows.length === 0 && (
              <tr>
                <td className="empty-state" colSpan={FAILURE_COLUMNS.length}>暂无失败清单记录，先在重试队列登记失败URL并回写结果</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <footer className="page-foot">
        <span>共 {total} 条抓取作业记录</span>
        {notice && <span className="ok-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
