import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/retryqueue'

type Row = Record<string, string | number | null | boolean>

const ROLES = ['管理员', '作业负责人', '节点执行员', '查看者'] as const
type Role = (typeof ROLES)[number]
const ROLE_SCOPE: Record<Role, string[]> = {
  管理员: ['discover', 'claim', 'writeback', 'seal', 'merge'],
  作业负责人: ['discover'],
  节点执行员: ['claim', 'writeback'],
  查看者: [],
}

const TABS = [
  { key: 'progress', label: '重试进度' },
  { key: 'attempts', label: '重试尝试' },
  { key: 'finals', label: '最终失败清单' },
  { key: 'integrity', label: '完整性台账' },
  { key: 'entries', label: '基础条目' },
] as const
type TabKey = (typeof TABS)[number]['key']

const PROGRESS_COLUMNS = ['作业编号', '失败URL', '退避级别', '已重试次数', '下次重试时间', '尝试状态', '节点编号', '恢复结果', '批次号', '批次版本', '冲突标记', '处理来源']
const ATTEMPT_COLUMNS = ['尝试编号', '作业编号', '失败URL', '退避级别', '节点编号', '尝试状态', '抢占时间', '完成时间', '回执次数', '恢复结果']
const FINAL_COLUMNS = ['作业编号', '失败URL', '失败原因码', '退避级别', '尝试编号', '节点编号', '入单时间', '清单状态', '移除时间', '快照批次']
const LEDGER_COLUMNS = ['台账编号', '数据集类型', '批次号', '批次版本', '行数', '校验和', '登记时间', '校验状态', '最后校验时间']
const ENTRY_COLUMNS = ['条目编号', '失败URL', '失败原因码', '首次失败时间', '已重试次数', '下次重试时间', '退避级别', '最终处置', 'status']

interface FormState {
  作业编号: string
  失败URL: string
  失败原因码: string
  节点编号: string
  尝试编号: string
  尝试token: string
  结果: 'success' | 'failure'
  恢复结果: string
  批次号: string
  批次号A: string
  批次号B: string
  数据集类型: 'retry_progress' | 'retry_final_failure'
}

const EMPTY_FORM: FormState = {
  作业编号: '',
  失败URL: '',
  失败原因码: '',
  节点编号: '',
  尝试编号: '',
  尝试token: '',
  结果: 'success',
  恢复结果: '',
  批次号: '',
  批次号A: '',
  批次号B: '',
  数据集类型: 'retry_progress',
}

async function postJson(path: string, values: Record<string, unknown>): Promise<{ ok: boolean; message: string; entry?: Row }> {
  const response = await request(path, { method: 'POST', body: JSON.stringify({ values }) })
  const payload = await response.json().catch(() => ({ ok: false, message: `接口返回 ${response.status}` }))
  if (!response.ok) {
    return { ok: false, message: typeof payload.detail === 'string' ? payload.detail : `请求被拒绝（${response.status}）` }
  }
  return payload
}

export default function RetryqueueView() {
  const [operator, setOperator] = useState(() => localStorage.getItem('retry-operator') || '值班管理员')
  const [role, setRole] = useState<Role>(() => (localStorage.getItem('retry-role') as Role) || '管理员')
  const [tab, setTab] = useState<TabKey>('progress')
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [form, setForm] = useState<FormState>(EMPTY_FORM)

  const can = useCallback((scope: string) => ROLE_SCOPE[role].includes(scope), [role])

  const persistIdentity = (nextOperator: string, nextRole: Role) => {
    setOperator(nextOperator)
    setRole(nextRole)
    localStorage.setItem('retry-operator', nextOperator)
    localStorage.setItem('retry-role', nextRole)
  }

  const patch = (key: keyof FormState, value: string) => setForm((prev) => ({ ...prev, [key]: value }))

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const pathMap: Record<TabKey, string> = {
        progress: `${ENDPOINT}/progress/list`,
        attempts: `${ENDPOINT}/attempts/list`,
        finals: `${ENDPOINT}/final-failures/list`,
        integrity: `${ENDPOINT}/integrity/ledger`,
        entries: `${ENDPOINT}?size=200`,
      }
      const response = await request(pathMap[tab])
      if (!response.ok) {
        throw new Error('重试数据集读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '重试队列数据读取失败')
    }
  }, [tab])

  useEffect(() => {
    void reload()
  }, [reload])

  const stats = useMemo(() => {
    const waiting = rows.filter((row) => row['尝试状态'] === '等待派发' || row.status === '等待重试').length
    const succeeded = rows.filter((row) => row['尝试状态'] === '重试成功' || row.status === '重试成功').length
    const rate = rows.length ? Math.round((succeeded / rows.length) * 100) : 0
    return [
      { label: tab === 'entries' ? '重试条目总数' : '当前数据集记录', value: rows.length },
      { label: '等待重试数', value: waiting },
      { label: '重试成功率', value: `${rate}%` },
    ]
  }, [rows, tab])

  async function submit(scope: string, path: string, values: Record<string, unknown>, successHint: string) {
    setMessage('')
    setErrorMessage('')
    if (!can(scope)) {
      setErrorMessage(`角色「${role}」对重试队列只有只读权限，该操作被受控范围拦截`)
      return
    }
    const result = await postJson(path, { operator, role, ...values })
    if (result.ok) {
      setMessage(result.message || successHint)
      await reload()
    } else {
      setErrorMessage(result.message)
    }
  }

  const columns = tab === 'progress' ? PROGRESS_COLUMNS
    : tab === 'attempts' ? ATTEMPT_COLUMNS
      : tab === 'finals' ? FINAL_COLUMNS
        : tab === 'integrity' ? LEDGER_COLUMNS
          : ENTRY_COLUMNS

  return (
    <section className="page" data-module="retryqueue">
      <header className="page-head">
        <div>
          <h2>重试队列管理</h2>
          <p className="page-desc">失败URL重试尝试、退避派发、成功回写与可校验的重试进度/最终失败数据集。</p>
        </div>
        <div className="page-actions identity-bar">
          <label className="filter-item">
            <span>操作人</span>
            <input value={operator} onChange={(event) => persistIdentity(event.target.value, role)} />
          </label>
          <label className="filter-item">
            <span>角色（受控范围）</span>
            <select value={role} onChange={(event) => persistIdentity(operator, event.target.value as Role)}>
              {ROLES.map((item) => <option key={item} value={item}>{item}</option>)}
            </select>
          </label>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出基础清单
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

      <div className="tabs">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            className={`tab ${tab === item.key ? 'active' : ''}`}
            onClick={() => setTab(item.key)}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="panel">
        <div className="form-grid">
          <label className="filter-item">
            <span>作业编号</span>
            <input value={form.作业编号} onChange={(event) => patch('作业编号', event.target.value)} placeholder="如 CRAW-0003" />
          </label>
          <label className="filter-item">
            <span>失败URL</span>
            <input value={form.失败URL} onChange={(event) => patch('失败URL', event.target.value)} placeholder="失败页面地址" />
          </label>
          <label className="filter-item">
            <span>失败原因码</span>
            <input value={form.失败原因码} onChange={(event) => patch('失败原因码', event.target.value)} placeholder="如 E503" />
          </label>
          <button
            className="btn primary"
            type="button"
            disabled={!can('discover')}
            title={can('discover') ? '登记失败URL，重复发现幂等' : '仅管理员/作业负责人可登记'}
            onClick={() => void submit('discover', `${ENDPOINT}/discoveries`, {
              作业编号: form.作业编号,
              失败URL: form.失败URL,
              失败原因码: form.失败原因码,
            }, '失败URL已登记')}
          >
            登记失败URL
          </button>
          <label className="filter-item">
            <span>抢占节点编号</span>
            <input value={form.节点编号} onChange={(event) => patch('节点编号', event.target.value)} placeholder="如 WORK-0003（需在线）" />
          </label>
          <button
            className="btn primary"
            type="button"
            disabled={!can('claim')}
            title={can('claim') ? '退避到点后抢占槽位，同URL并发只放一个槽位' : '仅管理员/节点执行员可抢占'}
            onClick={() => void submit('claim', `${ENDPOINT}/attempts/claim`, {
              作业编号: form.作业编号,
              失败URL: form.失败URL,
              节点编号: form.节点编号,
            }, '槽位抢占成功')}
          >
            抢占重试槽位
          </button>
        </div>

        <div className="form-grid">
          <label className="filter-item">
            <span>尝试编号</span>
            <input value={form.尝试编号} onChange={(event) => patch('尝试编号', event.target.value)} placeholder="如 ATT-0001" />
          </label>
          <label className="filter-item">
            <span>尝试token</span>
            <input value={form.尝试token} onChange={(event) => patch('尝试token', event.target.value)} placeholder="抢占时下发的身份令牌" />
          </label>
          <label className="filter-item">
            <span>回写节点</span>
            <input value={form.节点编号} onChange={(event) => patch('节点编号', event.target.value)} placeholder="必须与抢占节点一致" />
          </label>
          <label className="filter-item">
            <span>回执结果</span>
            <select value={form.结果} onChange={(event) => patch('结果', event.target.value)}>
              <option value="success">success（抓取成功）</option>
              <option value="failure">failure（仍然失败）</option>
            </select>
          </label>
          <label className="filter-item">
            <span>恢复结果/失败说明</span>
            <input value={form.恢复结果} onChange={(event) => patch('恢复结果', event.target.value)} placeholder="成功回执或失败原因补充" />
          </label>
          <button
            className="btn primary"
            type="button"
            disabled={!can('writeback')}
            title={can('writeback') ? '校验尝试身份后回写，重复回执幂等' : '仅管理员/节点执行员可回写'}
            onClick={() => void submit('writeback', `${ENDPOINT}/attempts/writeback`, {
              尝试编号: form.尝试编号,
              尝试token: form.尝试token,
              节点编号: form.节点编号,
              结果: form.结果,
              恢复结果: form.恢复结果,
              失败原因码: form.失败原因码,
            }, '结果已回写')}
          >
            回写抓取结果
          </button>
        </div>

        <div className="form-grid">
          <label className="filter-item">
            <span>批次号</span>
            <input value={form.批次号} onChange={(event) => patch('批次号', event.target.value)} placeholder="如 B-0001" />
          </label>
          <button
            className="btn"
            type="button"
            disabled={!can('seal')}
            onClick={() => void submit('seal', `${ENDPOINT}/progress/seal`, { 批次号: form.批次号 }, '批次已封存')}
          >
            封存批次并登记校验和
          </button>
          <label className="filter-item">
            <span>数据集类型</span>
            <select value={form.数据集类型} onChange={(event) => patch('数据集类型', event.target.value)}>
              <option value="retry_progress">重试进度</option>
              <option value="retry_final_failure">最终失败清单</option>
            </select>
          </label>
          <button
            className="btn"
            type="button"
            onClick={() => void submit('seal', `${ENDPOINT}/integrity/verify`, { 批次号: form.批次号, 数据集类型: form.数据集类型 }, '完整性已校验')}
          >
            验证数据集完整性
          </button>
          <label className="filter-item">
            <span>待合并批次A / B</span>
            <span className="merge-inputs">
              <input value={form.批次号A} onChange={(event) => patch('批次号A', event.target.value)} placeholder="B-0001" />
              <input value={form.批次号B} onChange={(event) => patch('批次号B', event.target.value)} placeholder="B-0002" />
            </span>
          </label>
          <button
            className="btn"
            type="button"
            disabled={!can('merge')}
            onClick={() => void submit('merge', `${ENDPOINT}/progress/merge`, { 批次号A: form.批次号A, 批次号B: form.批次号B }, '批次已合并')}
          >
            合并两个批次版本
          </button>
        </div>
      </div>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] === null || row[column] === '' ? '—' : String(row[column])}</td>)}
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length}>暂无数据，可通过上方受控操作生成重试记录</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条记录 · 当前操作人 {operator}（{role}，{ROLE_SCOPE[role].length ? '受控可写' : '只读'}）</span>
        {message && <span className="ok-text">{message}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
