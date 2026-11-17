# 分布式网页抓取调度平台

面向分布式网页抓取任务调度、节点协同、请求分配、反爬策略编排、数据校验入库与抓取质量大盘的采集工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点 | `worker` | 工作节点 | 节点编号、主机地址、并发槽位数 |
| 请求分配 | `dispatch` | 分配策略 | 策略编号、目标域名、分配算法 |
| 抓取作业 | `crawljob` | 作业定义 | 作业编号、目标站点、起始URL |
| URL过滤 | `urlfilter` | 过滤规则 | 规则编号、包含模式、排除模式 |
| DNS解析 | `dnsresolver` | 解析配置 | 配置编号、域名模式、DNS服务器 |
| 内容提取 | `contentextract` | 提取器 | 提取器编号、内容类型、正文选择器 |
| 存储策略 | `storagestrategy` | 存储方案 | 方案编号、存储类型、连接参数 |
| 渲染任务 | `rendertask` | 渲染作业 | 作业编号、目标URL、浏览器版本 |
| 采集质量 | `qualityreport` | 质量报告 | 报告编号、作业编号、总URL数 |
| 带宽控制 | `bandwidth` | 带宽策略 | 策略编号、节点标识、下载带宽上限 |
| 重试队列 | `retryqueue` | 重试条目/尝试/进度/最终失败 | 失败URL、退避级别、尝试token、节点编号、批次号、校验和 |
| 翻页管理 | `pagemarker` | 翻页规则 | 规则编号、目标站点、翻页类型 |
| 增量标记 | `incrementmark` | 增量水位 | 水位编号、目标源、增量字段名 |
| 配置同步 | `configsync` | 同步任务 | 任务编号、源节点、目标节点 |
| 采集API | `scrapeapi` | API定义 | API编号、API名称、请求路径 |
| 数据抽样 | `datasample` | 抽样方案 | 方案编号、数据源、抽样方式 |
| 采集告警 | `alertpolicy` | 告警策略 | 策略编号、监控指标、触发条件 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 重试队列：尝试、回写与可校验数据集

重试队列在基础条目之外维护四个数据集（内存表 `retry_attempt`、`retry_progress`、
`retry_final_failure`、`retry_integrity`），采集执行侧还有 `job_failure_item`。

受控接口（请求体 `values` 内带 `operator` 与 `role`）：

| 接口 | 作用 |
| --- | --- |
| `POST /api/retryqueue/discoveries` | 采集执行侧登记失败URL；同作业同URL重复发现幂等，不产生第二份尝试 |
| `POST /api/retryqueue/attempts/claim` | 节点抢占槽位，下发尝试编号与token；退避未到点/作业已停止拒绝，并发抢占同一URL只放一个槽位（冲突返回409） |
| `POST /api/retryqueue/attempts/writeback` | 凭token+节点编号回写结果，身份不符返回403；重复回执幂等；失败推进退避级别，耗尽（5级）才进最终失败清单，成功回写后从最终失败清单移除 |
| `GET /api/retryqueue/attempts/list` | 尝试记录：每次尝试的节点、token、抢占/完成时间、恢复结果可追溯 |
| `GET /api/retryqueue/progress/list` | 重试进度数据集，支持按作业/批次/状态过滤 |
| `POST /api/retryqueue/progress/seal`、`/merge` | 批次封存（登记SHA-256校验和）与两个批次版本合并，跨作业来源冲突显式标记 |
| `POST /api/retryqueue/integrity/verify`、`GET /integrity/ledger` | 数据集完整性验证（快照完整 + 当前一致两层口径）与台账 |
| `GET /api/retryqueue/final-failures/list` | 最终失败清单，仅含退避耗尽条目 |
| `POST /api/crawljob/failures/sync`、`GET /failures/list`、`POST /failures/verify` | 采集执行侧消费重试进度、回写作业失败清单（每次同步生成可校验快照，作业已停止时只更新清单，绝不把作业带回执行中） |

角色受控范围：查看者只读；作业负责人只能登记本作业失败URL；节点执行员只能抢占/回写；
管理员可封存合并。越权返回 403。已封存批次不可改，后续流转自动派生开放新版本。

端到端验证脚本：`backend/tests_retry_flow.py`（`python -I` 运行，覆盖退避、抢占、
幂等、停止作业、冲突合并、完整性篡改检测等 49 项断言）。

领域：数据工程
子方向：数据采集、爬虫与RPA
