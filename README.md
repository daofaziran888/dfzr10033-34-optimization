# 湖仓工坊 · 联邦查询与多维自助分析平台

面向异构数据源联邦查询、语义模型构建、OLAP多维立方物化、聚合缓存预热、时序窗口分析、查询改写优化与数据集市发布的多维自助分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
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
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源注册 | `source_registry` | 注册数据源 | 源编号、源类型、连接地址 |
| 语义模型 | `semantic_model` | 语义定义 | 模型编号、模型名称、源表映射 |
| 联邦查询 | `query_federation` | 联邦计划 | 计划编号、参与来源、关联键 |
| 多维立方 | `olap_cube` | OLAP立方 | 立方编号、立方名称、维度列表 |
| 指标工厂 | `metric_factory` | 计算指标 | 指标编号、指标名称、数据来源 |
| 维度树 | `dimension_tree` | 维度层级 | 层级编号、维度名称、层级路径 |
| 聚合缓存 | `aggregate_cache` | 缓存配置 | 配置编号、目标立方、缓存粒度 |
| 查询改写 | `query_rewrite` | 改写规则 | 规则编号、原始模式、替换模式 |
| 时序窗口 | `time_window` | 窗口定义 | 窗口编号、窗口类型、滑动宽度 |
| 数据切片 | `dataset_slice` | 切片配置 | 切片编号、源立方、维度过筛 |
| 下钻配置 | `drill_config` | 下钻路径 | 路径编号、起始维度、目标维度 |
| 结果集市 | `result_mart` | 数据Mart | 集市编号、集市名称、数据来源 |
| 基准集 | `benchmark_set` | 分析基线 | 基线编号、基线名称、数据范围 |
| 查询审计 | `query_audit` | 审计记录 | 记录编号、查询语句、执行耗时 |
| 成本治理 | `cost_govern` | 配额策略 | 策略编号、适用角色、查询上限 |
| 新鲜度监控 | `freshness_mon` | 新鲜度规则 | 规则编号、目标数据集、SLA秒数 |
| 分析导出 | `data_exporter` | 导出任务 | 任务编号、数据来源、筛选条件 |
| 定时计算 | `schedule_job` | 预计算任务 | 任务编号、目标立方、Cron表达式 |
| 湖仓读取 | `data_lake_read` | 湖仓路径 | 路径编号、存储位置、文件格式 |
| 结果缓存 | `result_cache` | 缓存条目 | 条目编号、查询摘要、结果哈希 |
| 数据契约 | `data_contract` | 契约定义 | 契约编号、数据集、Schema约束 |
| 实验配置 | `ab_config` | AB实验 | 实验编号、实验名称、分流比例 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 查询改写与代价估算批次

`POST /api/pipelines/rewrite-estimate-execute` 把查询语句、改写规则、模型版本
和代价估算收进同一个「改写与代价估算批次」，阶段依次为
`freeze_version → rewrite → estimate → federate → publish_decision`。

- 批次号由查询 + 冻结的模型/规则签名推导，同一组合重跑命中同一批次：
  命中次数只记一次，成本记录与改写缓存按批次幂等，不重复叠加节省量。
- 扫描预测由实际命中的规则（代价阈值）确定性推导，不依赖随机数，
  因此成本预估可以回溯到同一次查询构建。
- 批次的模型版本指纹写入改写缓存、成本记录和联邦计划；估算结果回写批次
  清单后，计划才有 `对外公布` 标记，`execute_federation_query` 只放行这类计划。
- 节省量按「本次估算 − 历史已结算基线」记增量，兼容旧布局（`适用角色=query_rewrite`
  且节省量存于 `查询上限` 的历史记录）。

断点续跑与版本漂移示例：

```bash
# 1. 改写完成、估算尚未写入时中断
curl -X POST 'http://127.0.0.1:8000/api/pipelines/rewrite-estimate-execute?query=SELECT%20*%20FROM%20source_registry&stop_before=estimate'
# 2. 期间上线一套新模型（如调用语义模型接口投产）
# 3. 用返回的 batch_id / run_id 续跑：检出 model_added 漂移，公布被拒绝
curl -X POST 'http://127.0.0.1:8000/api/pipelines/rewrite-estimate-execute?query=...&batch_id=REB-xxx&run_id=yyy'
# 4. 确认沿用冻结版本后公布并进入执行
curl -X POST 'http://127.0.0.1:8000/api/pipelines/rewrite-estimate-execute/REB-xxx/acknowledge-drift'
```

领域：数据工程
子方向：数据分析与数据应用
