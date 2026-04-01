# TaskFlow — FastAPI 全栈任务管理系统

## 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| Web 框架 | FastAPI 0.115 | 异步、自动 OpenAPI 文档 |
| ORM | SQLAlchemy 2.0 | 声明式映射，支持异步扩展 |
| 数据库 | SQLite | 开发零配置，可无缝切换 PostgreSQL |
| 数据验证 | Pydantic v2 | 类型安全，比 v1 快 3–5x |
| 模板引擎 | Jinja2 | 服务端渲染，SEO 友好 |
| 前端 | 原生 JS + CSS | 无构建依赖，极速加载 |

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 启动服务（热重载）
uvicorn main:app --reload

# 3. 访问
#  前端页面:   http://localhost:8000
#  API 文档:  http://localhost:8000/docs
#  ReDoc:     http://localhost:8000/redoc
```

## 项目结构

```
taskflow/
├── main.py            # 应用入口，路由注册，生命周期管理
├── database.py        # SQLAlchemy 引擎与 Session 配置
├── models.py          # ORM 数据模型（Task）
├── schemas.py         # Pydantic 请求/响应 Schema
├── routers/
│   └── tasks.py       # Task CRUD REST API
├── templates/
│   └── index.html     # 前端单页应用（纯 HTML/CSS/JS）
├── static/            # 静态资源目录
├── requirements.txt
└── taskflow.db        # SQLite 数据库（运行后自动生成）
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | /api/tasks/ | 列表（支持 filter/search/paginate） |
| POST | /api/tasks/ | 新建任务 |
| GET  | /api/tasks/stats | 统计摘要 |
| GET  | /api/tasks/{id} | 单个任务 |
| PATCH| /api/tasks/{id} | 部分更新 |
| DELETE| /api/tasks/{id} | 删除 |
| GET  | /health | 健康检查 |

## 切换到 PostgreSQL

修改 `database.py` 中的连接串：

```python
DATABASE_URL = "postgresql+psycopg2://user:pass@localhost/taskflow"
```

安装驱动：`pip install psycopg2-binary`

## 生产部署

```bash
# 使用 Gunicorn + Uvicorn workers
pip install gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```
