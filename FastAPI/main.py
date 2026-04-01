"""
main.py — FastAPI 应用入口
运行: uvicorn main:app --reload
"""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import Base, engine
from routers import tasks as tasks_router


# ─── 生命周期：启动时建表，关闭时可做清理 ─────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)  # 首次运行自动建表
    yield
    # 关闭时的清理逻辑可放在此处


# ─── 应用实例 ──────────────────────────────────────────────────────────────────

app = FastAPI(
    title="TaskFlow",
    description="现代化任务管理系统 — FastAPI + SQLAlchemy + Jinja2",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── 静态文件 & 模板 ───────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# ─── 路由注册 ──────────────────────────────────────────────────────────────────

app.include_router(tasks_router.router)


# ─── 前端页面路由 ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """返回单页应用主页面"""
    return templates.TemplateResponse("index.html", {"request": request})


# ─── 健康检查 ─────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok", "app": "TaskFlow", "version": "1.0.0"}


# ─── 开发模式直接运行 ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
