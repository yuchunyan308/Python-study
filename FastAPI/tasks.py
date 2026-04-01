"""
routers/tasks.py — Task 的 CRUD REST API
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database import get_db
from models import Task, Priority
from schemas import TaskCreate, TaskResponse, TaskStats, TaskUpdate

router = APIRouter(prefix="/api/tasks", tags=["tasks"])


# ─── CREATE ────────────────────────────────────────────────────────────────────

@router.post("/", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
def create_task(payload: TaskCreate, db: Session = Depends(get_db)):
    """新建任务"""
    task = Task(**payload.model_dump())
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


# ─── READ (list) ───────────────────────────────────────────────────────────────

@router.get("/", response_model=List[TaskResponse])
def list_tasks(
    completed: Optional[bool] = Query(None, description="过滤已完成/未完成"),
    priority: Optional[str] = Query(None, description="过滤优先级"),
    q: Optional[str] = Query(None, description="关键词搜索"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """查询任务列表，支持过滤和搜索"""
    stmt = select(Task)
    if completed is not None:
        stmt = stmt.where(Task.completed == completed)
    if priority:
        stmt = stmt.where(Task.priority == priority)
    if q:
        stmt = stmt.where(Task.title.contains(q))
    stmt = stmt.order_by(Task.created_at.desc()).offset(skip).limit(limit)
    return db.execute(stmt).scalars().all()


# ─── READ (single) ─────────────────────────────────────────────────────────────

@router.get("/stats", response_model=TaskStats)
def get_stats(db: Session = Depends(get_db)):
    """返回任务统计摘要"""
    total = db.execute(select(func.count()).select_from(Task)).scalar_one()
    completed = db.execute(
        select(func.count()).select_from(Task).where(Task.completed == True)
    ).scalar_one()
    high_priority = db.execute(
        select(func.count()).select_from(Task).where(Task.priority == Priority.high)
    ).scalar_one()
    return TaskStats(
        total=total,
        completed=completed,
        pending=total - completed,
        high_priority=high_priority,
    )


@router.get("/{task_id}", response_model=TaskResponse)
def get_task(task_id: int, db: Session = Depends(get_db)):
    """按 ID 查询单个任务"""
    task = db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} 不存在")
    return task


# ─── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch("/{task_id}", response_model=TaskResponse)
def update_task(task_id: int, payload: TaskUpdate, db: Session = Depends(get_db)):
    """部分更新任务（PATCH 语义）"""
    task = db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} 不存在")
    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(task, field, value)
    db.commit()
    db.refresh(task)
    return task


# ─── DELETE ────────────────────────────────────────────────────────────────────

@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_task(task_id: int, db: Session = Depends(get_db)):
    """删除任务"""
    task = db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} 不存在")
    db.delete(task)
    db.commit()
