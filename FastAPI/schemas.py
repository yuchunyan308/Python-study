"""
schemas.py — Pydantic v2 数据验证与序列化 Schema
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class TaskCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200, description="任务标题")
    description: Optional[str] = Field(None, max_length=1000, description="任务描述")
    priority: str = Field("medium", description="优先级: low / medium / high")

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: str) -> str:
        allowed = {"low", "medium", "high"}
        if v not in allowed:
            raise ValueError(f"priority 必须是 {allowed} 之一")
        return v

    @field_validator("title")
    @classmethod
    def strip_title(cls, v: str) -> str:
        return v.strip()


class TaskUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    completed: Optional[bool] = None
    priority: Optional[str] = None

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in {"low", "medium", "high"}:
            raise ValueError("priority 必须是 low / medium / high 之一")
        return v


class TaskResponse(BaseModel):
    id: int
    title: str
    description: Optional[str]
    completed: bool
    priority: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}  # Pydantic v2: 替代 orm_mode=True


class TaskStats(BaseModel):
    total: int
    completed: int
    pending: int
    high_priority: int
