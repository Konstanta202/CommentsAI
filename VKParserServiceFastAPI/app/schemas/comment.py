from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class CommentResponse(BaseModel):
    id: int
    user_id: int
    posts_id: int
    comment: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class CommentsFilterResponse(BaseModel):
    user_id: Optional[int] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    comments_count: int
    comments: list[CommentResponse]
