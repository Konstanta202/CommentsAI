from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.comment import Comment
from app.models.post import Post
from datetime import datetime
from typing import List, Optional


class CommentRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_user_comments(
        self,
        user_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> List[Comment]:

        query = select(Comment).where(Comment.user_id == user_id)

        if date_from:
            query = query.where(Comment.created_at >= date_from)
        if date_to:
            query = query.where(Comment.created_at <= date_to)

        result = await self.db.execute(query)
        return result.scalars().all()

    async def get_group_comments(
        self,
        group_id: int,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> List[Comment]:

        posts_query = select(Post.id).where(Post.group_id == group_id)
        posts_result = await self.db.execute(posts_query)
        post_ids = posts_result.scalars().all()

        if not post_ids:
            return []

        query = select(Comment).where(Comment.posts_id.in_(post_ids))

        if date_from:
            query = query.where(Comment.created_at >= date_from)
        if date_to:
            query = query.where(Comment.created_at <= date_to)

        result = await self.db.execute(query)
        return result.scalars().all()

    async def get_group_user_comments(
        self,
        group_id: int,
        user_id: Optional[int] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None
    ) -> List[Comment]:

        posts_query = select(Post.id).where(Post.group_id == group_id)
        posts_result = await self.db.execute(posts_query)
        post_ids = posts_result.scalars().all()

        if not post_ids:
            return []  # В группе нет постов

        query = select(Comment).where(Comment.posts_id.in_(post_ids))

        if user_id:
            query = query.where(Comment.user_id == user_id)

        if date_from:
            query = query.where(Comment.created_at >= date_from)
        if date_to:
            query = query.where(Comment.created_at <= date_to)

        result = await self.db.execute(query)
        return result.scalars().all()
