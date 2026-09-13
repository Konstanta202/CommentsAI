from app.repository.comment import CommentRepository
from app.schemas.comment import CommentResponse, CommentsFilterResponse
from datetime import datetime


class CommentService:
    def __init__(self, comment_repository: CommentRepository):
        self.comment_repository = comment_repository

    async def get_user_comments(
        self,
        user_id: int,
        date_from: datetime = None,
        date_to: datetime = None
    ) -> CommentsFilterResponse:

        if not user_id:
            raise ValueError("User ID is required")

        comments = await self.comment_repository.get_user_comments(
            user_id=user_id,
            date_from=date_from,
            date_to=date_to
        )

        comment_responses = [
            CommentResponse(
                id=comment.id,
                posts_id=comment.posts_id,
                user_id=comment.user_id,
                comment=comment.comment,
                created_at=comment.created_at
            )
            for comment in comments
        ]

        return CommentsFilterResponse(
            user_id=user_id,
            date_from=date_from,
            date_to=date_to,
            comments_count=len(comment_responses),
            comments=comment_responses
        )

    async def get_group_comments(
        self,
        group_id: int,
        date_from: datetime = None,
        date_to: datetime = None
    ) -> CommentsFilterResponse:

        if not group_id:
            raise ValueError("Group ID is required")

        comments = await self.comment_repository.get_group_comments(
            group_id=group_id,
            date_from=date_from,
            date_to=date_to
        )

        comment_responses = [
            CommentResponse(
                id=comment.id,
                posts_id=comment.posts_id,
                user_id=comment.user_id,
                comment=comment.comment,
                created_at=comment.created_at
            )
            for comment in comments
        ]

        return CommentsFilterResponse(
            group_id=group_id,
            date_from=date_from,
            date_to=date_to,
            comments_count=len(comment_responses),
            comments=comment_responses
        )

    async def get_group_user_comments(
        self,
        group_id: int,
        user_id: int = None,
        date_from: datetime = None,
        date_to: datetime = None
    ) -> CommentsFilterResponse:

        if not group_id:
            raise ValueError("Group ID is required")

        comments = await self.comment_repository.get_group_user_comments(
            group_id=group_id,
            user_id=user_id,
            date_from=date_from,
            date_to=date_to
        )

        comment_responses = [
            CommentResponse(
                id=comment.id,
                posts_id=comment.posts_id,
                user_id=comment.user_id,
                comment=comment.comment,
                created_at=comment.created_at
            )
            for comment in comments
        ]

        return CommentsFilterResponse(
            group_id=group_id,
            user_id=user_id,
            date_from=date_from,
            date_to=date_to,
            comments_count=len(comment_responses),
            comments=comment_responses
        )
