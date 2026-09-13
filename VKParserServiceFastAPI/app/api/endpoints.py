from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import datetime
from typing import Optional
from app.services.comment import CommentService
from app.repository.comment import CommentRepository
from app.database.database import get_db
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/comments", tags=["comments"])


@router.get("/user_comments")
async def get_user_comments(
    user: int = Query(..., description="ID пользователя VK (обязательный)"),
    datefrom: Optional[datetime] = Query(
        None,
        description="Дата от (необязательный)"
    ),
    dateto: Optional[datetime] = Query(
        None,
        description="Дата до (необязательный)"
    ),
    db: AsyncSession = Depends(get_db)
):
    try:
        comment_repository = CommentRepository(db)
        comment_service = CommentService(comment_repository)

        result = await comment_service.get_user_comments(
            user_id=user,
            date_from=datefrom,
            date_to=dateto
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/group_comments")
async def get_group_comments(
    group: int = Query(..., description="ID группы (обязательный)"),
    datefrom: Optional[datetime] = Query(
        None,
        description="Дата от (необязательный)"
    ),
    dateto: Optional[datetime] = Query(
        None,
        description="Дата до (необязательный)"
    ),
    db: AsyncSession = Depends(get_db)
):
    try:
        comment_repository = CommentRepository(db)
        comment_service = CommentService(comment_repository)

        result = await comment_service.get_group_comments(
            group_id=group,
            date_from=datefrom,
            date_to=dateto
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/group_user")
async def get_group_user_comments(
    group: int = Query(..., description="ID группы (обязательный)"),
    user: Optional[int] = Query(
        None,
        description="ID пользователя (необязательный)"
    ),
    datefrom: Optional[datetime] = Query(
        None,
        description="Дата от (необязательный)"
    ),
    dateto: Optional[datetime] = Query(
        None,
        description="Дата до (необязательный)"
    ),
    db: AsyncSession = Depends(get_db)
):
    try:
        comment_repository = CommentRepository(db)
        comment_service = CommentService(comment_repository)

        result = await comment_service.get_group_user_comments(
            group_id=group,
            user_id=user,
            date_from=datefrom,
            date_to=dateto
        )

        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
