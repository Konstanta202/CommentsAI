import httpx
import os
from dotenv import load_dotenv
from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Optional


router = APIRouter(prefix="/comments")

load_dotenv()
VK_PARSER_SERVICE = os.getenv("VK_PARSER_SERVICE", "http://localhost:8005")


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
    )
):
    try:
        async with httpx.AsyncClient() as client:
            params = {"user": user}
            if datefrom:
                params["datefrom"] = datefrom.isoformat()
            if dateto:
                params["dateto"] = dateto.isoformat()

            response = await client.get(
                f"{VK_PARSER_SERVICE}/comments/user_comments",
                params=params
            )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.json().get(
                    "detail",
                    "Error from comment service"
                )
            )

        return response.json()

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Comment service unavailable: {str(e)}"
        )
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
    )
):
    try:
        async with httpx.AsyncClient() as client:
            params = {"group": group}
            if datefrom:
                params["datefrom"] = datefrom.isoformat()
            if dateto:
                params["dateto"] = dateto.isoformat()

            response = await client.get(
                f"{VK_PARSER_SERVICE}/comments/group_comments",
                params=params
            )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.json().get(
                    "detail",
                    "Error from comment service"
                )
            )

        return response.json()

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Comment service unavailable: {str(e)}"
        )
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
    )
):
    try:
        async with httpx.AsyncClient() as client:
            params = {"group": group}
            if user:
                params["user"] = user
            if datefrom:
                params["datefrom"] = datefrom.isoformat()
            if dateto:
                params["dateto"] = dateto.isoformat()

            response = await client.get(
                f"{VK_PARSER_SERVICE}/comments/group_user",
                params=params
            )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.json().get(
                    "detail",
                    "Error from comment service"
                )
            )

        return response.json()

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Comment service unavailable: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
