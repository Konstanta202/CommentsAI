from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Optional, List
from app.clickhouse import get_overview
from app.schemas.overview import OverviewResponse

router = APIRouter(prefix="/api/dashboards", tags=["dashboards"])


@router.get("/overview", response_model=OverviewResponse)
async def overview(
    date_from: Optional[datetime] = Query(
        None,
        description="Начало периода (ISO 8601)"
    ),
    date_to: Optional[datetime] = Query(
        None,
        description="Конец периода (ISO 8601)"
    ),
    sources: Optional[List[str]] = Query(
        None,
        description="Источники (можно несколько)"
    ),
    regions: Optional[List[str]] = Query(
        None,
        description="Регионы (можно несколько)"
    ),
    sentiment: Optional[List[str]] = Query(
        None,
        description="Тональность: positive, neutral, negative (можно несколько)" # noqa
    )
):
    # Валидация sentiment
    if sentiment:
        valid_sentiments = {"positive", "neutral", "negative"}
        for s in sentiment:
            if s not in valid_sentiments:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid sentiment value: '{s}'. Must be one of {valid_sentiments}" # noqa
                )

    data = await get_overview(date_from, date_to, sources, regions, sentiment)
    return OverviewResponse(**data)
