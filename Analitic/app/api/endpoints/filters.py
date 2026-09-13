from fastapi import APIRouter
from app.clickhouse import get_filters_options
from app.schemas.filters import FiltersOptionsResponse

router = APIRouter(prefix="/api/v1/filters", tags=["filters"])


@router.get("/options", response_model=FiltersOptionsResponse)
async def filters_options():
    data = await get_filters_options()
    return FiltersOptionsResponse(**data)
