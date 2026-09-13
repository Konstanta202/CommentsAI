from pydantic import BaseModel
from typing import Dict


class OverviewResponse(BaseModel):
    total_messages: int
    sentiment_breakdown: Dict[str, int]
    sources_breakdown: Dict[str, int]
    regions_breakdown: Dict[str, int]
