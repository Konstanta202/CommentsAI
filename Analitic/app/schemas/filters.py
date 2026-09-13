from pydantic import BaseModel
from typing import List


class FiltersOptionsResponse(BaseModel):
    sources: List[str]
    regions: List[str]
    sentiment: List[str] = ["positive", "neutral", "negative"]
