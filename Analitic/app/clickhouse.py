import asyncio
from typing import Optional, List
from datetime import datetime
from clickhouse_driver import Client
from app.config import settings


def _get_sync_client():
    """Создаёт синхронного клиента ClickHouse."""
    return Client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        user=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DATABASE
    )


def _sync_check():
    """Синхронная проверка подключения."""
    client = _get_sync_client()
    try:
        result = client.execute("SELECT 1")
        return result[0][0] == 1
    finally:
        client.disconnect()


async def check_connection():
    """Асинхронная обёртка над синхронной проверкой."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_check)


def build_where_clause(
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        sources: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        sentiment: Optional[List[str]] = None
):
    conditions = []
    if date_from:
        conditions.append(f"created_at >= '{date_from.isoformat()}'")
    if date_to:
        conditions.append(f"created_at <= '{date_to.isoformat()}'")
    if sources:
        sources_escaped = [f"'{s}'" for s in sources]
        conditions.append(f"source IN ({','.join(sources_escaped)})")
    if regions:
        regions_escaped = [f"'{s}'" for s in regions]
        conditions.append(f"region IN ({','.join(regions_escaped)})")
    if sentiment:
        sentiment_escaped = [f"'{s}'" for s in sentiment]
        conditions.append(f"sentiment IN ({','.join(sentiment_escaped)})")

    return " AND ".join(conditions) if conditions else "1=1"


def _sync_get_overview(where: str):
    client = _get_sync_client()
    try:
        total = client.execute(
            f"SELECT count() FROM messages WHERE {where}"
            )[0][0]

        sentiment_rows = client.execute(
            f"SELECT sentiment, count() FROM messages WHERE {where} GROUP BY sentiment" # noqa
        )
        sentiment_breakdown = {row[0]: row[1] for row in sentiment_rows}

        sources_rows = client.execute(
            f"SELECT source, count() FROM messages WHERE {where} GROUP BY source" # noqa
        )
        sources_breakdown = {row[0]: row[1] for row in sources_rows}

        regions_rows = client.execute(
            f"SELECT region, count() FROM messages WHERE {where} GROUP BY region" # noqa
        )
        regions_breakdown = {row[0]: row[1] for row in regions_rows}

        return {
            "total_messages": total,
            "sentiment_breakdown": sentiment_breakdown,
            "sources_breakdown": sources_breakdown,
            "regions_breakdown": regions_breakdown
        }
    finally:
        client.disconnect()


async def get_overview(
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    sources: Optional[List[str]] = None,
    regions: Optional[List[str]] = None,
    sentiment: Optional[List[str]] = None
) -> dict:
    """Асинхронная обёртка для получения overview."""
    where = build_where_clause(date_from, date_to, sources, regions, sentiment)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync_get_overview, where)


def _sync_get_distinct_sources():
    client = _get_sync_client()
    try:
        rows = client.execute("SELECT DISTINCT source FROM messages WHERE source IS NOT NULL ORDER BY source") # noqa
        return [row[0] for row in rows]
    finally:
        client.disconnect()


def _sync_get_distinct_regions():
    client = _get_sync_client()
    try:
        rows = client.execute("SELECT DISTINCT region FROM messages WHERE region IS NOT NULL ORDER BY region") # noqa
        return [row[0] for row in rows]
    finally:
        client.disconnect()


async def get_filters_options():
    loop = asyncio.get_event_loop()
    sources, regions = await asyncio.gather(
        loop.run_in_executor(None, _sync_get_distinct_sources),
        loop.run_in_executor(None, _sync_get_distinct_regions)
    )
    return {
        "sources": sources,
        "regions": regions,
        "sentiment": ["positive", "neutral", "negative"]
    }
