import json
import os
from datetime import datetime
import asyncio
from typing import Optional
from redis.asyncio import Redis
from ingest.config import REDIS_URL, STREAM_KEY, CONSUMER_GROUP
from ingest.logging_conf import logger

async def get_redis() -> Redis:
    return Redis.from_url(REDIS_URL, decode_responses=True)

async def enqueue_batch(request_id: str, client_request_id: Optional[str], mb_ip: str, sent_at: datetime, items: list) -> None:
    redis = await get_redis()
    items_json = json.dumps([item.model_dump(mode="json") for item in items])
    message = {
        "request_id": request_id,
        "client_request_id": client_request_id or "",
        "mb_ip": mb_ip,
        "sent_at": sent_at.isoformat(),
        "items_json": items_json
    }
    try:
        await redis.xadd(STREAM_KEY, message)
        logger.info("Enqueued batch", request_id=request_id, client_request_id=client_request_id, mb_ip=mb_ip, item_count=len(items))
    except Exception as e:
        logger.error("Failed to enqueue batch", exc_info=e, request_id=request_id)
        raise

async def consume_and_process(processor_func):
    redis = await get_redis()
    consumer_name = f"worker-{os.getpid()}"
    try:
        await redis.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise
    while True:
        try:
            messages = await redis.xreadgroup(CONSUMER_GROUP, consumer_name, {STREAM_KEY: ">"}, count=10, block=5000)
            if messages:
                for stream, msgs in messages:
                    for msg_id, msg in msgs:
                        try:
                            await processor_func(msg)
                            await redis.xack(STREAM_KEY, CONSUMER_GROUP, msg_id)
                            await redis.xdel(STREAM_KEY, msg_id)
                        except Exception as e:
                            logger.error("Processing failed; retrying", exc_info=e, msg_id=msg_id)
                            await asyncio.sleep(1)  # Simple backoff
        except Exception as e:
            logger.error("Consumer error; retrying", exc_info=e)
            await asyncio.sleep(5)  # Backoff on connection issues
