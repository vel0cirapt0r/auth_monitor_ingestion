import asyncio
from datetime import datetime, timezone
from typing import Dict, Any
from tortoise import Model, fields, Tortoise
from tortoise.exceptions import IntegrityError
from tortoise.transactions import in_transaction
from ingest.queue import consume_and_process
from ingest.config import DATABASE_URL
from ingest.logging_conf import logger
import json


class Device(Model):
    id = fields.IntField(pk=True)
    serial_number = fields.CharField(max_length=255, unique=True)
    name = fields.CharField(max_length=255, default="")
    location = fields.TextField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "device_registry_device"


class DeviceProtocol(Model):
    id = fields.IntField(pk=True)
    device = fields.ForeignKeyField("models.Device", related_name="protocols")
    protocol_type = fields.CharField(max_length=255)
    mb_ip = fields.CharField(max_length=255)  # inet in DB, but str for ORM
    token = fields.TextField()
    token_created_at = fields.DatetimeField()
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "device_registry_deviceprotocol"
        unique_together = ("device_id", "protocol_type")


async def init_tortoise():
    await Tortoise.init(
        db_url=DATABASE_URL,
        modules={"models": ["__main__"]}
    )
    await Tortoise.generate_schemas(safe=True)


async def process_batch(msg: Dict[str, Any]):
    request_id = msg["request_id"]
    client_request_id = msg.get("client_request_id") or None
    mb_ip = msg["mb_ip"]
    sent_at = datetime.fromisoformat(msg["sent_at"])
    items = json.loads(msg["items_json"])
    logger_bound = logger.bind(request_id=request_id, client_request_id=client_request_id, mb_ip=mb_ip,
                               item_count=len(items))

    device_stats = {"created": 0, "updated": 0, "noop": 0}
    protocol_stats = {"created": 0, "updated": 0, "noop": 0}
    errors = []

    for idx, item in enumerate(items):
        serial_number = item["serial_number"]
        location = item["location"] if item["location"] else None
        protocol_type = item["protocol_type"]
        token = item["token"]
        token_created_at = datetime.fromisoformat(item["token_created_at"]).astimezone(timezone.utc)

        try:
            async with in_transaction():
                device, created = await Device.get_or_create(serial_number=serial_number,
                                                             defaults={"name": "", "location": location})
                if created:
                    device_stats["created"] += 1
                elif location and device.location != location:
                    device.location = location
                    await device.save()
                    device_stats["updated"] += 1
                else:
                    device_stats["noop"] += 1

                protocol, created = await DeviceProtocol.get_or_create(
                    device=device, protocol_type=protocol_type,
                    defaults={
                        "mb_ip": mb_ip,
                        "token": token,
                        "token_created_at": token_created_at
                    }
                )
                if created:
                    protocol_stats["created"] += 1
                elif token_created_at > protocol.token_created_at:
                    protocol.mb_ip = mb_ip
                    protocol.token = token
                    protocol.token_created_at = token_created_at
                    await protocol.save()
                    protocol_stats["updated"] += 1
                else:
                    protocol_stats["noop"] += 1
        except IntegrityError as e:
            logger_bound.warning("Integrity error; retrying", exc_info=e, idx=idx)
            await asyncio.sleep(0.1)  # Short backoff for race
            continue  # Or retry logic here if needed
        except Exception as e:
            errors.append({"idx": idx, "error": str(e)})
            logger_bound.error("Item processing failed", exc_info=e, idx=idx)

    logger_bound.info(
        "Batch processed",
        device_stats=device_stats,
        protocol_stats=protocol_stats,
        errors_summary=[e["error"] for e in errors] if errors else None
    )


async def main():
    await init_tortoise()
    await consume_and_process(process_batch)


if __name__ == "__main__":
    asyncio.run(main())
