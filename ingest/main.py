import uuid
from datetime import datetime
from typing import Optional
import gzip
import orjson
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import ORJSONResponse
from pydantic import ValidationError
from ingest.schemas import IngestRequest, TestRequest, HealthResponse, IngestResponse, TestResponse, ErrorDetail, Item
from ingest.config import APP_VERSION, SCHEMA_VERSION, MAX_BODY_SIZE, APP_HOST, APP_PORT
from ingest.queue import enqueue_batch
from ingest.logging_conf import logger

app = FastAPI(default_response_class=ORJSONResponse)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    mb_ip = request.headers.get("X-Real-IP") or request.client.host
    logger_bound = logger.bind(request_id=request_id, mb_ip=mb_ip, method=request.method, path=request.url.path)
    start_time = datetime.utcnow()
    try:
        response = await call_next(request)
        duration = (datetime.utcnow() - start_time).total_seconds() * 1000
        logger_bound.info("Request completed", status_code=response.status_code, duration_ms=duration)
        return response
    except Exception as e:
        logger_bound.error("Request failed", exc_info=e)
        raise

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        time=datetime.utcnow().isoformat() + "Z",
        version=APP_VERSION,
    )

async def process_request(request: Request, model: type, enqueue: bool = False):
    content_encoding = request.headers.get("Content-Encoding", "identity").lower()
    content_length_header = request.headers.get("Content-Length")
    content_length = int(content_length_header) if content_length_header else None
    body = await request.body()
    if len(body) > MAX_BODY_SIZE:
        raise HTTPException(status_code=413, detail="Payload Too Large")
    if content_encoding == "gzip":
        try:
            body = gzip.decompress(body)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid gzip")
        if len(body) > MAX_BODY_SIZE:
            raise HTTPException(status_code=413, detail="Payload Too Large")
        logger.debug("Decompressed gzip body", decompressed_size=len(body))
    try:
        data = orjson.loads(body) if body else {}
    except orjson.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    request_id = str(uuid.uuid4())
    mb_ip = request.headers.get("X-Real-IP") or request.client.host
    client_request_id = data.get("client_request_id")
    logger_bound = logger.bind(request_id=request_id, client_request_id=client_request_id, mb_ip=mb_ip)
    errors = []
    received = len(data.get("items", []))
    accepted = 0
    valid_items = []
    sent_at = None
    try:
        # Validate envelope
        parsed = model.model_validate(data)
        sent_at = parsed.sent_at
        client_request_id = parsed.client_request_id
        # Per-item validation
        if parsed.items:
            for idx, raw_item in enumerate(parsed.items):
                try:
                    item = Item.model_validate(raw_item)
                    valid_items.append(item)
                    accepted += 1
                except ValidationError as e:
                    errors.append(ErrorDetail(index=idx, code="validation_error", detail=str(e)))
                    if len(errors) >= 20:
                        break
    except ValidationError as e:
        # Envelope invalid
        raise HTTPException(status_code=400, detail=str(e))
    rejected = received - accepted
    if enqueue and valid_items and sent_at:
        try:
            await enqueue_batch(request_id, client_request_id, mb_ip, sent_at, valid_items)
        except Exception:
            raise HTTPException(status_code=500, detail="Internal Server Error")
    logger_bound.info("Processed request", received=received, accepted=accepted, rejected=rejected, errors_len=len(errors))
    response_data = {
        "status": "ok",
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "client_request_id": client_request_id,
        "mb_ip": mb_ip,
        "received": received,
        "accepted": accepted,
        "rejected": rejected,
        "errors": errors[:20]
    }
    return response_data, len(body) if content_encoding != "gzip" else content_length, content_encoding

@app.post("/v1/ingest", response_model=IngestResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest(request: Request):
    data, _, _ = await process_request(request, IngestRequest, enqueue=True)
    return data

@app.post("/v1/ingest/test", response_model=TestResponse)
async def ingest_test(request: Request):
    data, content_length, content_encoding = await process_request(request, TestRequest)
    mode = "ping" if data["received"] == 0 else "validate"
    data["mode"] = mode
    data["content_length"] = content_length if isinstance(content_length, int) else 0
    data["content_encoding"] = content_encoding
    data["note"] = "dry-run; nothing enqueued or persisted"
    return data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
