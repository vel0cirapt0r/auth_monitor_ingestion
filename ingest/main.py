import uuid
from datetime import datetime
from typing import Optional
import gzip
import orjson
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import ORJSONResponse
from ingest.schemas import IngestRequest, TestRequest, HealthResponse, IngestResponse, TestResponse, ErrorDetail
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
        ts=datetime.utcnow().isoformat() + "Z",
        version=APP_VERSION,
    )

async def process_request(request: Request, model: type, enqueue: bool = False):
    content_encoding = request.headers.get("Content-Encoding", "identity").lower()
    content_length = int(request.headers.get("Content-Length", 0))
    if content_length > MAX_BODY_SIZE:
        raise HTTPException(status_code=413, detail="Payload Too Large")
    body = await request.body()
    if content_encoding == "gzip":
        try:
            body = gzip.decompress(body)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid gzip")
    try:
        data = orjson.loads(body)
    except orjson.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    request_id = str(uuid.uuid4())
    mb_ip = request.headers.get("X-Real-IP") or request.client.host
    logger_bound = logger.bind(request_id=request_id, mb_ip=mb_ip)
    errors = []
    accepted = 0
    received = len(data.get("items", []))
    valid_items = []
    try:
        parsed = model.model_validate(data)
        if parsed.items:
            for idx, item in enumerate(parsed.items):
                try:
                    # Item already validated by Pydantic
                    valid_items.append(item)
                    accepted += 1
                except Exception as e:
                    errors.append(ErrorDetail(index=idx, code="validation_error", detail=str(e)))
                    if len(errors) >= 20:
                        break
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    rejected = received - accepted
    if enqueue and valid_items:
        try:
            await enqueue_batch(request_id, parsed.client_request_id, mb_ip, parsed.sent_at, valid_items)
        except Exception:
            raise HTTPException(status_code=500, detail="Internal Server Error")
    response_data = {
        "request_id": request_id,
        "client_request_id": parsed.client_request_id,
        "mb_ip": mb_ip,
        "received": received,
        "accepted": accepted,
        "rejected": rejected,
        "errors": errors[:20]
    }
    logger_bound.info("Processed request", accepted=accepted, rejected=rejected, errors_len=len(errors))
    return response_data, content_length, content_encoding

@app.post("/v1/ingest", status_code=202)
async def ingest(request: Request):
    data, _, _ = await process_request(request, IngestRequest, enqueue=True)
    return IngestResponse(**data, schema_version=SCHEMA_VERSION)

@app.post("/v1/ingest/test")
async def ingest_test(request: Request):
    data, content_length, content_encoding = await process_request(request, TestRequest)
    mode = "ping" if not data["received"] else "validate"
    return TestResponse(**data, schema_version=SCHEMA_VERSION, mode=mode, content_length=content_length, content_encoding=content_encoding)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
