import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis

from .config import settings

STREAM_DOCUMENTS = "documents:queue"
KEY_DOCUMENT_IDS = "documents:ids"
KEY_DOCUMENT_RESULT = "document:result:{}"

REDIS_CONNECT_RETRIES = 10
REDIS_CONNECT_DELAY = 2


@asynccontextmanager
async def lifespan(app: FastAPI):
    r = redis.from_url(settings.redis_url, decode_responses=True)
    for attempt in range(1, REDIS_CONNECT_RETRIES + 1):
        try:
            await r.ping()
            break
        except (redis.ConnectionError, OSError) as e:
            if attempt == REDIS_CONNECT_RETRIES:
                raise RuntimeError(f"Could not connect to Redis at {settings.redis_url} after {REDIS_CONNECT_RETRIES} attempts") from e
            await asyncio.sleep(REDIS_CONNECT_DELAY)
    app.state.redis = r
    os.makedirs(settings.upload_dir, exist_ok=True)
    try:
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(title="EPAM API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    try:
        r = app.state.redis
        await r.ping()
        return {"status": "ok", "message": "Backend + Redis connected"}
    except Exception:
        return {"status": "degraded", "message": "Backend up, Redis unreachable"}


# --- Document upload & list ---

ALLOWED_PARSING_METHODS = {"pypdf", "gemini", "mistral"}


@app.post("/documents/upload")
async def upload_document(
    file: UploadFile = File(...),
    parsing_method: str = Form("pypdf"),
):
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF file required")
    if parsing_method not in ALLOWED_PARSING_METHODS:
        raise HTTPException(
            status_code=400,
            detail=f"parsing_method must be one of: {sorted(ALLOWED_PARSING_METHODS)}",
        )
    job_id = str(uuid.uuid4())
    file_path = os.path.join(settings.upload_dir, f"{job_id}.pdf")
    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)
    uploaded_at = datetime.now(timezone.utc).isoformat()
    r = app.state.redis
    await r.xadd(
        STREAM_DOCUMENTS,
        {
            "job_id": job_id,
            "filename": file.filename or "document.pdf",
            "method": parsing_method,
            "file_path": file_path,
            "uploaded_at": uploaded_at,
        },
        maxlen=10000,
    )
    await r.rpush(KEY_DOCUMENT_IDS, job_id)
    await r.set(
        KEY_DOCUMENT_RESULT.format(job_id),
        json.dumps(
            {
                "job_id": job_id,
                "filename": file.filename or "document.pdf",
                "method": parsing_method,
                "status": "queued",
                "uploaded_at": uploaded_at,
            }
        ),
    )
    return {"job_id": job_id, "filename": file.filename, "status": "queued"}


@app.get("/documents")
async def list_documents():
    r = app.state.redis
    ids = await r.lrange(KEY_DOCUMENT_IDS, 0, -1)
    ids = list(reversed(ids))
    results = []
    for job_id in ids:
        raw = await r.get(KEY_DOCUMENT_RESULT.format(job_id))
        if raw:
            try:
                data = json.loads(raw)
                results.append(
                    {
                        "job_id": data.get("job_id", job_id),
                        "filename": data.get("filename", "?"),
                        "method": data.get("method", "?"),
                        "status": data.get("status", "unknown"),
                        "uploaded_at": data.get("uploaded_at"),
                    }
                )
            except json.JSONDecodeError:
                results.append(
                    {"job_id": job_id, "filename": "?", "method": "?", "status": "unknown", "uploaded_at": None}
                )
    return {"documents": results}


@app.get("/documents/{job_id}")
async def get_document(job_id: str):
    r = app.state.redis
    raw = await r.get(KEY_DOCUMENT_RESULT.format(job_id))
    if not raw:
        raise HTTPException(status_code=404, detail="Document not found")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Invalid document data")
    return data
