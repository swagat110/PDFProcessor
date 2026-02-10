"""
Redis Streams consumer: processes PDF jobs (PyPDF, Gemini, or Mistral OCR parsing + Gemini summary).
Run from backend dir; expects REDIS_URL, UPLOAD_DIR, GEMINI_API_KEY, MISTRAL_API_KEY (for mistral) in env.
"""
import base64
import json
import logging
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)
from pydantic_settings import BaseSettings
from pypdf import PdfReader

STREAM_DOCUMENTS = "documents:queue"
KEY_DOCUMENT_IDS = "documents:ids"
KEY_DOCUMENT_RESULT = "document:result:{}"
CONSUMER_GROUP = "documents_workers"
CONSUMER_NAME = "worker1"
BLOCK_MS = 5000
GEMINI_TIMEOUT_SEC = 90
GEMINI_MODELS = ("gemini-2.0-flash", "gemini-2.5-flash", "gemini-2.5-flash-lite")


def _is_throttle_error(exc: Exception) -> bool:
    """True if the exception looks like a rate limit / quota error (e.g. 429)."""
    msg = str(exc).lower()
    return (
        "429" in msg
        or "resource exhausted" in msg
        or "quota" in msg
        or "rate limit" in msg
        or "too many requests" in msg
    )


def _run_with_timeout(func, timeout_sec: int = GEMINI_TIMEOUT_SEC):
    """Run func() in a thread; raise TimeoutError if it exceeds timeout_sec."""
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(func)
        try:
            return future.result(timeout=timeout_sec)
        except FuturesTimeoutError:
            raise TimeoutError("Gemini request timed out")


def _is_error_message(s: str) -> bool:
    """True if the string is an error message from our helpers."""
    if not s or not s.strip():
        return False
    s = s.strip()
    return (
        s.startswith("[Gemini error:")
        or s.startswith("[Summary error:")
        or s.startswith("[GEMINI_API_KEY")
        or s.startswith("[Mistral error:")
    )


class WorkerSettings(BaseSettings):
    redis_url: str = "redis://redis:6379/0"
    upload_dir: str = "/data/uploads"
    gemini_api_key: str = ""
    mistral_api_key: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


def get_gemini_summary(text: str, api_key: str) -> str:
    if not api_key or not text.strip():
        return ""
    text_slice = text[:150000]
    last_error = None
    for model_name in GEMINI_MODELS:
        def _do(_name=model_name):
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(_name)
            response = model.generate_content(
                "Summarize the following document in a few clear paragraphs. "
                "Do not add preamble, just the summary.\n\n" + text_slice
            )
            return (response.text or "").strip()
        try:
            return _run_with_timeout(_do)
        except Exception as e:
            last_error = e
            if _is_throttle_error(e):
                continue
            return f"[Summary error: {e}]"
    return f"[Summary error: {last_error}]"


def _split_markdown_into_pages(raw: str) -> list[str]:
    """Split Gemini output by ---PAGE N--- delimiters into a list of per-page markdown."""
    if not raw or not raw.strip():
        return []
    # Split by ---PAGE n--- (case-insensitive, n = 1, 2, ...)
    parts = re.split(r"---PAGE\s+\d+---", raw.strip(), flags=re.IGNORECASE)
    pages = [p.strip() for p in parts if p.strip()]
    return pages if pages else [raw.strip()]


def get_gemini_markdown_pages_and_summary(pdf_path: str, api_key: str) -> tuple[list[str], str]:
    """Use Gemini to extract PDF as markdown per page and get a summary. Returns (pages, summary)."""
    if not api_key:
        return [], "[GEMINI_API_KEY not set]"
    prompt = (
        "Extract all text and structure from this PDF as clean Markdown **per page**. "
        "For each page, output exactly the line ---PAGE N--- where N is the page number (1, 2, 3, ...), "
        "then the markdown for that page. Use only ---PAGE N--- as page separators. "
        "Preserve headings, lists, and paragraphs. No other preamble or explanation."
    )
    prompt_fallback = (
        "Turn the following raw text (already split by ---PAGE N---) into clean Markdown per page. "
        "Keep the exact ---PAGE N--- lines. Output only the markdown for each page under its ---PAGE N---. "
        "No other preamble.\n\n"
    )
    last_error = None
    for model_name in GEMINI_MODELS:
        def _do(_name=model_name):
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(_name)
            try:
                file = genai.upload_file(pdf_path, mime_type="application/pdf")
                response = model.generate_content([file, prompt])
            except Exception:
                pypdf_pages = parse_pypdf(pdf_path)
                numbered = "\n\n".join(f"---PAGE {i+1}---\n{p}" for i, p in enumerate(pypdf_pages))
                response = model.generate_content(prompt_fallback + numbered[:120000])
            raw_markdown = (response.text or "").strip()
            pages = _split_markdown_into_pages(raw_markdown)
            full_markdown = "\n\n".join(pages) if pages else raw_markdown
            summary = get_gemini_summary(full_markdown, api_key)
            return pages, summary
        try:
            return _run_with_timeout(_do)
        except Exception as e:
            last_error = e
            if _is_throttle_error(e):
                continue
            return [], f"[Gemini error: {e}]"
    return [], f"[Gemini error: {last_error}]"


def parse_pypdf(pdf_path: str) -> list[str]:
    """Extract text per page using PyPDF."""
    reader = PdfReader(pdf_path)
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        pages.append(text)
    return pages


MISTRAL_OCR_TIMEOUT_SEC = 120
MISTRAL_OCR_MODEL = "mistral-ocr-latest"


def get_mistral_ocr_pages(pdf_path: str, api_key: str) -> tuple[list[str], str | None]:
    """Use Mistral OCR to extract PDF as markdown per page. Returns (pages, error_message or None)."""
    if not api_key:
        return [], "[Mistral error: MISTRAL_API_KEY not set]"
    try:
        with open(pdf_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
    except OSError as e:
        return [], f"[Mistral error: {e}]"
    document = {
        "type": "document_url",
        "document_url": f"data:application/pdf;base64,{b64}",
    }

    def _do():
        from mistralai import Mistral
        client = Mistral(api_key=api_key)
        response = client.ocr.process(model=MISTRAL_OCR_MODEL, document=document)
        pages = []
        for p in (response.pages or []):
            md = getattr(p, "markdown", None) or (p.get("markdown") if isinstance(p, dict) else None) or ""
            pages.append(md)
        return pages

    try:
        pages = _run_with_timeout(_do, timeout_sec=MISTRAL_OCR_TIMEOUT_SEC)
        return pages, None
    except TimeoutError as e:
        return [], f"[Mistral error: {e}]"
    except Exception as e:
        return [], f"[Mistral error: {e}]"


def process_job(
    job_id: str,
    filename: str,
    method: str,
    file_path: str,
    gemini_key: str,
    mistral_key: str = "",
) -> dict:
    if method == "pypdf":
        pages = parse_pypdf(file_path)
        full_text = "\n\n".join(pages)
        summary = get_gemini_summary(full_text, gemini_key)
        if _is_error_message(summary):
            return {
                "job_id": job_id,
                "filename": filename,
                "method": "pypdf",
                "status": "failed",
                "summary": "",
                "error": summary,
                "pages": [],
            }
        return {
            "job_id": job_id,
            "filename": filename,
            "method": "pypdf",
            "status": "completed",
            "summary": summary,
            "pages": pages,
        }
    elif method == "mistral":
        pages, err = get_mistral_ocr_pages(file_path, mistral_key)
        if err is not None:
            return {
                "job_id": job_id,
                "filename": filename,
                "method": "mistral",
                "status": "failed",
                "summary": "",
                "error": err,
                "pages": [],
            }
        full_text = "\n\n".join(pages)
        summary = get_gemini_summary(full_text, gemini_key)
        if _is_error_message(summary):
            return {
                "job_id": job_id,
                "filename": filename,
                "method": "mistral",
                "status": "failed",
                "summary": "",
                "error": summary,
                "pages": [],
            }
        return {
            "job_id": job_id,
            "filename": filename,
            "method": "mistral",
            "status": "completed",
            "summary": summary,
            "pages": pages,
        }
    else:
        pages, summary = get_gemini_markdown_pages_and_summary(file_path, gemini_key)
        if not pages and _is_error_message(summary):
            return {
                "job_id": job_id,
                "filename": filename,
                "method": "gemini",
                "status": "failed",
                "summary": "",
                "error": summary,
                "pages": [],
            }
        return {
            "job_id": job_id,
            "filename": filename,
            "method": "gemini",
            "status": "completed",
            "summary": summary,
            "pages": pages,
        }


def run_worker():
    cfg = WorkerSettings()
    if not cfg.gemini_api_key:
        cfg.gemini_api_key = os.environ.get("GEMINI_API_KEY", "")
    if not cfg.mistral_api_key:
        cfg.mistral_api_key = os.environ.get("MISTRAL_API_KEY", "")
    log.info(
        "Worker starting: redis=%s upload_dir=%s gemini_key_set=%s mistral_key_set=%s",
        cfg.redis_url.split("@")[-1] if "@" in cfg.redis_url else cfg.redis_url,
        cfg.upload_dir,
        bool(cfg.gemini_api_key),
        bool(cfg.mistral_api_key),
    )
    r = redis.from_url(cfg.redis_url, decode_responses=True)
    try:
        r.xgroup_create(STREAM_DOCUMENTS, CONSUMER_GROUP, id="0", mkstream=True)
        log.info("Created consumer group %s on stream %s", CONSUMER_GROUP, STREAM_DOCUMENTS)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        log.info("Consumer group %s already exists on %s", CONSUMER_GROUP, STREAM_DOCUMENTS)
    while True:
        try:
            messages = r.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {STREAM_DOCUMENTS: ">"},
                count=1,
                block=BLOCK_MS,
            )
            if not messages:
                continue
            for stream_name, stream_messages in messages:
                for msg_id, raw_fields in stream_messages:
                    if isinstance(raw_fields, list):
                        fields = dict(zip(raw_fields[::2], raw_fields[1::2]))
                    else:
                        fields = raw_fields or {}
                    job_id = fields.get("job_id")
                    filename = fields.get("filename", "document.pdf")
                    method = fields.get("method", "pypdf")
                    file_path = fields.get("file_path") or os.path.join(
                        cfg.upload_dir, f"{job_id}.pdf"
                    )
                    uploaded_at = fields.get("uploaded_at")
                    if not uploaded_at:
                        existing_raw = r.get(KEY_DOCUMENT_RESULT.format(job_id))
                        if existing_raw:
                            try:
                                existing = json.loads(existing_raw)
                                uploaded_at = existing.get("uploaded_at")
                            except (json.JSONDecodeError, TypeError):
                                pass
                    log.info("Processing job_id=%s filename=%s method=%s path=%s", job_id, filename, method, file_path)
                    processing_payload = {
                        "job_id": job_id,
                        "filename": filename,
                        "method": method,
                        "status": "processing",
                    }
                    if uploaded_at:
                        processing_payload["uploaded_at"] = uploaded_at
                    r.set(KEY_DOCUMENT_RESULT.format(job_id), json.dumps(processing_payload))
                    result = None
                    try:
                        if not os.path.isfile(file_path):
                            result = {
                                "job_id": job_id,
                                "filename": filename,
                                "method": method,
                                "status": "failed",
                                "summary": "",
                                "error": f"File not found: {file_path}",
                                "pages": [],
                            }
                            log.warning("File not found for job_id=%s: %s", job_id, file_path)
                        else:
                            result = process_job(
                                job_id,
                                filename,
                                method,
                                file_path,
                                cfg.gemini_api_key,
                                cfg.mistral_api_key,
                            )
                            log.info("Job job_id=%s status=%s", job_id, result.get("status"))
                    except Exception as e:
                        log.exception("Job job_id=%s failed: %s", job_id, e)
                        result = {
                            "job_id": job_id,
                            "filename": filename,
                            "method": method,
                            "status": "failed",
                            "summary": "",
                            "error": str(e),
                            "pages": [],
                        }
                    if result is not None:
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass
                        if uploaded_at:
                            result["uploaded_at"] = uploaded_at
                        else:
                            existing_raw = r.get(KEY_DOCUMENT_RESULT.format(job_id))
                            if existing_raw:
                                try:
                                    existing = json.loads(existing_raw)
                                    if existing.get("uploaded_at"):
                                        result["uploaded_at"] = existing["uploaded_at"]
                                except (json.JSONDecodeError, TypeError):
                                    pass
                        r.set(KEY_DOCUMENT_RESULT.format(job_id), json.dumps(result))
                        if result.get("status") == "failed":
                            r.lrem(KEY_DOCUMENT_IDS, 0, job_id)
                            r.expire(KEY_DOCUMENT_RESULT.format(job_id), 3600)
                        r.xack(STREAM_DOCUMENTS, CONSUMER_GROUP, msg_id)
                    else:
                        log.error("No result for job_id=%s; acking anyway to avoid stuck message", job_id)
                        r.xack(STREAM_DOCUMENTS, CONSUMER_GROUP, msg_id)
        except redis.ConnectionError as e:
            log.warning("Redis connection error: %s; retrying in 5s", e)
            time.sleep(5)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e) or "no such key" in str(e).lower():
                log.warning("Stream or consumer group missing (e.g. after Redis flush); recreating: %s", e)
                try:
                    r.xgroup_create(STREAM_DOCUMENTS, CONSUMER_GROUP, id="0", mkstream=True)
                    log.info("Recreated consumer group %s on stream %s", CONSUMER_GROUP, STREAM_DOCUMENTS)
                except redis.ResponseError as e2:
                    if "BUSYGROUP" not in str(e2):
                        log.exception("Failed to recreate group: %s", e2)
                        time.sleep(5)
            else:
                log.exception("Redis error: %s", e)
                time.sleep(5)
        except Exception as e:
            log.exception("Worker loop error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    run_worker()
