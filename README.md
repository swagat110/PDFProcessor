# PDF Processor — React + FastAPI + Redis

Dockerized full-stack app: **React** (Vite) frontend, **FastAPI** backend, **Redis** (with Streams) datastore, and a **worker** for async PDF processing.

## Features

- **PDF upload & parsing:** Choose **PyPDF** (text extraction per page), **Google Gemini 2.0 Flash** (extract and convert to Markdown), or **Mistral OCR** (per-page Markdown via Mistral; summary still via Gemini).
- **Async processing:** Uploads are queued via Redis Streams; you can upload more PDFs while others are being parsed.
- **Summaries:** Gemini 2.0 Flash generates a summary for every document (used for all parsing methods).
- **Document list & detail:** After parsing, click a filename to see the summary and per-page text (PyPDF) or Markdown (Gemini / Mistral OCR).

## Prerequisites

- Docker and Docker Compose
- For Gemini parsing/summary: set `GEMINI_API_KEY` in a `.env` file at the project root (see `.env.example`).
- For Mistral OCR parsing: set `MISTRAL_API_KEY` in `.env` (summary still uses Gemini).

## Run the app

From the project root:

```bash
docker compose up --build
```

- **Frontend:** http://localhost (port 80)
- **Backend API:** http://localhost:8000 (direct) or via frontend at `/api`
- **Redis:** localhost:6379 (for CLI or other clients)

## Services

| Service   | Port | Description                          |
|----------|------|--------------------------------------|
| frontend | 80   | React SPA (nginx)                    |
| backend  | 8000 | FastAPI + Redis                      |
| worker   | —    | Redis Streams consumer (PDF parsing) |
| redis    | 6379 | Redis 7 Alpine                       |

## Development

- **Frontend (local):** `cd frontend && npm install && npm run dev` — dev server with proxy to backend at `/api`.
- **Backend (local):** `cd backend && pip install -r requirements.txt && uvicorn app.main:app --reload` — set `REDIS_URL=redis://localhost:6379/0` and ensure upload dir exists.
- **Worker (local):** `cd backend && pip install -r worker_requirements.txt && python worker.py` — set `REDIS_URL`, `UPLOAD_DIR`, `GEMINI_API_KEY`, and `MISTRAL_API_KEY` (for Mistral OCR).
- **Redis (local):** `docker run -d -p 6379:6379 redis:7-alpine` or use the one from `docker compose up redis -d`.

## API

- `GET /health` — Health check (backend + Redis).
- `POST /documents/upload` — Form: `file` (PDF), `parsing_method` (`pypdf`, `gemini`, or `mistral`). Returns `job_id` and `status: queued`.
- `GET /documents` — List all document jobs (job_id, filename, method, status).
- `GET /documents/{job_id}` — Get document result: summary and `pages` (per-page text or Markdown; method is PyPDF, Gemini, or Mistral OCR).
