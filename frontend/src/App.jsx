import { useState, useEffect, useCallback } from 'react'

const API_BASE = import.meta.env.VITE_API_URL || '/api'
const PENDING_JOB_TIMEOUT_MS = 3 * 60 * 1000

function formatUploadedAt(value) {
  if (value == null || value === '') return ''
  const d = new Date(value)
  return isNaN(d.getTime()) ? String(value) : d.toLocaleString()
}

export default function App() {
  const [message, setMessage] = useState('')
  const [documents, setDocuments] = useState([])
  const [selectedDoc, setSelectedDoc] = useState(null)
  const [currentPageIndex, setCurrentPageIndex] = useState(0)
  const [uploading, setUploading] = useState(false)
  const [uploadMethod, setUploadMethod] = useState('pypdf')
  const [polling, setPolling] = useState(false)
  const [pendingJobId, setPendingJobId] = useState(null)

  useEffect(() => {
    fetch(`${API_BASE}/health`)
      .then((r) => r.json())
      .then((data) => setMessage(data.message || ''))
      .catch(() => setMessage('Backend unreachable'))
  }, [])

  const fetchDocuments = useCallback(() => {
    fetch(`${API_BASE}/documents`)
      .then((r) => r.json())
      .then((data) => setDocuments(data.documents || []))
      .catch(() => setDocuments([]))
  }, [])

  useEffect(() => {
    fetchDocuments()
  }, [fetchDocuments])

  useEffect(() => {
    if (!polling) return
    const t = setInterval(fetchDocuments, 2000)
    return () => clearInterval(t)
  }, [polling, fetchDocuments])

  useEffect(() => {
    if (!pendingJobId) return
    const jobId = pendingJobId
    const deadline = Date.now() + PENDING_JOB_TIMEOUT_MS
    const checkJob = () => {
      if (Date.now() > deadline) {
        setPendingJobId(null)
        setPolling(false)
        fetchDocuments()
        alert('Parsing is taking too long. The request may have timed out. Please try again.')
        return
      }
      fetch(`${API_BASE}/documents/${jobId}`)
        .then((r) => {
          if (r.status === 404) {
            setPendingJobId(null)
            setPolling(false)
            fetchDocuments()
            alert('Document result no longer available.')
            return null
          }
          return r.json()
        })
        .then((data) => {
          if (data == null) return
          if (data.status === 'completed') {
            setPendingJobId(null)
            fetchDocuments()
          } else if (data.status === 'failed') {
            setPendingJobId(null)
            fetchDocuments()
            alert(data.error || 'Parsing failed.')
          }
        })
        .catch(() => {})
    }
    const t = setInterval(checkJob, 2000)
    checkJob()
    return () => clearInterval(t)
  }, [pendingJobId, fetchDocuments])

  const hasPending = documents.some((d) => d.status === 'queued' || d.status === 'processing')
  useEffect(() => {
    if (hasPending && !polling) setPolling(true)
    if (!hasPending && polling) setPolling(false)
  }, [hasPending, polling])

  const handleUpload = (e) => {
    e.preventDefault()
    const fileInput = e.target.elements?.pdf
    const file = fileInput?.files?.[0]
    if (!file || !file.name.toLowerCase().endsWith('.pdf')) {
      alert('Please select a PDF file.')
      return
    }
    setUploading(true)
    const form = new FormData()
    form.append('file', file)
    form.append('parsing_method', uploadMethod)
    fetch(`${API_BASE}/documents/upload`, {
      method: 'POST',
      body: form,
    })
      .then((r) => r.json())
      .then((data) => {
        fileInput.value = ''
        setPolling(true)
        setPendingJobId(data.job_id || null)
        fetchDocuments()
      })
      .catch(() => alert('Upload failed'))
      .finally(() => setUploading(false))
  }

  const openDocument = (jobId) => {
    fetch(`${API_BASE}/documents/${jobId}`)
      .then((r) => r.json())
      .then((data) => {
        setSelectedDoc(data)
        setCurrentPageIndex(0)
      })
      .catch(() => setSelectedDoc(null))
  }

  const closeDocument = () => setSelectedDoc(null)

  const pages = selectedDoc?.pages?.length
    ? selectedDoc.pages
    : selectedDoc?.content
      ? [selectedDoc.content]
      : []
  const totalPages = pages.length
  const canPrev = totalPages > 0 && currentPageIndex > 0
  const canNext = totalPages > 0 && currentPageIndex < totalPages - 1
  const goPrev = () => setCurrentPageIndex((i) => Math.max(0, i - 1))
  const goNext = () => setCurrentPageIndex((i) => Math.min(totalPages - 1, i + 1))

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-inner">
          <h1>PDF Processor</h1>
          <p className="subtitle">React + FastAPI + Redis</p>
          <p className="status">{message}</p>
        </div>
      </header>

      <main className="main-content">
        <section className="upload-section">
          <h2>PDF documents</h2>
          <form onSubmit={handleUpload} className="upload-form">
            <input
              type="file"
              name="pdf"
              accept=".pdf"
              required
              disabled={uploading}
            />
            <div className="method-row">
              <label>
                <input
                  type="radio"
                  name="method"
                  value="pypdf"
                  checked={uploadMethod === 'pypdf'}
                  onChange={(e) => setUploadMethod(e.target.value)}
                  disabled={uploading}
                />
                PyPDF (text)
              </label>
              <label>
                <input
                  type="radio"
                  name="method"
                  value="gemini"
                  checked={uploadMethod === 'gemini'}
                  onChange={(e) => setUploadMethod(e.target.value)}
                  disabled={uploading}
                />
                Gemini 2.0 Flash (markdown)
              </label>
              <label>
                <input
                  type="radio"
                  name="method"
                  value="mistral"
                  checked={uploadMethod === 'mistral'}
                  onChange={(e) => setUploadMethod(e.target.value)}
                  disabled={uploading}
                />
                Mistral OCR (markdown)
              </label>
            </div>
            <button type="submit" disabled={uploading}>
              {uploading ? 'Uploading…' : 'Upload PDF'}
            </button>
          </form>
          <p className="hint">You can upload more PDFs while others are being parsed.</p>
        </section>
        <section className="documents-section">
          <h2>Document list</h2>
          {documents.length > 0 ? (
            <ul className="doc-list">
              {documents.map((d) => (
                <li key={d.job_id}>
                  <button
                    type="button"
                    className={`doc-link ${selectedDoc?.job_id === d.job_id ? 'doc-link-selected' : ''}`}
                    onClick={() => openDocument(d.job_id)}
                  >
                    <span className="doc-link-main">
                      <span className="doc-filename">{d.filename}</span>
                      <span className="doc-meta">
                        {d.method === 'gemini' ? 'Gemini 2.0 Flash' : d.method === 'mistral' ? 'Mistral OCR' : 'PyPDF'}
                        {d.uploaded_at != null && d.uploaded_at !== '' && (
                          <> · {formatUploadedAt(d.uploaded_at)}</>
                        )}
                      </span>
                    </span>
                    <span className={`status-badge status-${d.status}`}>{d.status}</span>
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <p className="hint">No documents yet. Upload a PDF above.</p>
          )}
        </section>
      </main>

      {selectedDoc && (
        <section className="document-view">
          <div className="document-view-header">
            <h2>{selectedDoc.filename}</h2>
            <button type="button" className="close-btn" onClick={closeDocument} title="Close">
              ×
            </button>
          </div>
          <div className="document-view-body">
            {selectedDoc.status === 'completed' ? (
              <>
                <div className="document-summary-block">
                  <h3>Summary</h3>
                  <div className="summary-box">
                    {selectedDoc.summary || '—'}
                  </div>
                </div>
                <div className="document-pages-block">
                  <h3>{selectedDoc.method === 'pypdf' ? 'Extracted text (per page)' : 'Markdown (per page)'}</h3>
                  {totalPages > 0 ? (
                    <>
                      <div className="page-nav">
                        <button
                          type="button"
                          className="page-nav-btn"
                          onClick={goPrev}
                          disabled={!canPrev}
                        >
                          ← Prev
                        </button>
                        <span className="page-nav-label">
                          Page {currentPageIndex + 1} of {totalPages}
                        </span>
                        <button
                          type="button"
                          className="page-nav-btn"
                          onClick={goNext}
                          disabled={!canNext}
                        >
                          Next →
                        </button>
                      </div>
                      <div className="page-content">
                        <pre className="page-content-inner">{pages[currentPageIndex] || '(empty)'}</pre>
                      </div>
                    </>
                  ) : (
                    <pre className="content-box markdown-content">{selectedDoc.content || '—'}</pre>
                  )}
                </div>
              </>
            ) : (
              <p className="muted">Status: {selectedDoc.status}. Wait for parsing to finish.</p>
            )}
          </div>
        </section>
      )}
    </div>
  )
}
