"""
FastAPI application that serves:
  - REST API at /api/...  (metadata read/write)
  - Static UI files at /          (built from ui/)

The server is started by `pamde edit <file>` and lives for the duration of
the editing session.  All mutations go through the REST API and are applied
via pamde.editor.ParquetEditor backed by the Rust core.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from pamde.server.routes import metadata as metadata_router

_STATIC_DIR = Path(__file__).parent / "static"


def create_app(parquet_path: str) -> FastAPI:
    app = FastAPI(title="pamde", description="Parquet Metadata Editor API")

    # Allow the Vite dev server (localhost:5173) to reach the API during development.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://localhost:2971"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Attach parquet path to app state so routes can access it.
    app.state.parquet_path = parquet_path

    # REST API routes
    app.include_router(metadata_router.router, prefix="/api")

    # Serve built UI — only mount when the static dir exists (i.e. after `ui build`)
    if _STATIC_DIR.exists():
        app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")

    return app
