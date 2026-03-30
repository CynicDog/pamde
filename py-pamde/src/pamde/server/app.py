"""
FastAPI application that serves:
  - REST API at /api/...  (metadata read/write)
  - Static UI files at /  (built from ui/)

Started by `pamde edit <file>` (file pre-loaded) or `pamde run` (upload mode).
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from pamde.server.routes import metadata as metadata_router

_STATIC_DIR = Path(__file__).parent / "static"


def create_app(parquet_path: str | None = None) -> FastAPI:
    app = FastAPI(title="pamde", description="Parquet Metadata Editor API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://localhost:2971"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.parquet_path = parquet_path
    app.state.mode = "edit" if parquet_path else "run"

    app.include_router(metadata_router.router, prefix="/api")

    if _STATIC_DIR.exists():
        app.mount("/", StaticFiles(directory=str(_STATIC_DIR), html=True), name="ui")

    return app
