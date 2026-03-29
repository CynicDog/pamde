# CLAUDE.md — pamde development guide

## Repository layout

```
pamde/
├── crates/pamde-core/       Rust: all parquet logic + PyO3 c_api
├── py-pamde/
│   ├── runtime/pamde-runtime/   maturin bridge (produces _pamde_runtime.so)
│   ├── src/pamde/               Python package (editor, cli, server)
│   └── tests/
└── ui/                      React/TypeScript SPA
```

## Three-layer rule

Code always flows in one direction:

```
pamde-core (Rust domain logic)
  → pamde-runtime (maturin cdylib, thin re-export)
    → pamde Python package (ergonomic API + CLI + server)
```

- **pamde-core** must never import from or depend on Python or the UI.
- **pamde-runtime** is a stub: `pub use pamde_core::c_api::*;`.  No logic here.
- **Python** wraps `_pamde_runtime` types in dataclasses and ergonomic methods.

## Rust crate: pamde-core

Location: `crates/pamde-core/`

- `src/metadata/mod.rs` — open a `.parquet` file, parse the footer, read/write `key_value_metadata`.
- `src/schema/mod.rs` — build `ColumnInfo` structs from the parsed metadata (one per leaf column).
- `src/c_api/mod.rs` — PyO3 `#[pyclass]`/`#[pymethods]` bindings.  Only compiled under `features = ["extension-module"]`.

### Parquet dependency

Use the `parquet` crate from apache/arrow-rs.  **Do not** add the `arrow` crate as a dependency unless actual columnar data (not just metadata) is needed.  The parquet crate can be used standalone for footer parsing.

Relevant parquet types:
- `parquet::file::metadata::ParquetMetaData` — top-level after `parse_metadata`
- `parquet::schema::types::SchemaDescriptor` — schema tree traversal
- `parquet::format::FileMetaData` — raw thrift struct (via `parquet::format`)
- `parquet::format::KeyValue` — `{ key: String, value: Option<String> }`

### Writing metadata back

Rewriting parquet metadata requires reserializing the `FileMetaData` thrift struct and patching the footer.  The process:
1. Read the file into memory (or mmap).
2. Modify the `FileMetaData`.
3. Serialize the new thrift footer.
4. Write: `[original data pages][new footer][4-byte footer length][PAR1 magic]`.

The data pages (row groups) are never touched — only the last N bytes are replaced.

## Python package: pamde

Location: `py-pamde/src/pamde/`

- `editor.py` — `ParquetEditor`: the main user-facing Python object.
- `cli.py` — `typer` CLI.  Entry point registered as `pamde` in `pyproject.toml`.
- `server/app.py` — `create_app(parquet_path)` returns a FastAPI app.
- `server/routes/metadata.py` — REST endpoints.
- `_types.pyi` — IDE stubs for `PyParquetFile` and `PyColumnInfo`.

### Import convention

```python
# In production code, import from the runtime like this:
from _pamde_runtime._pamde_runtime import PyParquetFile, PyColumnInfo

# Type-checking only (no runtime cost):
if TYPE_CHECKING:
    from _pamde_runtime._pamde_runtime import PyParquetFile, PyColumnInfo
```

## UI: ui/

Location: `ui/`

- **Stack**: React 18, TypeScript, Vite.
- **API client**: `ui/src/api.ts` — typed wrapper around fetch.
- **Main component**: `MetadataTable.tsx` — rows = parquet columns, fixed columns = read-only metadata fields, user-defined columns = editable tag cells (backed by `key_value_metadata`).

### Dev workflow

```bash
# Terminal 1: start the pamde backend (no browser auto-open)
pamde edit fixtures/fruits.parquet --no-browser

# Terminal 2: start Vite dev server
cd ui && npm run dev
# Open http://localhost:5173
```

Vite proxies `/api/*` to `http://localhost:7474` (see `vite.config.ts`).

### Production build

```bash
cd ui && npm run build
```

This writes compiled assets to `py-pamde/src/pamde/server/static/`.  FastAPI mounts this directory at `/` when it exists.

## Build commands

```bash
# Build the Rust extension (development, no --release)
cd py-pamde/runtime/pamde-runtime
maturin develop

# Run tests
cd py-pamde
pytest tests/

# Full release build of the .so
maturin build --release
```

## Key design decisions

| Decision | Reason |
|----------|--------|
| `parquet` crate only, not `arrow` | We need metadata, not columnar arrays.  Keeping the dep surface minimal. |
| maturin runtime bridge as a separate crate | Keeps `pamde-core` free of `cdylib` constraints; allows unit-testing Rust logic without building a Python extension. |
| FastAPI + uvicorn (not a Rust HTTP server) | Python ecosystem is the right home for the server layer; async is sufficient. |
| React (not marimo / panel) | We need a custom table with inline editing; generic notebook UIs don't fit the specific UX. |
| Single port for API + static files | Mirrors marimo's model; simpler to distribute and run. |
| `key_value_metadata` as the tagging mechanism | It is the official Parquet spec extension point (`FileMetaData.key_value_metadata`, `ColumnMetaData.key_value_metadata`).  No schema changes needed. |

## What NOT to do

- Do not add logic to `pamde-runtime/src/lib.rs` beyond `pub use pamde_core::c_api::*;`.
- Do not add Iceberg-specific logic to the core.  Iceberg field IDs are surfaced via `field_id` on `ColumnInfo` and `key_value_metadata`, but pamde is storage-agnostic.
- Do not read actual row data.  pamde is a metadata tool; data pages are opaque byte ranges.
- Do not commit `py-pamde/src/pamde/server/static/` — it is a build artifact.
