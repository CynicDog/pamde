# pamde — Parquet Metadata Editor

Open a Parquet file, inspect every column's schema and statistics, and tag metadata key/value pairs directly from your browser.

```bash
pamde edit my_data.parquet
```

## Quick Start

### Docker

```bash
docker run -p 2971:2971 \
  -v "$(pwd)/my_data.parquet:/data/file.parquet" \
  ghcr.io/cynicdog/pamde:latest /data/file.parquet
```

Open **http://localhost:2971** in your browser.

### Local

**Requirements:** Python ≥ 3.10, Node.js ≥ 18, Rust toolchain, `uv`, `maturin`

**1. Build the Rust extension**

```bash
cd py-pamde/runtime/pamde-runtime
maturin develop
cd ../../..
```

**2. Build the UI**

```bash
cd ui
npm install
npm run build
cd ..
```

**3. Install the Python package**

```bash
cd py-pamde
uv venv .venv
uv pip install -e .
```

**4. Run**

```bash
.venv/bin/pamde edit path/to/your_file.parquet
```

Open **http://localhost:2971** in your browser.

## Running Tests

**Requirements:** complete steps 1 and 3 above first (Rust extension + Python package installed).

```bash
cd py-pamde
uv pip install pytest
.venv/bin/pytest tests/ -v
```

Expected output: **39 passed**.

The tests cover:
- Schema introspection (column count, names, types, repetition, field_id)
- Statistics (null count, min/max, compression, sizes)
- File-level tag set / update / remove / unicode
- Column-level tag set / update / remove / isolation
- File integrity after edits (data unchanged, stats preserved, in-place roundtrip)

## What it does

Parquet files carry rich metadata in their footer:

- **Schema** — physical type, logical type, repetition, field_id per column
- **Statistics** — null count, distinct count, min/max values per row group
- **Encoding & compression** — per column chunk
- **`key_value_metadata`** — arbitrary string tags at file level and per column

pamde shows all of this in a table (rows = columns, columns = metadata fields) and lets you add and edit `key_value_metadata` inline. Only the file footer is read or rewritten — row data is never touched.

## CLI

```bash
# Open editor UI
pamde edit my_data.parquet

# Start without a file — upload one from the browser
pamde run

# Print metadata summary to stdout
pamde inspect my_data.parquet

# Print as JSON
pamde inspect my_data.parquet --json
```

## Python API

```python
from pamde import ParquetEditor

editor = ParquetEditor.open("fruits.parquet")

# Inspect
for col in editor.columns():
    print(col.physical_name, col.physical_type, col.null_count)

# Annotate
editor.set_file_tag("owner", "data-team")
editor.set_column_tag("name", "description", "Primary key for fruit taxonomy")
editor.set_column_tag("acidity", "unit", "pH")
```

## Architecture

```
pamde/
├── crates/pamde-core/       Rust: parquet parsing + metadata mutation
├── py-pamde/
│   ├── runtime/pamde-runtime/   maturin bridge → _pamde_runtime.so
│   ├── src/pamde/
│   │   ├── editor.py        ParquetEditor Python API
│   │   ├── cli.py           pamde edit / pamde run / pamde inspect
│   │   └── server/          FastAPI server + REST routes
│   └── tests/
└── ui/                      React + TypeScript (Vite)
```

Metadata reads and writes go through the Rust extension exclusively. The footer is rewritten in-place using thrift serialization — data pages are never touched.
