# pamde — Parquet Metadata Editor

Open a Parquet file, inspect every column's schema and statistics, and tag metadata key/value pairs directly from your browser.

```bash 
pamde edit my_data.parquet
```

## Quick Start

### Docker

```bash
docker build -t pamde .
```

```bash
docker run -p 2971:2971 \
  -v "$(pwd)/my_data.parquet:/data/file.parquet" \
  pamde /data/file.parquet
```

Open **http://localhost:2971** in your browser.

To edit a file in the current directory, replace `$(pwd)/my_data.parquet` with the actual path to your file.

### Local

**Requirements:** Python ≥ 3.10, Node.js ≥ 18, `uv`

**1. Build the UI**

```bash
cd ui
npm install
npm run build
cd ..
```

**2. Install the Python package**

```bash
cd py-pamde
uv venv .venv
uv pip install -e .
```

**3. Run**

```bash
.venv/bin/pamde edit path/to/your_file.parquet
```

Open **http://localhost:2971** in your browser.

## What it does

Parquet files carry rich metadata in their footer:

- **Schema** — physical type, logical type, repetition, field_id per column
- **Statistics** — null count, distinct count, min/max values per row group
- **Encoding & compression** — per column chunk
- **`key_value_metadata`** — arbitrary string tags at file level and per column

pamde shows all of this in a table (rows = columns, columns = metadata fields) and lets you add and edit `key_value_metadata` inline.

## CLI

```bash
# Open editor UI
pamde edit my_data.parquet

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
│   │   ├── editor.py        ParquetEditor (auto-selects Rust or pyarrow backend)
│   │   ├── cli.py           pamde edit / pamde inspect
│   │   └── server/          FastAPI server + REST routes
│   └── tests/
└── ui/                      React + TypeScript (Vite)
```

The editor currently uses a **pyarrow backend** for metadata access. The Rust backend (`pamde-runtime`) is a drop-in replacement that will be preferred automatically once built via `maturin develop`.