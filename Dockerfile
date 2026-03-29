FROM python:3.13-slim

# Install Node.js 20 for the UI build step
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

# Build the UI → writes compiled assets to py-pamde/src/pamde/server/static/
RUN cd ui && npm install && npm run build

# Install the Python package (pyarrow backend, no Rust compilation needed)
RUN pip install --no-cache-dir ./py-pamde

EXPOSE 2971

# User passes the parquet file path as the argument, e.g.:
#   docker run -p 2971:2971 -v $(pwd)/data.parquet:/data/file.parquet pamde /data/file.parquet
ENTRYPOINT ["pamde", "edit", "--host", "0.0.0.0", "--no-browser"]
