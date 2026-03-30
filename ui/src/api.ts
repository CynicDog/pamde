export interface ColumnInfo {
  physical_name: string;
  path_in_schema: string;
  physical_type: string;
  logical_type: string | null;
  repetition: string;
  field_id: number | null;
  null_count: number | null;
  distinct_count: number | null;
  min_value: string | null;
  max_value: string | null;
  compression: string;
  total_compressed_size: number;
  total_uncompressed_size: number;
  tags: Record<string, string | null>;
}

export interface FileInfo {
  path: string;
  file: string;
  tags: Record<string, string | null>;
}

export interface Status {
  mode: "edit" | "run";
  file: string | null;
}

const BASE = "/api";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`GET ${path} failed: ${res.statusText}`);
  return res.json();
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`POST ${path} failed: ${res.statusText}`);
  return res.json();
}

export const api = {
  getStatus: () => get<Status>("/status"),
  getFile: () => get<FileInfo>("/file"),
  getColumns: () => get<ColumnInfo[]>("/columns"),

  uploadFile: async (file: File): Promise<{ file: string }> => {
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${BASE}/upload`, { method: "POST", body: form });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail ?? res.statusText);
    }
    return res.json();
  },

  setFileTag: (key: string, value: string | null) =>
    post("/file/tags", { key, value }),
  setColumnTag: (column_path: string, key: string, value: string | null) =>
    post("/columns/tags", { column_path, key, value }),
  setColumnTagsBatch: (updates: { column_path: string; key: string; value: string | null }[]) =>
    post("/columns/tags/batch", { updates }),

  downloadFile: () => {
    const a = document.createElement("a");
    a.href = `${BASE}/download`;
    a.click();
  },
};
