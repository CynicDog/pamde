import { useRef, useState } from "react";
import { api } from "../api";

interface Props {
  onLoaded: (filename: string) => void;
}

export default function UploadView({ onLoaded }: Props) {
  const [dragging, setDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handle = async (file: File) => {
    if (!file.name.endsWith(".parquet")) {
      setError("Only .parquet files are accepted.");
      return;
    }
    setError(null);
    setLoading(true);
    try {
      const { file: filename } = await api.uploadFile(file);
      onLoaded(filename);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={css.page}>
      <span style={css.logo}>pamde</span>
      <span style={css.subtitle}>Parquet Metadata Editor</span>
      <div
        style={{ ...css.zone, ...(dragging ? css.zoneDragging : {}) }}
        onDragOver={e => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={e => { e.preventDefault(); setDragging(false); const f = e.dataTransfer.files[0]; if (f) handle(f); }}
        onClick={() => inputRef.current?.click()}
      >
        <input ref={inputRef} type="file" accept=".parquet" style={{ display: "none" }} onChange={e => { const f = e.target.files?.[0]; if (f) handle(f); }} />
        {loading ? (
          <span style={{ color: "#999" }}>Reading metadata…</span>
        ) : (
          <>
            <span style={css.icon}>↑</span>
            <span>Drop a <code>.parquet</code> file here, or click to browse</span>
            <span style={css.hint}>Only the file footer (metadata) is read — row data stays on your machine</span>
          </>
        )}
      </div>
      {error && <p style={css.error}>{error}</p>}
    </div>
  );
}

const css: Record<string, React.CSSProperties> = {
  page: {
    fontFamily: "'SF Mono', 'Fira Code', 'Consolas', monospace",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    height: "100vh",
    gap: 10,
    background: "#fafafa",
  },
  logo: {
    fontWeight: 700,
    fontSize: 20,
    letterSpacing: 2,
    color: "#111",
  },
  subtitle: {
    fontSize: 12,
    color: "#aaa",
  },
  zone: {
    marginTop: 20,
    width: 460,
    padding: "44px 24px",
    border: "1px dashed #ccc",
    borderRadius: 6,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    gap: 8,
    cursor: "pointer",
    background: "#fff",
    transition: "border-color 0.15s, background 0.15s",
    fontSize: 13,
    textAlign: "center",
    color: "#555",
  },
  zoneDragging: {
    borderColor: "#1677ff",
    background: "#f0f7ff",
  },
  icon: {
    fontSize: 24,
    color: "#bbb",
    marginBottom: 4,
  },
  hint: {
    fontSize: 11,
    color: "#bbb",
    marginTop: 4,
  },
  error: {
    fontSize: 12,
    color: "#c0392b",
    margin: 0,
  },
};
