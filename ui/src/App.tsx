import { useEffect, useState } from "react";
import { api, ColumnInfo, FileInfo } from "./api";
import MetadataTable from "./components/MetadataTable";
import UploadView from "./components/UploadView";

type View = "loading" | "upload" | "editor";

export default function App() {
  const [view, setView] = useState<View>("loading");
  const [file, setFile] = useState<FileInfo | null>(null);
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [mode, setMode] = useState<"edit" | "run">("edit");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get("file")) {
      loadEditor();
      return;
    }
    api.getStatus()
      .then((status) => {
        setMode(status.mode);
        if (status.mode === "edit" && status.file) {
          setUrlFile(status.file);
          return loadEditor();
        }
        setView("upload");
      })
      .catch((e) => setError(String(e)));
  }, []);

  const setUrlFile = (filename: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("file", filename);
    window.history.replaceState(null, "", url.toString());
  };

  const loadEditor = async () => {
    try {
      const [f, cols] = await Promise.all([api.getFile(), api.getColumns()]);
      setFile(f);
      setColumns(cols);
      setView("editor");
    } catch (e) {
      setError(String(e));
    }
  };

  const onUploaded = (filename: string) => {
    setMode("run");
    setUrlFile(filename);
    loadEditor();
  };

  const reloadColumns = () => {
    api.getColumns().then(setColumns).catch((e) => setError(String(e)));
  };

  if (error) {
    return <div style={{ color: "red", padding: 24, fontFamily: "monospace" }}>{error}</div>;
  }

  if (view === "loading") return null;
  if (view === "upload") return <UploadView onLoaded={onUploaded} />;

  return (
    <div style={css.root}>
      <header style={css.header}>
        <span style={css.logo}>pamde</span>
        <span style={css.filename} title={file?.path}>{file?.file}</span>
        <div style={css.actions}>
          {mode === "run" && (
            <button onClick={api.downloadFile} style={css.btnDownload}>
              Download
            </button>
          )}
        </div>
      </header>
      <main style={css.main}>
        <MetadataTable columns={columns} onTagChange={reloadColumns} />
      </main>
    </div>
  );
}

const css: Record<string, React.CSSProperties> = {
  root: {
    display: "flex",
    flexDirection: "column",
    height: "100vh",
    overflow: "hidden",
    fontFamily: "'SF Mono', 'Fira Code', 'Consolas', monospace",
    background: "#fff",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "0 16px",
    height: 48,
    borderBottom: "1px solid #f0f0f0",
    flexShrink: 0,
  },
  logo: {
    fontWeight: 700,
    fontSize: 15,
    letterSpacing: 1.5,
    color: "#111",
  },
  filename: {
    flex: 1,
    fontSize: 12,
    color: "#999",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  actions: {
    display: "flex",
    gap: 8,
    alignItems: "center",
    flexShrink: 0,
  },
  btnDownload: {
    padding: "4px 12px",
    fontSize: 12,
    cursor: "pointer",
    background: "transparent",
    border: "1px solid #d9d9d9",
    borderRadius: 4,
    color: "#333",
    fontFamily: "inherit",
  },
  main: {
    flex: 1,
    overflow: "hidden",
    display: "flex",
    flexDirection: "column",
  },
};
