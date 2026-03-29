import { useEffect, useState } from "react";
import { api, ColumnInfo, FileInfo } from "./api";
import MetadataTable from "./components/MetadataTable";

export default function App() {
  const [file, setFile] = useState<FileInfo | null>(null);
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([api.getFile(), api.getColumns()])
      .then(([f, cols]) => {
        setFile(f);
        setColumns(cols);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const reload = () => {
    api.getColumns().then(setColumns).catch((e) => setError(String(e)));
  };

  if (error) return <div style={{ color: "red", padding: 24 }}>{error}</div>;
  if (!file) return <div style={{ padding: 24 }}>Loading…</div>;

  return (
    <div style={{ fontFamily: "monospace", padding: 24 }}>
      <h1 style={{ marginBottom: 4 }}>pamde</h1>
      <p style={{ color: "#666", marginTop: 0 }}>{file.path}</p>
      <MetadataTable columns={columns} onTagChange={reload} />
    </div>
  );
}
