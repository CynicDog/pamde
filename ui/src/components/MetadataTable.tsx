/**
 * MetadataTable — the central UI component.
 *
 * Rows = Parquet columns.
 * Fixed columns: physical_name, path_in_schema, physical_type, logical_type,
 *                repetition, field_id, null_count, min_value, max_value,
 *                compression, compressed_size.
 * User-defined columns: any key in the union of all column .tags objects.
 *                       Cells are editable inline.
 *
 * Users can add a new tag key via the "+ Add tag" header button, which creates
 * a new column and lets them fill values per row.
 */

import { useState } from "react";
import { api, ColumnInfo } from "../api";

interface Props {
  columns: ColumnInfo[];
  onTagChange: () => void;
}

const FIXED_COLS: { key: keyof ColumnInfo; label: string }[] = [
  { key: "physical_name", label: "Name" },
  { key: "path_in_schema", label: "Path" },
  { key: "physical_type", label: "Physical Type" },
  { key: "logical_type", label: "Logical Type" },
  { key: "repetition", label: "Repetition" },
  { key: "field_id", label: "Field ID" },
  { key: "null_count", label: "Null Count" },
  { key: "min_value", label: "Min" },
  { key: "max_value", label: "Max" },
  { key: "compression", label: "Compression" },
  { key: "total_compressed_size", label: "Compressed (B)" },
];

export default function MetadataTable({ columns, onTagChange }: Props) {
  const [newTagKey, setNewTagKey] = useState("");
  const [addingTag, setAddingTag] = useState(false);

  // Collect all tag keys across all columns.
  const tagKeys = Array.from(
    new Set(columns.flatMap((c) => Object.keys(c.tags)))
  ).sort();

  const handleCellEdit = async (
    col: ColumnInfo,
    tagKey: string,
    value: string
  ) => {
    await api.setColumnTag(col.path_in_schema, tagKey, value || null);
    onTagChange();
  };

  const handleAddTag = () => {
    const key = newTagKey.trim();
    if (!key) return;
    setNewTagKey("");
    setAddingTag(false);
    // Tag will appear once a value is set for any row; for now just close.
  };

  return (
    <div style={{ overflowX: "auto" }}>
      <table
        style={{
          borderCollapse: "collapse",
          fontSize: 13,
          width: "100%",
        }}
      >
        <thead>
          <tr>
            {FIXED_COLS.map((c) => (
              <Th key={c.key}>{c.label}</Th>
            ))}
            {tagKeys.map((k) => (
              <Th key={k} mutable>
                {k}
              </Th>
            ))}
            <th style={{ padding: "6px 8px" }}>
              {addingTag ? (
                <span>
                  <input
                    autoFocus
                    value={newTagKey}
                    onChange={(e) => setNewTagKey(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleAddTag()}
                    style={{ width: 100, fontFamily: "monospace", fontSize: 12 }}
                    placeholder="tag key"
                  />
                  <button onClick={handleAddTag} style={{ marginLeft: 4 }}>
                    ✓
                  </button>
                  <button
                    onClick={() => setAddingTag(false)}
                    style={{ marginLeft: 2 }}
                  >
                    ✕
                  </button>
                </span>
              ) : (
                <button
                  onClick={() => setAddingTag(true)}
                  style={{ cursor: "pointer" }}
                >
                  + Add tag
                </button>
              )}
            </th>
          </tr>
        </thead>
        <tbody>
          {columns.map((col) => (
            <tr key={col.path_in_schema}>
              {FIXED_COLS.map((c) => (
                <td
                  key={c.key}
                  style={{
                    padding: "5px 8px",
                    borderBottom: "1px solid #eee",
                    color: col[c.key] == null ? "#aaa" : undefined,
                  }}
                >
                  {col[c.key] == null ? "—" : String(col[c.key])}
                </td>
              ))}
              {tagKeys.map((k) => (
                <EditableCell
                  key={k}
                  value={col.tags[k] ?? ""}
                  onSave={(v) => handleCellEdit(col, k, v)}
                />
              ))}
              <td />
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Th({
  children,
  mutable,
}: {
  children: React.ReactNode;
  mutable?: boolean;
}) {
  return (
    <th
      style={{
        padding: "6px 8px",
        textAlign: "left",
        borderBottom: "2px solid #ccc",
        background: mutable ? "#fffbe6" : undefined,
        fontWeight: 600,
      }}
    >
      {children}
    </th>
  );
}

function EditableCell({
  value,
  onSave,
}: {
  value: string;
  onSave: (v: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  if (editing) {
    return (
      <td style={{ padding: "3px 6px", background: "#fffbe6" }}>
        <input
          autoFocus
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={() => {
            onSave(draft);
            setEditing(false);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              onSave(draft);
              setEditing(false);
            }
            if (e.key === "Escape") setEditing(false);
          }}
          style={{ fontFamily: "monospace", fontSize: 12, width: "100%" }}
        />
      </td>
    );
  }

  return (
    <td
      onClick={() => {
        setDraft(value);
        setEditing(true);
      }}
      style={{
        padding: "5px 8px",
        borderBottom: "1px solid #eee",
        cursor: "text",
        color: value ? undefined : "#aaa",
        background: "#fffbe6",
      }}
    >
      {value || "—"}
    </td>
  );
}
