import { useEffect, useState, useMemo } from "react";
import { Table } from "antd";
import type { TableColumnsType } from "antd";
import { api, ColumnInfo } from "../api";

interface Props {
  columns: ColumnInfo[];
  onTagChange: () => void;
}

const LABEL_W = 150;
const DATA_W = 130;

const META_ROWS: { key: keyof ColumnInfo; label: string }[] = [
  { key: "path_in_schema",          label: "Path"         },
  { key: "physical_name",           label: "Name"         },
  { key: "physical_type",           label: "Type"         },
  { key: "logical_type",            label: "Logical Type" },
  { key: "repetition",              label: "Repetition"   },
  { key: "field_id",                label: "Field ID"     },
  { key: "null_count",              label: "Nulls"        },
  { key: "distinct_count",          label: "Distincts"    },
  { key: "min_value",               label: "Min"          },
  { key: "max_value",               label: "Max"          },
  { key: "compression",             label: "Compression"  },
  { key: "total_compressed_size",   label: "Compressed"   },
  { key: "total_uncompressed_size", label: "Uncompressed" },
];

type Row = { key: string; label: string; isTag: boolean; [colPath: string]: unknown };

export default function MetadataTable({ columns, onTagChange }: Props) {
  const [pendingTagKeys, setPendingTagKeys] = useState<string[]>([]);
  const [addingTag, setAddingTag] = useState(false);
  const [newTagKey, setNewTagKey] = useState("");
  const [saveError, setSaveError] = useState<string | null>(null);

  const allTagKeys = useMemo(() => {
    const persisted = Array.from(new Set(columns.flatMap(c => Object.keys(c.tags || {}))));
    return [...persisted, ...pendingTagKeys.filter(k => !persisted.includes(k))];
  }, [columns, pendingTagKeys]);

  const rows: Row[] = useMemo(() => {
    const metaRows = META_ROWS.map(({ key, label }) => {
      const row: Row = { key: key as string, label, isTag: false };
      for (const col of columns) row[col.path_in_schema] = col[key];
      return row;
    });
    const tagRows = allTagKeys.map(tagKey => {
      const row: Row = { key: `tag:${tagKey}`, label: tagKey, isTag: true };
      for (const col of columns) row[col.path_in_schema] = col.tags[tagKey] ?? null;
      return row;
    });
    const addRow: Row = { key: "__add_tag__", label: "", isTag: false };
    return [...metaRows, ...tagRows, addRow];
  }, [columns, allTagKeys]);

  const handleAddTag = () => {
    const key = newTagKey.trim();
    if (key && !allTagKeys.includes(key)) setPendingTagKeys(prev => [...prev, key]);
    setNewTagKey("");
    setAddingTag(false);
  };

  const handleDeleteTag = async (tagKey: string) => {
    if (pendingTagKeys.includes(tagKey)) {
      setPendingTagKeys(prev => prev.filter(k => k !== tagKey));
      return;
    }
    try {
      const updates = columns.map(col => ({ column_path: col.path_in_schema, key: tagKey, value: null as null }));
      await api.setColumnTagsBatch(updates);
      onTagChange();
    } catch (e) {
      setSaveError(String(e));
    }
  };

  const tableColumns: TableColumnsType<Row> = [
    {
      key: "__field__",
      title: "",
      dataIndex: "label",
      width: LABEL_W,
      fixed: "left" as const,
      onHeaderCell: () => ({ style: { background: "#fafafa" } }),
      onCell: (row: Row) => ({ style: { background: row.isTag ? "#fffbe6" : row.key === "__add_tag__" ? "#fafafa" : "#fafafa" } }),
      render: (_: string, row: Row) => {
        if (row.key === "__add_tag__") {
          if (addingTag) {
            return (
              <span style={{ display: "flex", gap: 4, alignItems: "center" }}>
                <input
                  autoFocus
                  value={newTagKey}
                  onChange={e => setNewTagKey(e.target.value)}
                  onKeyDown={e => { if (e.key === "Enter") handleAddTag(); if (e.key === "Escape") { setAddingTag(false); setNewTagKey(""); } }}
                  placeholder="tag name"
                  style={{ ...css.input, width: 90 }}
                />
                <button onClick={handleAddTag} style={css.btnConfirm}>Add</button>
                <button onClick={() => { setAddingTag(false); setNewTagKey(""); }} style={css.btnCancel}>✕</button>
              </span>
            );
          }
          return <button onClick={() => setAddingTag(true)} style={css.btnAddTag}>+ Add tag</button>;
        }
        if (row.isTag) {
          return (
            <span style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 4 }}>
              <span style={{ fontSize: 12, color: "#c47d00", fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {row.label}
              </span>
              <button onClick={() => handleDeleteTag(row.label)} style={css.btnDelete} title={`Delete tag "${row.label}"`}>×</button>
            </span>
          );
        }
        return (
          <span style={{ fontSize: 12, color: "#555" }}>{row.label}</span>
        );
      },
    },
    ...columns.map(col => ({
      key: col.path_in_schema,
      title: col.path_in_schema,
      width: DATA_W,
      onHeaderCell: () => ({
        title: col.path_in_schema,
        style: { overflow: "hidden", whiteSpace: "nowrap", textOverflow: "ellipsis" },
      }),
      onCell: (row: Row) => ({
        style: { background: row.isTag ? "#fffbe6" : undefined, padding: row.isTag ? 0 : undefined },
      }),
      render: (_: unknown, row: Row) => {
        if (row.key === "__add_tag__") return null;
        const val = row[col.path_in_schema];
        if (row.isTag) {
          return (
            <EditableCell
              value={String(val ?? "")}
              onSave={(v: string) =>
                api.setColumnTag(col.path_in_schema, row.label, v || null)
                  .then(onTagChange)
                  .catch(e => setSaveError(String(e)))
              }
            />
          );
        }
        return val == null
          ? <span style={{ color: "#ccc" }}>—</span>
          : <span style={{ fontSize: 12 }}>{String(val)}</span>;
      },
    })),
  ];

  const totalWidth = LABEL_W + columns.length * DATA_W;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {saveError && (
        <div style={css.errorBanner}>
          {saveError}
          <button onClick={() => setSaveError(null)} style={css.btnDismiss}>✕</button>
        </div>
      )}
      <div style={{ flex: 1, overflow: "hidden" }}>
        <Table<Row>
          dataSource={rows}
          columns={tableColumns}
          rowKey="key"
          pagination={false}
          size="small"
          bordered
          tableLayout="fixed"
          scroll={{ x: totalWidth, y: "calc(100vh - 90px)" }}
          style={{ fontFamily: "'SF Mono', 'Fira Code', 'Consolas', monospace" }}
        />
      </div>
    </div>
  );
}

function EditableCell({ value, onSave }: { value: string; onSave: (v: string) => void }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  useEffect(() => { if (!editing) setDraft(value); }, [value, editing]);

  return (
    <div
      onClick={() => !editing && setEditing(true)}
      style={{ position: "relative", minHeight: 24, cursor: editing ? "default" : "text" }}
    >
      {editing ? (
        <input
          autoFocus
          value={draft}
          onChange={e => setDraft(e.target.value)}
          onBlur={() => { onSave(draft); setEditing(false); }}
          onKeyDown={e => {
            if (e.key === "Enter") { onSave(draft); setEditing(false); }
            if (e.key === "Escape") { setDraft(value); setEditing(false); }
          }}
          style={css.cellInput}
        />
      ) : (
        <span style={{ display: "block", padding: "4px 8px", fontSize: 12 }}>
          {value || <span style={{ color: "#ccc" }}>—</span>}
        </span>
      )}
    </div>
  );
}

const css: Record<string, React.CSSProperties> = {
  errorBanner: {
    fontSize: 12,
    color: "#c0392b",
    background: "#fdf0ed",
    border: "1px solid #f5c6bc",
    borderRadius: 4,
    padding: "4px 10px",
    margin: "0 16px 6px",
    display: "flex",
    alignItems: "center",
    gap: 8,
  },
  btnDismiss: {
    background: "none",
    border: "none",
    cursor: "pointer",
    color: "#c0392b",
    padding: 0,
    fontSize: 11,
  },
  input: {
    fontSize: 12,
    padding: "3px 8px",
    border: "1px solid #d9d9d9",
    borderRadius: 3,
    outline: "none",
    fontFamily: "inherit",
    width: 160,
  },
  btnConfirm: {
    padding: "3px 10px",
    fontSize: 12,
    cursor: "pointer",
    background: "#1677ff",
    border: "none",
    borderRadius: 3,
    color: "#fff",
    fontFamily: "inherit",
  },
  btnCancel: {
    padding: "3px 8px",
    fontSize: 12,
    cursor: "pointer",
    background: "transparent",
    border: "1px solid #d9d9d9",
    borderRadius: 3,
    color: "#888",
    fontFamily: "inherit",
  },
  btnAddTag: {
    padding: "3px 10px",
    fontSize: 12,
    cursor: "pointer",
    background: "transparent",
    border: "1px dashed #ccc",
    borderRadius: 3,
    color: "#aaa",
    fontFamily: "inherit",
  },
  btnDelete: {
    flexShrink: 0,
    padding: "0 3px",
    fontSize: 13,
    lineHeight: 1,
    cursor: "pointer",
    background: "none",
    border: "none",
    color: "#ccc",
    fontFamily: "inherit",
  },
  cellInput: {
    position: "absolute",
    inset: 0,
    width: "100%",
    padding: "4px 8px",
    border: "none",
    outline: "2px solid #1677ff",
    outlineOffset: -2,
    fontFamily: "inherit",
    fontSize: 12,
    background: "#fff",
    zIndex: 1,
  },
};
