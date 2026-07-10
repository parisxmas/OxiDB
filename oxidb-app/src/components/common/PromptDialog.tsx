import { useState } from "react";

interface Props {
  title: string;
  label?: string;
  placeholder?: string;
  initial?: string;
  confirmLabel?: string;
  onConfirm: (value: string) => void;
  onCancel: () => void;
}

/**
 * A simple text-input modal — a replacement for window.prompt, which the
 * Tauri webview (WKWebView on macOS) does not support (it returns null
 * without showing anything).
 */
export function PromptDialog({
  title,
  label,
  placeholder,
  initial = "",
  confirmLabel = "OK",
  onConfirm,
  onCancel,
}: Props) {
  const [value, setValue] = useState(initial);
  const submit = () => {
    if (value.trim()) onConfirm(value.trim());
  };
  return (
    <div className="dialog-overlay" onClick={onCancel}>
      <div className="dialog" style={{ width: 380 }} onClick={(e) => e.stopPropagation()}>
        <div className="dialog-title">{title}</div>
        <div className="form-group">
          {label && <label>{label}</label>}
          <input
            autoFocus
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder={placeholder}
            onKeyDown={(e) => {
              if (e.key === "Enter") submit();
              else if (e.key === "Escape") onCancel();
            }}
            style={{ fontFamily: "var(--font-mono)" }}
          />
        </div>
        <div className="dialog-actions">
          <button className="btn btn-secondary" onClick={onCancel}>Cancel</button>
          <button className="btn btn-primary" onClick={submit} disabled={!value.trim()}>
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
