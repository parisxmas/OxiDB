import Editor from "@monaco-editor/react";
import { useTheme } from "../../context/ThemeContext";

interface Props {
  value: string;
  onChange?: (value: string) => void;
  readOnly?: boolean;
  height?: string;
}

export function JsonEditor({
  value,
  onChange,
  readOnly = false,
  height = "300px",
}: Props) {
  const { theme } = useTheme();
  return (
    <Editor
      height={height}
      defaultLanguage="json"
      value={value}
      onChange={(v) => onChange?.(v || "")}
      theme={theme === "dark" ? "vs-dark" : "light"}
      options={{
        readOnly,
        minimap: { enabled: false },
        fontSize: 13,
        fontFamily: "var(--font-mono)",
        lineNumbers: "on",
        scrollBeyondLastLine: false,
        tabSize: 2,
        automaticLayout: true,
        wordWrap: "on",
      }}
    />
  );
}
