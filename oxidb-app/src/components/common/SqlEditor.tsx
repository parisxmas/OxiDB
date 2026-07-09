import Editor from "@monaco-editor/react";
import type { OnMount } from "@monaco-editor/react";
import type { editor as MonacoEditor } from "monaco-editor";
import { useTheme } from "../../context/ThemeContext";

interface Props {
  value: string;
  onChange?: (value: string) => void;
  onRun?: () => void;
  /** Receives the editor instance once mounted (for insert-at-cursor, etc.). */
  onReady?: (editor: MonacoEditor.IStandaloneCodeEditor) => void;
  readOnly?: boolean;
  height?: string;
}

/** Monaco configured for SQL, with ⌘/Ctrl+Enter wired to `onRun`. */
export function SqlEditor({
  value,
  onChange,
  onRun,
  onReady,
  readOnly = false,
  height = "100%",
}: Props) {
  const { theme } = useTheme();

  const handleMount: OnMount = (editor, monaco) => {
    editor.addCommand(
      monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      () => onRun?.()
    );
    onReady?.(editor);
  };

  return (
    <Editor
      height={height}
      defaultLanguage="sql"
      value={value}
      onChange={(v) => onChange?.(v || "")}
      onMount={handleMount}
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
        wordWrap: "off",
        suggestOnTriggerCharacters: true,
      }}
    />
  );
}
