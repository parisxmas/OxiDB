import Editor from "@monaco-editor/react";
import type { OnMount } from "@monaco-editor/react";
import type { editor as MonacoEditor } from "monaco-editor";
import { useTheme } from "../../context/ThemeContext";
import { useFontScale } from "../../context/FontScaleContext";

interface Props {
  value: string;
  onChange?: (value: string) => void;
  onRun?: () => void;
  /** Receives the editor instance once mounted (for insert-at-cursor, etc.). */
  onReady?: (editor: MonacoEditor.IStandaloneCodeEditor) => void;
  readOnly?: boolean;
  height?: string;
  /**
   * Monaco language id. Defaults to "sql". Pass "python" for Cobra source so
   * the SQL tokenizer doesn't mis-read an apostrophe (e.g. "CALL'da") as an
   * unterminated string and turn the whole file red.
   */
  language?: string;
}

/** Monaco configured for SQL, with ⌘/Ctrl+Enter wired to `onRun`. */
export function SqlEditor({
  value,
  onChange,
  onRun,
  onReady,
  readOnly = false,
  height = "100%",
  language = "sql",
}: Props) {
  const { theme } = useTheme();
  const { scale } = useFontScale();

  const handleMount: OnMount = (editor, monaco) => {
    editor.addCommand(
      monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      () => onRun?.()
    );
    onReady?.(editor);
  };

  return (
    <Editor
      // key forces a fresh mount (and a fresh model) when the language
      // changes, so Monaco actually re-tokenizes instead of keeping the
      // model created with the initial defaultLanguage.
      key={language}
      height={height}
      defaultLanguage={language}
      language={language}
      value={value}
      onChange={(v) => onChange?.(v || "")}
      onMount={handleMount}
      theme={theme === "dark" ? "vs-dark" : "light"}
      options={{
        readOnly,
        minimap: { enabled: false },
        fontSize: Math.round(13 * scale),
        fontFamily: "var(--font-mono)",
        lineNumbers: "on",
        scrollBeyondLastLine: false,
        tabSize: 2,
        automaticLayout: true,
        wordWrap: "off",
        suggestOnTriggerCharacters: true,
        // Don't box non-ASCII/"ambiguous" chars — Turkish letters (ı, ş…) in
        // comments and string literals are legitimate, not homoglyph attacks.
        unicodeHighlight: {
          ambiguousCharacters: false,
          invisibleCharacters: false,
          nonBasicASCII: false,
        },
      }}
    />
  );
}
