import { useMemo, useRef } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql as sqlLang, SQLDialect } from "@codemirror/lang-sql";
import { keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";

// OxiDB's SQL surface: standard SQL plus its own SHOW/DESCRIBE family.
const OXIDB_SQL = SQLDialect.define({
  keywords:
    "select from where insert into values update set delete create table drop index view procedure " +
    "alter add column rename modify type if not exists primary key auto_increment unique references " +
    "foreign check default null and or in is like between limit offset order by group having as on " +
    "inner left right full outer cross join lateral union except intersect all distinct case when then " +
    "else end cast begin commit rollback savepoint show tables indexes columns views procedures describe " +
    "with recursive returning call sequence next value for text integer int bigint smallint varchar char " +
    "double float real decimal numeric bool boolean timestamp datetime blob count sum avg min max",
});

/** SQL-highlighted editor (CodeMirror 6) — theme-aware, Cmd/Ctrl+Enter runs. */
export function SqlEditor({
  value,
  onChange,
  onRun,
  minHeight = "160px",
}: {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  minHeight?: string;
}) {
  const dark = window.matchMedia("(prefers-color-scheme: dark)").matches;

  // Latest onRun via a ref so the (memoized) keymap never goes stale.
  const runRef = useRef(onRun);
  runRef.current = onRun;
  // Cmd/Ctrl+Enter must win over CodeMirror's own Enter handling.
  const runKey = useMemo(
    () =>
      Prec.highest(
        keymap.of([
          {
            key: "Mod-Enter",
            run: () => {
              runRef.current();
              return true;
            },
          },
        ]),
      ),
    [],
  );

  return (
    <div className="cm-wrap">
      <CodeMirror
        value={value}
        onChange={onChange}
        theme={dark ? "dark" : "light"}
        extensions={[sqlLang({ dialect: OXIDB_SQL, upperCaseKeywords: true }), runKey]}
        minHeight={minHeight}
        basicSetup={{
          lineNumbers: true,
          foldGutter: false,
          highlightActiveLine: false,
          autocompletion: true,
        }}
      />
    </div>
  );
}
