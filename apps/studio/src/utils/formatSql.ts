// A small, dependency-free SQL pretty-printer.
//
// The SQL engine re-serializes a stored procedure body onto a single line
// (sqlparser's Display), so `SHOW PROCEDURES` returns it unformatted. This
// re-introduces line breaks and indentation for readable display/editing.
// It is heuristic, not a full parser — good enough for the SELECT/CTE-shaped
// bodies the engine accepts.

// Break onto a new line before these keywords (case-insensitive). Multi-word
// entries match by lookahead. `ON` is intentionally absent so a join predicate
// stays on the JOIN line and `DISTINCT ON` isn't split.
const BREAK = [
  "WITH", "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY",
  "LIMIT", "OFFSET", "UNION ALL", "UNION",
  "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN", "CROSS JOIN", "JOIN",
];

// Keep a space before '(' after these (e.g. `AS (`, `IN (`, `WITHIN GROUP (`);
// a function name (COUNT/SUM/…) gets no space, so `COUNT(*)`.
const KW_BEFORE_PAREN = new Set([
  "AS", "IN", "ON", "WITHIN", "GROUP", "AND", "OR", "NOT", "ALL", "BY",
  "VALUES", "RETURNING",
]);

export function formatSql(src: string): string {
  const s = src.trim();
  if (!s) return s;

  // Tokenize: single-quoted strings, parens, commas, or runs of other chars.
  const toks: string[] = [];
  let i = 0;
  while (i < s.length) {
    const c = s[i];
    if (c === "'") {
      let j = i + 1;
      while (j < s.length && !(s[j] === "'" && s[j - 1] !== "\\")) j++;
      toks.push(s.slice(i, j + 1));
      i = j + 1;
    } else if (c === "(" || c === ")" || c === ",") {
      toks.push(c);
      i++;
    } else if (/\s/.test(c)) {
      i++;
    } else {
      let j = i;
      while (j < s.length && !/[\s(),']/.test(s[j])) j++;
      toks.push(s.slice(i, j));
      i = j;
    }
  }

  const up = toks.map((t) => t.toUpperCase());
  let out = "";
  let depth = 0;
  const stack: boolean[] = []; // true = a SELECT/WITH paren that indents
  const pad = () => "  ".repeat(depth);
  const nl = () => {
    out = out.replace(/[ \t]+$/, "") + "\n" + pad();
  };
  const atLineStart = () => out === "" || out.endsWith("\n");

  for (let k = 0; k < toks.length; k++) {
    const t = toks[k];
    const insidePlainParen =
      stack.length > 0 && stack[stack.length - 1] === false;

    let kw: string | null = null;
    let span = 1;
    if (!insidePlainParen) {
      for (const key of BREAK) {
        const parts = key.split(" ");
        if (parts.every((p, d) => up[k + d] === p)) {
          kw = key;
          span = parts.length;
          break;
        }
      }
    }

    if (t === "(") {
      const block = up[k + 1] === "SELECT" || up[k + 1] === "WITH";
      if (!atLineStart()) {
        if (KW_BEFORE_PAREN.has(up[k - 1])) {
          if (!out.endsWith(" ")) out += " ";
        } else {
          out = out.replace(/ $/, "");
        }
      }
      out += "(";
      stack.push(block);
      if (block) {
        depth++;
        nl();
      }
      continue;
    }
    if (t === ")") {
      const block = stack.pop();
      if (block) {
        depth = Math.max(0, depth - 1);
        nl();
      }
      out = out.replace(/ $/, "");
      out += ")";
      continue;
    }
    if (t === ",") {
      out = out.replace(/ $/, "");
      out += ",";
      // break the list at CTE/select level, but not inside function args
      if (stack.length === 0 || stack[stack.length - 1] === true) nl();
      else out += " ";
      continue;
    }

    if (kw) {
      const word = toks.slice(k, k + span).join(" ");
      if (!atLineStart()) nl();
      out += word + " ";
      k += span - 1;
      continue;
    }

    if (!atLineStart() && !out.endsWith("(") && !out.endsWith(" ")) out += " ";
    out += t;
    const nx = toks[k + 1];
    if (nx && nx !== "," && nx !== ")" && nx !== "(") out += " ";
  }

  return out.replace(/[ \t]+\n/g, "\n").replace(/\n{2,}/g, "\n").trim();
}
