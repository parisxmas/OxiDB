// Parse CSV / JSON text into a uniform { columns, rows } shape for import.
// Values are strings or null (null = empty / missing / JSON null).

export interface ParsedData {
  columns: string[];
  rows: (string | null)[][];
}

/** RFC-4180-ish CSV parser: quoted fields, "" escapes, CR/LF in quotes. */
export function parseCsv(text: string): ParsedData {
  const rows: string[][] = [];
  let field = "";
  let row: string[] = [];
  let i = 0;
  let inQuotes = false;
  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    pushField();
    rows.push(row);
    row = [];
  };
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
        } else {
          inQuotes = false;
          i++;
        }
      } else {
        field += c;
        i++;
      }
    } else if (c === '"') {
      inQuotes = true;
      i++;
    } else if (c === ",") {
      pushField();
      i++;
    } else if (c === "\r") {
      i++; // handled by the \n that follows, or a lone \r
      if (text[i] !== "\n") pushRow();
    } else if (c === "\n") {
      pushRow();
      i++;
    } else {
      field += c;
      i++;
    }
  }
  // Trailing field/row (no final newline).
  if (field !== "" || row.length > 0) pushRow();
  // Drop a trailing empty row (file ended with newline).
  while (rows.length && rows[rows.length - 1].every((f) => f === "")) rows.pop();
  if (rows.length === 0) return { columns: [], rows: [] };

  const columns = rows[0].map((h, idx) => h.trim() || `col${idx + 1}`);
  const dataRows = rows.slice(1).map((r) =>
    columns.map((_, idx) => {
      const v = r[idx];
      return v === undefined || v === "" ? null : v;
    })
  );
  return { columns, rows: dataRows };
}

/** JSON array of objects → columns (union of keys, first-seen order). */
export function parseJson(text: string): ParsedData {
  const data = JSON.parse(text);
  if (!Array.isArray(data)) {
    throw new Error("JSON import expects an array of objects");
  }
  const columns: string[] = [];
  const seen = new Set<string>();
  for (const item of data) {
    if (item === null || typeof item !== "object" || Array.isArray(item)) {
      throw new Error("every array element must be a JSON object");
    }
    for (const k of Object.keys(item)) {
      if (!seen.has(k)) {
        seen.add(k);
        columns.push(k);
      }
    }
  }
  const rows = data.map((item: Record<string, unknown>) =>
    columns.map((c) => {
      const v = item[c];
      if (v === undefined || v === null) return null;
      return typeof v === "object" ? JSON.stringify(v) : String(v);
    })
  );
  return { columns, rows };
}

const INT_RE = /^-?\d+$/;
const FLOAT_RE = /^-?\d*\.\d+$/;
const BOOL_RE = /^(true|false)$/i;

/** Infer a SQL type from up to `sample` non-null values of a column. */
export function inferType(values: (string | null)[], sample = 200): string {
  let seen = 0;
  let allInt = true;
  let allNum = true;
  let allBool = true;
  for (const v of values) {
    if (v === null) continue;
    seen++;
    if (!INT_RE.test(v)) allInt = false;
    if (!INT_RE.test(v) && !FLOAT_RE.test(v)) allNum = false;
    if (!BOOL_RE.test(v)) allBool = false;
    if (seen >= sample) break;
  }
  if (seen === 0) return "TEXT";
  if (allBool) return "BOOL";
  if (allInt) return "INT";
  if (allNum) return "DOUBLE";
  return "TEXT";
}

/** Coerce a cell string to the JSON param value for its SQL type. */
export function coerceValue(v: string | null, type: string): unknown {
  if (v === null) return null;
  switch (type) {
    case "INT":
      return parseInt(v, 10);
    case "DOUBLE":
      return parseFloat(v);
    case "BOOL":
      return /^true$/i.test(v);
    default:
      return v;
  }
}
