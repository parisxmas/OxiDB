/** Page controls shared by every table in the dashboard.
 *
 *  Nothing here fetches a whole table. Each panel asks for `PAGE_SIZE + 1` rows
 *  and shows `PAGE_SIZE` of them — the extra row is how it knows a next page
 *  exists without counting the collection.
 */
export const PAGE_SIZE = 50;

export function Pager({
  page,
  hasNext,
  shown,
  total,
  onPage,
  disabled = false,
  unit = "rows",
}: {
  page: number;
  hasNext: boolean;
  /** How many rows are on screen. */
  shown: number;
  /** How many were loaded before filtering, when a filter is applied. */
  total?: number;
  onPage: (next: number) => void;
  disabled?: boolean;
  unit?: string;
}) {
  return (
    <div className="row between" style={{ marginTop: 8 }}>
      <span className="muted small">
        page {page + 1} · {shown}
        {total !== undefined && total !== shown ? ` of ${total}` : ""} {unit}
      </span>
      <div className="row" style={{ gap: 6 }}>
        <button
          className="ghost small"
          disabled={page === 0 || disabled}
          onClick={() => onPage(Math.max(0, page - 1))}
        >
          ← Newer
        </button>
        <button className="ghost small" disabled={!hasNext || disabled} onClick={() => onPage(page + 1)}>
          Older →
        </button>
      </div>
    </div>
  );
}
