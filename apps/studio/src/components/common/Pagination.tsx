interface Props {
  page: number; // 0-based
  pageSize: number;
  /** Total rows if known (server-side); omit for client-side over `count`. */
  total?: number;
  /** Rows on the current page (used when total is unknown to detect the end). */
  currentCount: number;
  onPage: (page: number) => void;
  onPageSize: (size: number) => void;
  busy?: boolean;
}

const SIZES = [50, 100, 500, 1000];

/** Prev/next pager with a page-size selector and a "X–Y of N" readout. */
export function Pagination({
  page,
  pageSize,
  total,
  currentCount,
  onPage,
  onPageSize,
  busy,
}: Props) {
  const from = currentCount === 0 ? 0 : page * pageSize + 1;
  const to = page * pageSize + currentCount;
  const lastPage = total != null ? Math.max(0, Math.ceil(total / pageSize) - 1) : null;
  const atEnd =
    lastPage != null ? page >= lastPage : currentCount < pageSize;

  return (
    <div className="pager">
      <button
        className="pager-btn"
        onClick={() => onPage(0)}
        disabled={page === 0 || busy}
        title="First"
      >
        «
      </button>
      <button
        className="pager-btn"
        onClick={() => onPage(page - 1)}
        disabled={page === 0 || busy}
        title="Previous"
      >
        ‹
      </button>
      <span className="pager-info">
        {from.toLocaleString()}–{to.toLocaleString()}
        {total != null ? ` of ${total.toLocaleString()}` : ""}
      </span>
      <button
        className="pager-btn"
        onClick={() => onPage(page + 1)}
        disabled={atEnd || busy}
        title="Next"
      >
        ›
      </button>
      {lastPage != null && (
        <button
          className="pager-btn"
          onClick={() => onPage(lastPage)}
          disabled={page >= lastPage || busy}
          title="Last"
        >
          »
        </button>
      )}
      <select
        className="pager-size"
        value={pageSize}
        onChange={(e) => onPageSize(parseInt(e.target.value, 10))}
        disabled={busy}
      >
        {SIZES.map((s) => (
          <option key={s} value={s}>
            {s} / page
          </option>
        ))}
      </select>
    </div>
  );
}
