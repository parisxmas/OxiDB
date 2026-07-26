import { useMemo } from "react";
import type { LogRow } from "./api.ts";
import { WORLD_H, WORLD_PATH, WORLD_W } from "./worldPath.ts";

/** Where the requests on the loaded pages came from.
 *
 *  Coordinates are Cloudflare's `CF-IPLatitude`/`CF-IPLongitude` — a city's
 *  position, not a person's, and only present when the zone sends location
 *  headers. Rows without them are counted in the caption rather than dropped
 *  silently, so the map never implies it is showing everything.
 *
 *  Aggregated from the rows already fetched, like the location filter: asking
 *  the server to group over the log collection would be a full scan of an
 *  unbounded, unindexed-by-place collection.
 */
export function LogsMap({
  rows,
  selected,
  onSelect,
}: {
  rows: LogRow[];
  selected: string;
  onSelect: (place: string) => void;
}) {
  const { points, placed, missing } = useMemo(() => {
    const byPlace = new Map<string, { x: number; y: number; n: number; label: string }>();
    let placed = 0;
    let missing = 0;
    for (const r of rows) {
      const lat = Number(r.lat);
      const lon = Number(r.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) {
        missing++;
        continue;
      }
      placed++;
      const label = r.city || r.country || `${lat.toFixed(1)},${lon.toFixed(1)}`;
      const hit = byPlace.get(label);
      if (hit) {
        hit.n++;
      } else {
        byPlace.set(label, {
          x: ((lon + 180) / 360) * WORLD_W,
          y: ((90 - lat) / 180) * WORLD_H,
          n: 1,
          label,
        });
      }
    }
    return { points: [...byPlace.values()].sort((a, b) => b.n - a.n), placed, missing };
  }, [rows]);

  const busiest = points[0]?.n ?? 1;
  // Area, not radius, tracks the count — a bubble twice as wide reads as four
  // times as much, which is how a bubble map misleads if you scale the radius.
  const radius = (n: number) => 3 + 9 * Math.sqrt(n / busiest);

  return (
    <div style={{ marginTop: 10 }}>
      <div
        style={{
          border: "1px solid var(--line, rgba(127,127,127,0.25))",
          borderRadius: 10,
          overflow: "hidden",
          background: "rgba(127,150,190,0.06)",
        }}
      >
        <svg viewBox={`0 0 ${WORLD_W} ${WORLD_H}`} style={{ display: "block", width: "100%" }}>
          <path d={WORLD_PATH} fill="rgba(127,140,165,0.28)" stroke="rgba(127,140,165,0.5)" strokeWidth={0.4} />
          {points.map((p) => {
            const on = selected === "" || selected.endsWith(p.label);
            return (
              <g
                key={p.label}
                onClick={() => onSelect(p.label)}
                style={{ cursor: "pointer" }}
                opacity={on ? 1 : 0.35}
              >
                <title>{`${p.label} · ${p.n} request${p.n === 1 ? "" : "s"}`}</title>
                <circle cx={p.x} cy={p.y} r={radius(p.n)} fill="rgba(79,163,255,0.35)" />
                <circle cx={p.x} cy={p.y} r={2} fill="#4FA3FF" />
              </g>
            );
          })}
        </svg>
      </div>
      <div className="muted small" style={{ marginTop: 6 }}>
        {points.length} location{points.length === 1 ? "" : "s"} · {placed} request
        {placed === 1 ? "" : "s"} placed
        {missing > 0 && ` · ${missing} without coordinates`}
        {points.length > 0 && " · click a bubble to filter"}
      </div>
    </div>
  );
}
