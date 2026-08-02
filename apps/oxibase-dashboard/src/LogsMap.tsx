import { useEffect, useMemo, useRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { LogRow } from "./api.ts";
import { WORLD_CITIES, WORLD_COUNTRIES } from "./worldGeo.ts";

/** Where the requests on the loaded pages came from — pan and zoom.
 *
 *  Leaflet, drawing Natural Earth's public-domain countries as a vector layer
 *  rather than fetching raster tiles: you get zoom and pan, borders and city
 *  names, and the dashboard still talks to your server and nobody else. A tile
 *  provider would otherwise learn which projects you look at, and when.
 *
 *  Coordinates are Cloudflare's `CF-IPLatitude`/`CF-IPLongitude` — a city's
 *  position, not a person's. Rows without them are counted in the caption rather
 *  than dropped silently, so the map never implies it is showing everything.
 *
 *  Aggregated from the rows already fetched, like the location filter: asking
 *  the server to group over the log collection would be a full scan of an
 *  unbounded, unindexed-by-place collection. Service-key requests are filtered
 *  out by the caller — those are your own backend, and its datacentre is not a
 *  place any of your users live.
 */
export function LogsMap({
  rows,
  serverSide = 0,
  arrivals,
  selected,
  onSelect,
}: {
  rows: LogRow[];
  /** How many rows the caller held back as machine traffic — worth saying, so an
   *  empty map reads as "none of this was a visitor" rather than "broken". */
  serverSide?: number;
  /** Places from the newest fetch, with a counter that changes each fetch so the
   *  same place arriving twice running still animates twice. */
  arrivals?: { places: string[]; nonce: number };
  selected: string;
  onSelect: (place: string) => void;
}) {
  const host = useRef<HTMLDivElement>(null);
  const map = useRef<L.Map | null>(null);
  const bubbles = useRef<L.LayerGroup | null>(null);
  // Stop refitting once the reader has moved the map themselves: a view that
  // jumps back every five seconds under Live is worse than one that never fits.
  const touched = useRef(false);
  const fitting = useRef(false);
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;

  const { points, placed, missing } = useMemo(() => {
    const byPlace = new Map<string, { lat: number; lon: number; n: number; label: string }>();
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
      if (hit) hit.n++;
      else byPlace.set(label, { lat, lon, n: 1, label });
    }
    return { points: [...byPlace.values()].sort((a, b) => b.n - a.n), placed, missing };
  }, [rows]);

  // The map itself is created once; only its layers change.
  useEffect(() => {
    if (map.current || !host.current) return;
    const m = L.map(host.current, {
      center: [25, 10],
      zoom: 1,
      minZoom: 1,
      maxZoom: 8,
      worldCopyJump: true,
      attributionControl: false,
      // A drag inside the map should not also scroll the page it sits in.
      scrollWheelZoom: true,
    });
    L.geoJSON(WORLD_COUNTRIES, {
      style: {
        color: "rgba(127,140,165,0.75)",
        weight: 0.7,
        fillColor: "rgba(127,140,165,0.22)",
        fillOpacity: 1,
      },
      onEachFeature: (feature, layer) => {
        const name = (feature.properties as { name?: string })?.name;
        if (name) layer.bindTooltip(name, { sticky: true });
      },
    }).addTo(m);

    // City names, thinned by zoom: capitals and the largest cities at first,
    // more of them as you go in. A world map covered in labels is unreadable,
    // and one with none is a silhouette.
    const cityLayer = L.layerGroup().addTo(m);
    const drawCities = () => {
      cityLayer.clearLayers();
      const z = m.getZoom();
      // Some names at every zoom — a world view with no labels is the
      // silhouette this was meant to stop being. How many, and how small, grows
      // as you go in: at zoom 1 only the largest handful, by zoom 6 everything
      // Natural Earth knows about.
      const minPop = [9e6, 9e6, 6e6, 3e6, 1.5e6, 8e5, 3e5, 0, 0][Math.min(z, 8)];
      const cap = [8, 12, 24, 48, 90, 160, 243, 243, 243][Math.min(z, 8)];
      const bounds = m.getBounds();
      const shown = WORLD_CITIES.filter(
        (c) => (c.pop >= minPop || (c.cap === 1 && z >= 3)) && bounds.contains([c.y, c.x]),
      ).slice(0, cap);
      for (const c of shown) {
        L.marker([c.y, c.x], {
          interactive: false,
          icon: L.divIcon({
            className: "map-city",
            html: `<span>${c.n}</span>`,
            iconSize: [0, 0],
          }),
        }).addTo(cityLayer);
      }
    };
    drawCities();
    m.on("zoomend moveend", drawCities);

    // Only a drag or a wheel counts as the reader taking over; fitBounds fires
    // the same events, so it flags itself first.
    const claim = () => {
      if (!fitting.current) touched.current = true;
    };
    m.on("dragstart", claim);
    m.on("zoomstart", claim);

    bubbles.current = L.layerGroup().addTo(m);
    map.current = m;
    return () => {
      m.remove();
      map.current = null;
      bubbles.current = null;
    };
  }, []);

  // Bubbles: redrawn when the counts change. Area tracks the count, not radius —
  // a circle twice as wide reads as four times the traffic.
  useEffect(() => {
    const layer = bubbles.current;
    if (!layer) return;
    layer.clearLayers();
    const busiest = points[0]?.n ?? 1;
    for (const p of points) {
      const dim = selected !== "" && !selected.endsWith(p.label);
      const marker = L.circleMarker([p.lat, p.lon], {
        radius: 4 + 11 * Math.sqrt(p.n / busiest),
        color: "#4FA3FF",
        weight: 1,
        fillColor: "#4FA3FF",
        fillOpacity: dim ? 0.12 : 0.4,
        opacity: dim ? 0.35 : 1,
      })
        // Named on the map, not on hover. The cities your traffic comes from are
        // mostly not in a 243-place gazetteer — Kardzhali and İzmir are not —
        // so without this they are unlabelled dots among labelled cities that
        // have nothing to do with you.
        .bindTooltip(`${p.label} · ${p.n}`, {
          permanent: true,
          direction: "right",
          offset: [4, 0],
          className: `map-bubble-label${dim ? " dim" : ""}`,
          opacity: 1,
        })
        .on("click", () => onSelectRef.current(p.label));
      layer.addLayer(marker);
    }

    // Frame the requests. One place gets a sensible city-level zoom rather than
    // the maximum, which would show a street corner an IP cannot justify.
    const m = map.current;
    if (m && points.length > 0 && !touched.current) {
      fitting.current = true;
      const bounds = L.latLngBounds(points.map((p) => [p.lat, p.lon] as [number, number]));
      m.fitBounds(bounds.pad(0.35), { maxZoom: points.length === 1 ? 6 : 7, animate: false });
      // Released after the events fitBounds triggers have been handled.
      window.setTimeout(() => {
        fitting.current = false;
      }, 0);
    }
  }, [points, selected]);

  // A request just arrived from here: one expanding ring, twice, then gone.
  useEffect(() => {
    const m = map.current;
    if (!m || !arrivals || arrivals.places.length === 0) return;
    const rings: L.CircleMarker[] = [];
    for (const p of points) {
      if (!arrivals.places.some((a) => a.endsWith(p.label))) continue;
      const ring = L.circleMarker([p.lat, p.lon], {
        radius: 6,
        color: "#4FA3FF",
        weight: 2,
        fill: false,
        className: "map-ping",
        interactive: false,
      }).addTo(m);
      rings.push(ring);
    }
    // Removed when the animation is over, so nothing accumulates on a long Live
    // session.
    const done = window.setTimeout(() => rings.forEach((r) => r.remove()), 2900);
    return () => {
      window.clearTimeout(done);
      rings.forEach((r) => r.remove());
    };
  }, [arrivals?.nonce, points]);

  return (
    <div style={{ marginTop: 10 }}>
      <div
        ref={host}
        style={{
          height: 320,
          borderRadius: 10,
          overflow: "hidden",
          border: "1px solid var(--line, rgba(127,127,127,0.25))",
          background: "rgba(127,150,190,0.06)",
        }}
      />
      <div className="row between" style={{ marginTop: 6 }}>
        <span className="muted small">
          {points.length} location{points.length === 1 ? "" : "s"} · {placed} request
        {placed === 1 ? "" : "s"} placed
          {missing > 0 && ` · ${missing} without coordinates`}
          {serverSide > 0 && ` · ${serverSide} from your backend, not shown`}
          {points.length > 0 && " · click a bubble to filter · scroll to zoom"}
        </span>
        {points.length > 0 && (
          <button
            className="ghost small"
            onClick={() => {
              touched.current = false;
              const m = map.current;
              if (!m) return;
              fitting.current = true;
              const bounds = L.latLngBounds(points.map((p) => [p.lat, p.lon] as [number, number]));
              m.fitBounds(bounds.pad(0.35), { maxZoom: points.length === 1 ? 6 : 7 });
              window.setTimeout(() => {
                fitting.current = false;
              }, 0);
            }}
            title="Frame the requests again"
          >
            Fit
          </button>
        )}
      </div>
    </div>
  );
}
