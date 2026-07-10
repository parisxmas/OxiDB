// Inline stroke icons (currentColor, 18px) for the sidebar — no external
// assets, so they work under the app's strict CSP.

type P = { size?: number };

const base = (size: number) => ({
  width: size,
  height: size,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.8,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
});

export function IconDashboard({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <rect x="3" y="3" width="7" height="9" rx="1" />
      <rect x="14" y="3" width="7" height="5" rx="1" />
      <rect x="14" y="12" width="7" height="9" rx="1" />
      <rect x="3" y="16" width="7" height="5" rx="1" />
    </svg>
  );
}

export function IconCollections({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M12 3 3 7.5 12 12l9-4.5L12 3Z" />
      <path d="M3 12l9 4.5L21 12" />
      <path d="M3 16.5 12 21l9-4.5" />
    </svg>
  );
}

export function IconQuery({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <circle cx="11" cy="11" r="7" />
      <path d="m20 20-3.2-3.2" />
    </svg>
  );
}

export function IconSql({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <ellipse cx="12" cy="5" rx="8" ry="3" />
      <path d="M4 5v6c0 1.66 3.58 3 8 3s8-1.34 8-3V5" />
      <path d="M4 11v6c0 1.66 3.58 3 8 3s8-1.34 8-3v-6" />
    </svg>
  );
}

export function IconOxiMem({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <rect x="6" y="6" width="12" height="12" rx="1.5" />
      <path d="M9 2v3M12 2v3M15 2v3M9 19v3M12 19v3M15 19v3M2 9h3M2 12h3M2 15h3M19 9h3M19 12h3M19 15h3" />
    </svg>
  );
}

export function IconIndexes({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M8 6h13M8 12h13M8 18h13" />
      <path d="M3 6h.01M3 12h.01M3 18h.01" />
    </svg>
  );
}

export function IconBlobs({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M21 8 12 3 3 8v8l9 5 9-5V8Z" />
      <path d="M3 8l9 5 9-5" />
      <path d="M12 13v8" />
    </svg>
  );
}

export function IconAggregation({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M3 4h18l-7 8v6l-4 2v-8L3 4Z" />
    </svg>
  );
}

export function IconMonitor({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M3 12h4l2 6 4-14 2 8h6" />
    </svg>
  );
}

export function IconTable({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <rect x="3" y="4" width="18" height="16" rx="1.5" />
      <path d="M3 9h18M3 14.5h18M9 9v11" />
    </svg>
  );
}

export function IconKey({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <circle cx="7.5" cy="15.5" r="3.5" />
      <path d="M10 13 20 3M17 6l2 2M14 9l2 2" />
    </svg>
  );
}

export function IconFunction({ size = 18 }: P) {
  return (
    <svg {...base(size)}>
      <path d="M8 21c2 0 3-1 3.4-3.5L14 4c.4-2.5 1.4-3 3-3" />
      <path d="M5 9h9" />
    </svg>
  );
}
