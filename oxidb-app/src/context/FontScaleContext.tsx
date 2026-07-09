import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";

interface FontScaleCtx {
  scale: number;
  inc: () => void;
  dec: () => void;
  reset: () => void;
}

const MIN = 0.7;
const MAX = 1.8;
const STEP = 0.1;
const clamp = (n: number) => Math.min(MAX, Math.max(MIN, Math.round(n * 100) / 100));

const FontScaleContext = createContext<FontScaleCtx>({
  scale: 1,
  inc: () => {},
  dec: () => {},
  reset: () => {},
});

/**
 * Global UI scale. Applied as CSS `zoom` on the document root so *every*
 * font on screen — including Monaco and hard-coded px sizes — scales together,
 * like browser zoom. Persisted; driven from the header and ⌘+ / ⌘- / ⌘0.
 */
export function FontScaleProvider({ children }: { children: ReactNode }) {
  const [scale, setScale] = useState<number>(() => {
    const v = parseFloat(localStorage.getItem("oxidb-font-scale") || "1");
    return Number.isFinite(v) ? clamp(v) : 1;
  });

  useEffect(() => {
    // `zoom` is honored by the Tauri webview (Chromium/WebKit) and scales
    // layout with the text, which is the intended "make everything bigger".
    (document.documentElement.style as CSSStyleDeclaration & { zoom?: string }).zoom =
      String(scale);
    localStorage.setItem("oxidb-font-scale", String(scale));
  }, [scale]);

  const inc = useCallback(() => setScale((s) => clamp(s + STEP)), []);
  const dec = useCallback(() => setScale((s) => clamp(s - STEP)), []);
  const reset = useCallback(() => setScale(1), []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key === "=" || e.key === "+") {
        e.preventDefault();
        inc();
      } else if (e.key === "-") {
        e.preventDefault();
        dec();
      } else if (e.key === "0") {
        e.preventDefault();
        reset();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [inc, dec, reset]);

  return (
    <FontScaleContext.Provider value={{ scale, inc, dec, reset }}>
      {children}
    </FontScaleContext.Provider>
  );
}

export const useFontScale = () => useContext(FontScaleContext);
