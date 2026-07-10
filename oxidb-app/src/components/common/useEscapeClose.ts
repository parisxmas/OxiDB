import { useEffect } from "react";

/**
 * Close a dialog on the Escape key. Used since dialogs no longer close on an
 * outside click (that was losing in-progress edits). A capture-phase listener
 * catches Escape even when a Monaco editor has focus.
 */
export function useEscapeClose(onClose: () => void) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [onClose]);
}
