import * as page from "./page"
import { ogImage, size, contentType } from "@/lib/og"

export const dynamic = "force-static"
export { size, contentType }
export const alt = "OxiDB"

// Per-page social card, generated from this page's own metadata.
export default function Image() {
  return ogImage((page as { metadata?: { title?: unknown; description?: unknown } }).metadata)
}
