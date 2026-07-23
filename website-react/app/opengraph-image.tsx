import { ogImage, size, contentType } from "@/lib/og"

export const dynamic = "force-static"
export { size, contentType }
export const alt = "OxiDB — a fast, multi-model database"

// Root / homepage social card.
export default function Image() {
  return ogImage({ title: "OxiDB", description: "A fast, multi-model database." })
}
