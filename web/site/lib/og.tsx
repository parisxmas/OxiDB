import { ImageResponse } from "next/og"
import versionData from "@/site-version.json"

// Shared Open Graph card renderer. Every page generates its own social image
// from its own metadata (title + description) via `next/og` — no hand-made PNG.
export const size = { width: 1200, height: 630 }
export const contentType = "image/png"

const VERSION = (versionData as { version: string }).version

type MetaLike = { title?: unknown; description?: unknown } | undefined

function readTitle(t: unknown): string {
  if (typeof t === "string") return t
  if (t && typeof t === "object" && "default" in t) {
    const d = (t as { default?: unknown }).default
    if (typeof d === "string") return d
  }
  return "OxiDB"
}

export function ogImage(meta?: MetaLike) {
  const title = readTitle(meta?.title)
  const description =
    typeof meta?.description === "string" && meta.description.length > 0
      ? meta.description
      : "A fast, multi-model database."
  const titleSize = title.length > 26 ? 68 : title.length > 16 ? 88 : 108

  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          backgroundColor: "#0a0c10",
          color: "#e7e9ed",
          fontFamily: "sans-serif",
          padding: "70px 80px",
          borderTop: "3px solid #e2784a",
          position: "relative",
        }}
      >
        {/* rust bloom from the top-left corner */}
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background:
              "radial-gradient(60% 75% at 12% 0%, rgba(226,120,74,0.30), rgba(226,120,74,0) 70%)",
          }}
        />
        {/* steel bloom from the bottom-right corner */}
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            background:
              "radial-gradient(50% 60% at 100% 100%, rgba(124,158,180,0.16), rgba(124,158,180,0) 65%)",
          }}
        />

        {/* header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center" }}>
            <svg width="46" height="52" viewBox="0 0 72 82">
              <polygon
                points="36,0 72,18 72,54 36,72 0,54 0,18"
                fill="#e2784a"
                stroke="#a8451f"
              />
            </svg>
            <div style={{ display: "flex", fontSize: 27, marginLeft: 20 }}>
              <span>Oxi</span>
              <span style={{ color: "#e2784a" }}>DB</span>
            </div>
            <div
              style={{
                fontSize: 13,
                marginLeft: 18,
                letterSpacing: "0.18em",
                color: "#6c7585",
              }}
            >
              MULTI-MODEL DATABASE
            </div>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              border: "1px solid #a8451f",
              background: "rgba(226,120,74,0.12)",
              borderRadius: 20,
              padding: "8px 18px",
              color: "#e2784a",
              fontSize: 13,
              letterSpacing: "0.16em",
            }}
          >
            <div
              style={{
                width: 8,
                height: 8,
                borderRadius: 8,
                background: "#e2784a",
                marginRight: 10,
                display: "flex",
              }}
            />
            V{VERSION} · LATEST
          </div>
        </div>

        {/* body: page title + description */}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            flexGrow: 1,
            justifyContent: "center",
          }}
        >
          <div
            style={{
              display: "flex",
              fontSize: titleSize,
              fontWeight: 600,
              letterSpacing: "-0.03em",
              lineHeight: 1.05,
              maxWidth: 1010,
              backgroundImage: "linear-gradient(180deg,#ffffff 40%,#e2784a)",
              backgroundClip: "text",
              color: "transparent",
            }}
          >
            {title}
          </div>
          <div
            style={{
              display: "flex",
              fontSize: 30,
              color: "#a4adba",
              marginTop: 26,
              maxWidth: 980,
              lineHeight: 1.32,
            }}
          >
            {description}
          </div>
        </div>

        {/* footer */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            fontSize: 15,
            color: "#6c7585",
            letterSpacing: "0.04em",
          }}
        >
          <div style={{ display: "flex" }}>
            JSON · SQL · TSDB · Redis · MQTT · AMQP · S3
          </div>
          <div style={{ display: "flex" }}>oxidb.baltavista.com</div>
        </div>
      </div>
    ),
    size,
  )
}
