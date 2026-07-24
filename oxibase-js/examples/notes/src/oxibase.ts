// The whole "connect to OxiBase" story in one call. Three values come from the
// OxiBase dashboard (Open a project → API keys):
//   VITE_OXIBASE_URL  the data-plane REST origin (the OxiDB server)
//   VITE_OXIBASE_REF  the project ref
//   VITE_OXIBASE_KEY  a project key (service_role for this demo — see README)
import { createClient } from "oxibase-js";

const url = import.meta.env.VITE_OXIBASE_URL ?? "http://127.0.0.1:8087";
const ref = import.meta.env.VITE_OXIBASE_REF ?? "";
const key = import.meta.env.VITE_OXIBASE_KEY ?? "";

export const configured = Boolean(ref && key);

export const oxibase = createClient(url, key, { ref });
