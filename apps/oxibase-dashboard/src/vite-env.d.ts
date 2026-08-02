/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_OXIBASE_URL?: string;
  readonly VITE_OXIDB_URL?: string;
}
interface ImportMeta {
  readonly env: ImportMetaEnv;
}
