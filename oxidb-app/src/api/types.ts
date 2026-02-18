export interface ConnectionStatus {
  connected: boolean;
  mode: string;
  detail: string;
}

export interface CollectionStat {
  name: string;
  doc_count: number;
  storage_bytes: number;
}

export interface DashboardStats {
  collections: CollectionStat[];
  total_docs: number;
  total_storage_bytes: number;
}

export interface IndexInfo {
  name: string;
  index_type: string;
  fields: string[];
  unique: boolean;
}

export interface FindParams {
  collection: string;
  query?: Record<string, unknown>;
  sort?: Record<string, number>;
  skip?: number;
  limit?: number;
}

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };
