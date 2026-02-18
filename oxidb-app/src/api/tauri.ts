import { invoke } from "@tauri-apps/api/core";
import type {
  ConnectionStatus,
  DashboardStats,
  FindParams,
  JsonValue,
} from "./types";

// Connection
export const openEmbedded = (path: string) =>
  invoke<ConnectionStatus>("open_embedded", { path });

export const connectRemote = (host: string, port: number) =>
  invoke<ConnectionStatus>("connect_remote", { host, port });

export const disconnect = () => invoke<void>("disconnect");

export const getConnectionStatus = () =>
  invoke<ConnectionStatus>("get_connection_status");

// Dashboard
export const getDashboardStats = () =>
  invoke<DashboardStats>("get_dashboard_stats");

// Collections
export const listCollections = () =>
  invoke<string[]>("list_collections");

export const createCollection = (name: string) =>
  invoke<string>("create_collection", { name });

export const dropCollection = (name: string) =>
  invoke<string>("drop_collection", { name });

export const compactCollection = (name: string) =>
  invoke<JsonValue>("compact_collection", { name });

// Documents
export const findDocuments = (params: FindParams) =>
  invoke<JsonValue[]>("find_documents", { params });

export const insertDocument = (collection: string, doc: JsonValue) =>
  invoke<JsonValue>("insert_document", { collection, doc });

export const updateDocuments = (
  collection: string,
  query: JsonValue,
  update: JsonValue
) => invoke<JsonValue>("update_documents", { collection, query, update });

export const deleteDocuments = (collection: string, query: JsonValue) =>
  invoke<JsonValue>("delete_documents", { collection, query });

export const countDocuments = (collection: string, query?: JsonValue) =>
  invoke<number>("count_documents", { collection, query });

// Indexes
export const listIndexes = (collection: string) =>
  invoke<JsonValue>("list_indexes", { collection });

export const createIndex = (collection: string, field: string) =>
  invoke<string>("create_index", { collection, field });

export const createUniqueIndex = (collection: string, field: string) =>
  invoke<string>("create_unique_index", { collection, field });

export const createCompositeIndex = (collection: string, fields: string[]) =>
  invoke<string>("create_composite_index", { collection, fields });

export const createTextIndex = (collection: string, fields: string[]) =>
  invoke<string>("create_text_index", { collection, fields });

export const dropIndex = (collection: string, index: string) =>
  invoke<string>("drop_index", { collection, index });

// Query
export const executeRawCommand = (command: JsonValue) =>
  invoke<JsonValue>("execute_raw_command", { command });

// Aggregation
export const runAggregation = (collection: string, pipeline: JsonValue) =>
  invoke<JsonValue[]>("run_aggregation", { collection, pipeline });

// Blobs
export const listBuckets = () => invoke<string[]>("list_buckets");

export const createBucket = (name: string) =>
  invoke<string>("create_bucket", { name });

export const deleteBucket = (name: string) =>
  invoke<string>("delete_bucket", { name });

export const listObjects = (
  bucket: string,
  prefix?: string,
  limit?: number
) => invoke<JsonValue[]>("list_objects", { bucket, prefix, limit });

export const putObject = (
  bucket: string,
  key: string,
  dataB64: string,
  contentType?: string,
  metadata?: Record<string, string>
) =>
  invoke<JsonValue>("put_object", {
    bucket,
    key,
    dataB64,
    contentType,
    metadata,
  });

export const getObject = (bucket: string, key: string) =>
  invoke<JsonValue>("get_object", { bucket, key });

export const deleteObject = (bucket: string, key: string) =>
  invoke<string>("delete_object", { bucket, key });

export const searchObjects = (
  query: string,
  bucket?: string,
  limit?: number
) => invoke<JsonValue[]>("search_objects", { query, bucket, limit });

// Transactions
export const beginTransaction = () =>
  invoke<JsonValue>("begin_transaction");

export const commitTransaction = () =>
  invoke<string>("commit_transaction");

export const rollbackTransaction = () =>
  invoke<string>("rollback_transaction");
