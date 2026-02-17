# frozen_string_literal: true

# OxiDB Ruby client library.
#
# Zero external dependencies — uses only the Ruby standard library.
# Communicates with oxidb-server over TCP using the length-prefixed JSON protocol.
#
# Usage:
#   require_relative 'lib/oxidb'
#
#   db = OxiDb::Client.new("127.0.0.1", 4444)
#   db.insert("users", { name: "Alice", age: 30 })
#   docs = db.find("users", { name: "Alice" })
#   db.close
#
#   # or with a block:
#   OxiDb::Client.open("127.0.0.1", 4444) do |db|
#     db.insert("users", { name: "Bob" })
#   end

require "socket"
require "json"
require "base64"

module OxiDb
  class Error < StandardError; end
  class TransactionConflictError < Error; end

  # TCP client for oxidb-server.
  #
  # Protocol: each message is [4-byte little-endian length][JSON payload].
  # Server responds with {"ok" => true, "data" => ...} or {"ok" => false, "error" => "..."}.
  #
  # Thread-safe: all send/receive operations are synchronized via Mutex.
  class Client
    # Connect to oxidb-server.
    #
    # @param host [String] server host (default "127.0.0.1")
    # @param port [Integer] server port (default 4444)
    # @param timeout [Numeric] connect/read timeout in seconds (default 5)
    def initialize(host = "127.0.0.1", port = 4444, timeout: 5)
      @sock = Socket.tcp(host, port, connect_timeout: timeout)
      @mutex = Mutex.new
    end

    # Connect and yield the client, ensuring close on block exit.
    def self.open(host = "127.0.0.1", port = 4444, **opts)
      client = new(host, port, **opts)
      begin
        yield client
      ensure
        client.close
      end
    end

    # Close the TCP connection.
    def close
      @sock.close rescue nil
    end

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    # Ping the server. Returns "pong".
    def ping
      checked(cmd: "ping")
    end

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    # Explicitly create a collection.
    def create_collection(name)
      checked(cmd: "create_collection", collection: name)
    end

    # Return a list of collection names.
    def list_collections
      checked(cmd: "list_collections")
    end

    # Drop a collection and its data.
    def drop_collection(name)
      checked(cmd: "drop_collection", collection: name)
    end

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    # Insert a single document. Returns {"id" => ...} outside tx, "buffered" inside tx.
    def insert(collection, doc)
      checked(cmd: "insert", collection: collection, doc: doc)
    end

    # Insert multiple documents.
    def insert_many(collection, docs)
      checked(cmd: "insert_many", collection: collection, docs: docs)
    end

    # Find documents matching a query.
    #
    # @param collection [String]
    # @param query [Hash] (default {})
    # @param sort [Hash, nil] sort specification
    # @param skip [Integer, nil]
    # @param limit [Integer, nil]
    # @return [Array<Hash>]
    def find(collection, query = {}, sort: nil, skip: nil, limit: nil)
      payload = { cmd: "find", collection: collection, query: query }
      payload[:sort] = sort if sort
      payload[:skip] = skip if skip
      payload[:limit] = limit if limit
      checked(payload)
    end

    # Find a single document matching a query. Returns the document or nil.
    def find_one(collection, query = {})
      checked(cmd: "find_one", collection: collection, query: query)
    end

    # Update documents matching a query. Returns {"modified" => n} outside tx.
    def update(collection, query, update)
      checked(cmd: "update", collection: collection, query: query, update: update)
    end

    # Delete documents matching a query. Returns {"deleted" => n} outside tx.
    def delete(collection, query)
      checked(cmd: "delete", collection: collection, query: query)
    end

    # Count documents matching a query.
    def count(collection, query = {})
      result = checked(cmd: "count", collection: collection, query: query)
      result["count"]
    end

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    # Create a non-unique index on a field.
    def create_index(collection, field)
      checked(cmd: "create_index", collection: collection, field: field)
    end

    # Create a unique index on a field.
    def create_unique_index(collection, field)
      checked(cmd: "create_unique_index", collection: collection, field: field)
    end

    # Create a composite index on multiple fields.
    def create_composite_index(collection, fields)
      checked(cmd: "create_composite_index", collection: collection, fields: fields)
    end

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    # Run an aggregation pipeline. Returns list of result documents.
    def aggregate(collection, pipeline)
      checked(cmd: "aggregate", collection: collection, pipeline: pipeline)
    end

    # ------------------------------------------------------------------
    # Compaction
    # ------------------------------------------------------------------

    # Compact a collection. Returns {old_size, new_size, docs_kept}.
    def compact(collection)
      checked(cmd: "compact", collection: collection)
    end

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    # Begin a transaction on this connection. Returns {"tx_id" => ...}.
    def begin_tx
      checked(cmd: "begin_tx")
    end

    # Commit the active transaction. Raises TransactionConflictError on OCC conflict.
    def commit_tx
      checked(cmd: "commit_tx")
    end

    # Rollback the active transaction.
    def rollback_tx
      checked(cmd: "rollback_tx")
    end

    # Execute a block within a transaction. Auto-commits on success, auto-rolls back on exception.
    #
    #   db.transaction do
    #     db.insert("col", { x: 1 })
    #     db.update("col", { x: 1 }, { "$set" => { x: 2 } })
    #   end
    def transaction
      begin_tx
      begin
        yield
        commit_tx
      rescue => e
        begin
          rollback_tx
        rescue Error
          # rollback may fail if commit already failed
        end
        raise
      end
    end

    # ------------------------------------------------------------------
    # Blob storage
    # ------------------------------------------------------------------

    # Create a blob storage bucket.
    def create_bucket(bucket)
      checked(cmd: "create_bucket", bucket: bucket)
    end

    # List all blob storage buckets.
    def list_buckets
      checked(cmd: "list_buckets")
    end

    # Delete a blob storage bucket.
    def delete_bucket(bucket)
      checked(cmd: "delete_bucket", bucket: bucket)
    end

    # Upload a blob object. Data is base64-encoded automatically.
    #
    # @param bucket [String]
    # @param key [String]
    # @param data [String] raw binary data
    # @param content_type [String]
    # @param metadata [Hash, nil]
    def put_object(bucket, key, data, content_type: "application/octet-stream", metadata: nil)
      payload = {
        cmd: "put_object",
        bucket: bucket,
        key: key,
        data: Base64.strict_encode64(data),
        content_type: content_type
      }
      payload[:metadata] = metadata if metadata && !metadata.empty?
      checked(payload)
    end

    # Download a blob object. Returns [data_bytes, metadata_hash].
    def get_object(bucket, key)
      result = checked(cmd: "get_object", bucket: bucket, key: key)
      data = Base64.strict_decode64(result["content"])
      [data, result["metadata"]]
    end

    # Get blob object metadata without downloading the content.
    def head_object(bucket, key)
      checked(cmd: "head_object", bucket: bucket, key: key)
    end

    # Delete a blob object.
    def delete_object(bucket, key)
      checked(cmd: "delete_object", bucket: bucket, key: key)
    end

    # List objects in a bucket.
    def list_objects(bucket, prefix: nil, limit: nil)
      payload = { cmd: "list_objects", bucket: bucket }
      payload[:prefix] = prefix if prefix
      payload[:limit] = limit if limit
      checked(payload)
    end

    # ------------------------------------------------------------------
    # Full-text search
    # ------------------------------------------------------------------

    # Full-text search across blobs. Returns [{bucket, key, score}, ...].
    def search(query, bucket: nil, limit: 10)
      payload = { cmd: "search", query: query, limit: limit }
      payload[:bucket] = bucket if bucket
      checked(payload)
    end

    private

    # ------------------------------------------------------------------
    # Low-level protocol
    # ------------------------------------------------------------------

    def send_raw(data)
      @sock.write([data.bytesize].pack("V"))
      @sock.write(data)
    end

    def recv_raw
      len_bytes = recv_exact(4)
      length = len_bytes.unpack1("V")
      recv_exact(length)
    end

    def recv_exact(n)
      buf = "".b
      while buf.bytesize < n
        chunk = @sock.read(n - buf.bytesize)
        raise Error, "connection closed by server" if chunk.nil? || chunk.empty?
        buf << chunk
      end
      buf
    end

    def request(payload)
      @mutex.synchronize do
        json_str = JSON.generate(payload)
        send_raw(json_str)
        resp_bytes = recv_raw
        JSON.parse(resp_bytes)
      end
    end

    def checked(payload)
      resp = request(payload)
      unless resp["ok"]
        error_msg = resp["error"] || "unknown error"
        if error_msg.downcase.include?("conflict")
          raise TransactionConflictError, error_msg
        end
        raise Error, error_msg
      end
      resp["data"]
    end
  end
end
