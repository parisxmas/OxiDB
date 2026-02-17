# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/oxidb"

class OxiDbClientTest < Minitest::Test
  HOST = ENV.fetch("OXIDB_HOST", "127.0.0.1")
  PORT = ENV.fetch("OXIDB_PORT", "4444").to_i

  # Tests are stateful and must run in order
  def self.test_order
    :alpha
  end

  def setup
    @db = OxiDb::Client.new(HOST, PORT)
  end

  def teardown
    @db.close
  end

  def test_01_ping
    assert_equal "pong", @db.ping
  end

  def test_02_create_collection
    @db.create_collection("rb_test")
    cols = @db.list_collections
    assert_includes cols, "rb_test"
  end

  def test_03_insert_and_find
    result = @db.insert("rb_test", { "name" => "Alice", "age" => 30 })
    assert result["id"]

    docs = @db.find("rb_test", { "name" => "Alice" })
    assert docs.length >= 1
    assert_equal "Alice", docs[0]["name"]
    assert_equal 30, docs[0]["age"]
  end

  def test_04_insert_many
    result = @db.insert_many("rb_test", [
      { "name" => "Bob", "age" => 25 },
      { "name" => "Charlie", "age" => 35 }
    ])
    assert_kind_of Array, result
    assert_equal 2, result.length
  end

  def test_05_find_with_options
    docs = @db.find("rb_test", {}, sort: { "age" => 1 })
    assert docs.length >= 3

    docs = @db.find("rb_test", {}, limit: 1)
    assert_equal 1, docs.length
  end

  def test_06_find_one
    doc = @db.find_one("rb_test", { "name" => "Bob" })
    assert_equal "Bob", doc["name"]
  end

  def test_07_count
    n = @db.count("rb_test")
    assert n >= 3
  end

  def test_08_update
    result = @db.update("rb_test",
      { "name" => "Alice" },
      { "$set" => { "age" => 31 } })
    assert_equal 1, result["modified"]

    doc = @db.find_one("rb_test", { "name" => "Alice" })
    assert_equal 31, doc["age"]
  end

  def test_09_delete
    result = @db.delete("rb_test", { "name" => "Charlie" })
    assert_equal 1, result["deleted"]
  end

  def test_10_indexes
    @db.create_index("rb_test", "name")
    @db.create_unique_index("rb_test", "age")
    @db.create_composite_index("rb_test", ["name", "age"])
  end

  def test_11_aggregation
    results = @db.aggregate("rb_test", [
      { "$group" => { "_id" => nil, "avg_age" => { "$avg" => "$age" } } }
    ])
    assert results.length >= 1
  end

  def test_12_transaction
    @db.transaction do
      @db.insert("rb_tx", { "action" => "debit", "amount" => 100 })
      @db.insert("rb_tx", { "action" => "credit", "amount" => 100 })
    end

    docs = @db.find("rb_tx")
    assert_equal 2, docs.length
  end

  def test_13_blob_storage
    @db.create_bucket("rb-bucket")

    buckets = @db.list_buckets
    assert_includes buckets, "rb-bucket"

    @db.put_object("rb-bucket", "hello.txt", "Hello from Ruby!")

    data, meta = @db.get_object("rb-bucket", "hello.txt")
    assert_equal "Hello from Ruby!", data

    head = @db.head_object("rb-bucket", "hello.txt")
    assert head.key?("size")

    objs = @db.list_objects("rb-bucket")
    assert objs.length >= 1

    @db.delete_object("rb-bucket", "hello.txt")
  end

  def test_14_search
    results = @db.search("hello")
    assert_kind_of Array, results
  end

  def test_15_compact
    stats = @db.compact("rb_test")
    assert stats.key?("docs_kept")
  end

  def test_16_cleanup
    @db.drop_collection("rb_test")
    @db.drop_collection("rb_tx")
    @db.delete_bucket("rb-bucket")
  end
end
