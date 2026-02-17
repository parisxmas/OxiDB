# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name        = "oxidb"
  s.version     = "0.1.0"
  s.summary     = "OxiDB client for Ruby"
  s.description = "Zero-dependency TCP client for oxidb-server. Supports CRUD, transactions, blob storage, full-text search, and aggregation."
  s.authors     = ["OxiDB Contributors"]
  s.license     = "MIT"
  s.files       = ["lib/oxidb.rb"]
  s.required_ruby_version = ">= 3.0"
end
