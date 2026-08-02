#!/usr/bin/env julia
# 06_encryption_at_rest.jl — pass an encryption key to open_db and the engine
# transparently AES-encrypts everything on disk. The same key is required to
# reopen the database.

using OxiDbEmbedded

path    = mktempdir()
keyfile = tempname()
write(keyfile, rand(UInt8, 32))            # a 32-byte AES-256 key

# Write some data through the encrypted handle.
let db = open_db(path; encryption_key_path = keyfile)
    insert(db, "secrets", Dict("owner" => "alice", "api_key" => "sk-live-SENSITIVE"))
    close(db)
end
println("wrote an encrypted database at $path")

# Reopening with the same key works.
let db = open_db(path; encryption_key_path = keyfile)
    println("reopened with the key -> ", find_one(db, "secrets", Dict("owner" => "alice")))
    close(db)
end

# The bytes on disk are ciphertext — the plaintext secret never appears in
# any of the database's files.
function any_file_contains(dir, needle)
    for (root, _, files) in walkdir(dir), f in files
        occursin(needle, String(read(joinpath(root, f)))) && return true
    end
    return false
end
println("plaintext 'sk-live-SENSITIVE' present in the on-disk files? ",
        any_file_contains(path, "sk-live-SENSITIVE"))
