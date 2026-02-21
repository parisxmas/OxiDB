"""
OxiDB Vector Search Integration Test
=====================================
Tests: create_vector_index, insert documents with embeddings, vector_search.
"""

import sys
import time
import math
import random

sys.path.insert(0, "../../python")
from oxidb import OxiDbClient, OxiDbError

COLLECTION = "test_vectors"
FIELD = "embedding"
DIM = 8
NUM_DOCS = 50

passed = 0
failed = 0
results = []


def report(name, ok, detail=""):
    global passed, failed
    status = "PASS" if ok else "FAIL"
    if ok:
        passed += 1
    else:
        failed += 1
    results.append((name, status, detail))
    mark = "\u2705" if ok else "\u274c"
    line = f"  {mark} {name}"
    if detail:
        line += f" -- {detail}"
    print(line)


def normalize(v):
    norm = math.sqrt(sum(x * x for x in v))
    return [x / norm for x in v] if norm > 0 else v


def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def rand_vec(dim):
    return [random.gauss(0, 1) for _ in range(dim)]


print("=" * 60)
print("OxiDB Vector Search Test")
print("=" * 60)

client = OxiDbClient("127.0.0.1", 4444)

# ---- Cleanup ----
try:
    client.drop_collection(COLLECTION)
except OxiDbError:
    pass

# ==================================================================
# TEST 1: Create vector index on empty collection
# ==================================================================
print("\n--- Test 1: Create vector index ---")
try:
    res = client.create_vector_index(COLLECTION, FIELD, DIM, metric="cosine")
    report("Create vector index (cosine, dim=8)", True, f"response={res}")
except Exception as e:
    report("Create vector index (cosine, dim=8)", False, str(e))

# ==================================================================
# TEST 2: List indexes to verify vector index appears
# ==================================================================
print("\n--- Test 2: Verify index in list_indexes ---")
try:
    indexes = client.list_indexes(COLLECTION)
    vec_idx = [i for i in indexes if i.get("index_type") == "vector"]
    ok = len(vec_idx) == 1
    detail = f"found {len(vec_idx)} vector index(es)"
    if ok:
        detail += f": name={vec_idx[0].get('name')}, dim={vec_idx[0].get('dimension')}, metric={vec_idx[0].get('metric')}"
    report("Vector index appears in list_indexes", ok, detail)
except Exception as e:
    report("Vector index appears in list_indexes", False, str(e))

# ==================================================================
# TEST 3: Insert documents with embeddings
# ==================================================================
print("\n--- Test 3: Insert documents with embeddings ---")
random.seed(42)

# Create a known cluster: 5 docs near a target point
target = normalize([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
cluster_docs = []
for i in range(5):
    # Small perturbation from target
    v = [target[j] + random.gauss(0, 0.05) for j in range(DIM)]
    v = normalize(v)
    cluster_docs.append({"title": f"cluster_{i}", "category": "near", "embedding": v})

# 45 random docs far from the target
random_docs = []
for i in range(NUM_DOCS - 5):
    v = normalize(rand_vec(DIM))
    random_docs.append({"title": f"random_{i}", "category": "far", "embedding": v})

all_docs = cluster_docs + random_docs
random.shuffle(all_docs)

try:
    res = client.insert_many(COLLECTION, all_docs)
    inserted = len(res) if isinstance(res, list) else len(res.get("ids", []))
    report(f"Insert {NUM_DOCS} documents with {DIM}-dim embeddings", inserted == NUM_DOCS,
           f"inserted={inserted}")
except Exception as e:
    report(f"Insert {NUM_DOCS} documents", False, str(e))

# ==================================================================
# TEST 4: Basic vector search
# ==================================================================
print("\n--- Test 4: Basic vector search ---")
try:
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=10)
    ok = len(results_search) == 10
    report("vector_search returns 10 results", ok, f"got {len(results_search)}")

    # Check that results have _similarity and _distance
    first = results_search[0] if results_search else {}
    has_sim = "_similarity" in first
    has_dist = "_distance" in first
    report("Results include _similarity and _distance fields", has_sim and has_dist,
           f"_similarity={'yes' if has_sim else 'no'}, _distance={'yes' if has_dist else 'no'}")

    # Check ordering: _similarity should be descending
    sims = [r["_similarity"] for r in results_search]
    ordered = all(sims[i] >= sims[i + 1] - 1e-9 for i in range(len(sims) - 1))
    report("Results sorted by similarity (descending)", ordered,
           f"similarities={[round(s, 4) for s in sims]}")

except Exception as e:
    report("Basic vector search", False, str(e))

# ==================================================================
# TEST 5: Cluster docs should be top results
# ==================================================================
print("\n--- Test 5: Cluster documents ranked highest ---")
try:
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=5)
    top_titles = [r["title"] for r in results_search]
    cluster_in_top = sum(1 for t in top_titles if t.startswith("cluster_"))
    ok = cluster_in_top >= 4  # at least 4 of 5 cluster docs in top 5
    report(f"Cluster docs in top-5 results: {cluster_in_top}/5", ok,
           f"top-5 titles: {top_titles}")

    # Check similarity values are high for cluster docs
    top_sim = results_search[0]["_similarity"]
    report(f"Top result similarity > 0.9", top_sim > 0.9,
           f"_similarity={top_sim:.4f}")

except Exception as e:
    report("Cluster documents ranked highest", False, str(e))

# ==================================================================
# TEST 6: Similarity scores match expected cosine similarity
# ==================================================================
print("\n--- Test 6: Similarity score accuracy ---")
try:
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=3)
    for r in results_search[:3]:
        emb = r["embedding"]
        expected_cos = cosine_similarity(target, emb)
        expected_sim = (1.0 + expected_cos) / 2.0  # cosine distance = 1-cos, sim = 1 - dist/2
        actual_sim = r["_similarity"]
        diff = abs(actual_sim - expected_sim)
        ok = diff < 0.01
        report(f"Similarity accuracy for '{r['title']}'", ok,
               f"expected={expected_sim:.4f}, actual={actual_sim:.4f}, diff={diff:.6f}")
except Exception as e:
    report("Similarity score accuracy", False, str(e))

# ==================================================================
# TEST 7: Search with limit=1
# ==================================================================
print("\n--- Test 7: Search with limit=1 ---")
try:
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=1)
    ok = len(results_search) == 1
    report("vector_search limit=1 returns exactly 1 result", ok, f"got {len(results_search)}")
except Exception as e:
    report("Search with limit=1", False, str(e))

# ==================================================================
# TEST 8: Search with opposite vector (should have low similarity)
# ==================================================================
print("\n--- Test 8: Opposite vector search ---")
try:
    opposite = [-x for x in target]
    results_search = client.vector_search(COLLECTION, FIELD, opposite, limit=5)
    top_sim = results_search[0]["_similarity"] if results_search else 0
    cluster_in_top = sum(1 for r in results_search if r["title"].startswith("cluster_"))
    report(f"Opposite vector: no cluster docs in top-5", cluster_in_top == 0,
           f"cluster_in_top={cluster_in_top}, top_similarity={top_sim:.4f}")
except Exception as e:
    report("Opposite vector search", False, str(e))

# ==================================================================
# TEST 9: Dimension mismatch error
# ==================================================================
print("\n--- Test 9: Dimension mismatch ---")
try:
    wrong_dim = [0.1] * 3  # 3-dim instead of 8
    client.vector_search(COLLECTION, FIELD, wrong_dim, limit=5)
    report("Dimension mismatch raises error", False, "no error raised")
except OxiDbError as e:
    report("Dimension mismatch raises error", True, f"error={e}")
except Exception as e:
    report("Dimension mismatch raises error", False, f"unexpected: {e}")

# ==================================================================
# TEST 10: Update a document and re-search
# ==================================================================
print("\n--- Test 10: Update document embedding ---")
try:
    # Find a cluster doc and move it far away
    cluster_doc = client.find_one(COLLECTION, {"title": "cluster_0"})
    far_vec = normalize([-1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    client.update(COLLECTION, {"title": "cluster_0"}, {"$set": {"embedding": far_vec}})

    # Now search again -- cluster_0 should NOT be in top 5
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=5)
    top_titles = [r["title"] for r in results_search]
    ok = "cluster_0" not in top_titles
    report("Updated doc excluded from top results", ok, f"top-5: {top_titles}")
except Exception as e:
    report("Update document embedding", False, str(e))

# ==================================================================
# TEST 11: Delete a document and re-search
# ==================================================================
print("\n--- Test 11: Delete document and search ---")
try:
    client.delete(COLLECTION, {"title": "cluster_1"})
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=5)
    top_titles = [r["title"] for r in results_search]
    ok = "cluster_1" not in top_titles
    report("Deleted doc absent from results", ok, f"top-5: {top_titles}")
except Exception as e:
    report("Delete document and search", False, str(e))

# ==================================================================
# TEST 12: Euclidean metric
# ==================================================================
print("\n--- Test 12: Euclidean metric ---")
COLLECTION_EUC = "test_vectors_euclidean"
try:
    try:
        client.drop_collection(COLLECTION_EUC)
    except OxiDbError:
        pass

    client.create_vector_index(COLLECTION_EUC, FIELD, DIM, metric="euclidean")
    client.insert_many(COLLECTION_EUC, [
        {"title": "near", "embedding": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
        {"title": "far", "embedding": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]},
    ])
    res = client.vector_search(COLLECTION_EUC, FIELD,
                               [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], limit=2)
    ok = res[0]["title"] == "near" and res[1]["title"] == "far"
    report("Euclidean metric: near doc ranked first", ok,
           f"order=[{res[0]['title']}, {res[1]['title']}], "
           f"sims=[{res[0]['_similarity']:.4f}, {res[1]['_similarity']:.4f}]")
    client.drop_collection(COLLECTION_EUC)
except Exception as e:
    report("Euclidean metric", False, str(e))

# ==================================================================
# TEST 13: Dot product metric
# ==================================================================
print("\n--- Test 13: Dot product metric ---")
COLLECTION_DOT = "test_vectors_dot"
try:
    try:
        client.drop_collection(COLLECTION_DOT)
    except OxiDbError:
        pass

    client.create_vector_index(COLLECTION_DOT, FIELD, DIM, metric="dot_product")
    client.insert_many(COLLECTION_DOT, [
        {"title": "aligned", "embedding": [1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
        {"title": "orthogonal", "embedding": [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]},
    ])
    res = client.vector_search(COLLECTION_DOT, FIELD,
                               [1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], limit=2)
    ok = res[0]["title"] == "aligned"
    report("Dot product metric: aligned doc ranked first", ok,
           f"order=[{res[0]['title']}, {res[1]['title']}], "
           f"sims=[{res[0]['_similarity']:.4f}, {res[1]['_similarity']:.4f}]")
    client.drop_collection(COLLECTION_DOT)
except Exception as e:
    report("Dot product metric", False, str(e))

# ==================================================================
# TEST 14: Compact preserves vector index
# ==================================================================
print("\n--- Test 14: Compact preserves vector index ---")
try:
    # Delete some docs to make compact meaningful
    client.delete(COLLECTION, {"category": "far"})
    compact_res = client.compact(COLLECTION)
    report("Compact succeeded", True, f"stats={compact_res}")

    # Search should still work after compact
    results_search = client.vector_search(COLLECTION, FIELD, target, limit=5)
    ok = len(results_search) > 0
    report("Vector search works after compact", ok, f"got {len(results_search)} results")
except Exception as e:
    report("Compact preserves vector index", False, str(e))

# ---- Cleanup ----
try:
    client.drop_collection(COLLECTION)
except OxiDbError:
    pass

client.close()

# ==================================================================
# REPORT
# ==================================================================
print("\n" + "=" * 60)
print(f"RESULTS: {passed} passed, {failed} failed, {passed + failed} total")
print("=" * 60)
for name, status, detail in results:
    mark = "\u2705" if status == "PASS" else "\u274c"
    line = f"  {mark} [{status}] {name}"
    print(line)

print()
if failed == 0:
    print("ALL TESTS PASSED")
else:
    print(f"{failed} TEST(S) FAILED")
    sys.exit(1)
