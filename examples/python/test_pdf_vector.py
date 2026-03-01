"""
PDF Vector Search Test
======================
Tests OxiDB's built-in PDF text extraction + vector search with 20 real PDF files.
Uses OxiDB extract_text command (server-side PDF parsing) + Ollama embeddings.
"""

import sys
import os
import time
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

# --- Config ---
OXIDB_HOST = "127.0.0.1"
OXIDB_PORT = 4444
OLLAMA_URL = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "nomic-embed-text"
EMBED_DIM = 768
BUCKET = "pdf_test"
COLLECTION = "pdf_vectors"
FIELD = "embedding"
CHUNK_SIZE = 2000
PDF_DIR = "/tmp/test_pdfs"

# Expected topics for search relevance testing
PDF_TOPICS = {
    "rfc768_udp.pdf": "UDP protocol datagram networking",
    "rfc791_ip.pdf": "IP internet protocol addressing routing",
    "rfc793_tcp.pdf": "TCP transmission control protocol connections",
    "rfc2119_keywords.pdf": "RFC requirement keywords MUST SHALL",
    "rfc4180_csv.pdf": "CSV comma separated values format",
    "rfc7519_jwt.pdf": "JSON web token JWT authentication claims",
    "rfc6749_oauth2.pdf": "OAuth 2.0 authorization framework tokens",
    "rfc8259_json.pdf": "JSON data interchange format parsing",
    "nist_aes.pdf": "AES encryption cipher block symmetric",
    "nist_sha.pdf": "SHA secure hash algorithm digest",
    "nist_pbkdf.pdf": "password key derivation PBKDF",
    "irs_1040.pdf": "income tax return individual filing",
    "irs_w9.pdf": "taxpayer identification number form",
    "irs_w4.pdf": "employee withholding certificate payroll",
    "treasury_exec_summary.pdf": "US government financial report budget",
    "treasury_comptroller.pdf": "comptroller general government audit",
    "fda_guidance.pdf": "FDA food drug administration regulation",
    "gutenberg_ebooks_history.pdf": "ebook digital publishing history",
    "dickens_christmas.pdf": "Christmas stories literature fiction Dickens",
    "gutenberg_project_history.pdf": "Project Gutenberg digital library",
}


def get_embedding(text):
    resp = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text})
    resp.raise_for_status()
    return resp.json()["embedding"]


def chunk_text(text, size=CHUNK_SIZE):
    text = text.strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]
    chunks = []
    for i in range(0, len(text), size):
        chunk = text[i:i + size].strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def guess_content_type(path):
    ext = os.path.splitext(path)[1].lower()
    return {".pdf": "application/pdf", ".txt": "text/plain"}.get(ext, "application/octet-stream")


def main():
    client = OxiDbClient(OXIDB_HOST, OXIDB_PORT)
    passed = 0
    failed = 0
    results = []

    def test(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            passed += 1
            results.append(("PASS", name, detail))
            print(f"  PASS  {name}" + (f" -- {detail}" if detail else ""))
        else:
            failed += 1
            results.append(("FAIL", name, detail))
            print(f"  FAIL  {name}" + (f" -- {detail}" if detail else ""))

    try:
        # ============================================================
        # TEST 1: Setup - create bucket and vector index
        # ============================================================
        print("\n[1/7] Setup")
        try:
            client.create_bucket(BUCKET)
        except OxiDbError:
            pass
        try:
            client.create_vector_index(COLLECTION, FIELD, EMBED_DIM, metric="cosine")
        except OxiDbError:
            pass
        test("Bucket and vector index ready", True)

        # ============================================================
        # TEST 2: Upload all 20 PDFs as blobs
        # ============================================================
        print("\n[2/7] Upload PDFs as blobs")
        pdf_files = sorted([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])
        test("Found 20 PDF files", len(pdf_files) == 20, f"found {len(pdf_files)}")

        uploaded = 0
        for fname in pdf_files:
            path = os.path.join(PDF_DIR, fname)
            data = open(path, "rb").read()
            ct = guess_content_type(path)
            client.put_object(BUCKET, fname, data, content_type=ct)
            uploaded += 1
        test("All PDFs uploaded to blob store", uploaded == 20, f"{uploaded} uploaded")

        # ============================================================
        # TEST 3: Extract text from all PDFs using OxiDB extract_text
        # ============================================================
        print("\n[3/7] Extract text (server-side PDF parsing)")
        extracted = {}
        extract_failures = []
        for fname in pdf_files:
            try:
                text = client.extract_text(BUCKET, fname)
                extracted[fname] = text
                chars = len(text)
                print(f"    {fname:<40} {chars:>6,} chars")
            except OxiDbError as e:
                extract_failures.append((fname, str(e)))
                print(f"    {fname:<40} FAILED: {e}")

        test("Text extracted from PDFs", len(extracted) >= 15,
             f"{len(extracted)}/20 succeeded, {len(extract_failures)} failed")

        for fname, err in extract_failures:
            test(f"Extract {fname}", False, err)

        # ============================================================
        # TEST 4: Chunk and embed all extracted text, store in vector index
        # ============================================================
        print("\n[4/7] Embed and index (this may take a minute...)") 
        # Clean old data
        try:
            client.delete(COLLECTION, {})
        except OxiDbError:
            pass

        total_chunks = 0
        t0 = time.time()
        for fname, text in extracted.items():
            chunks = chunk_text(text)
            for i, chunk in enumerate(chunks):
                embedding = get_embedding(chunk)
                doc = {
                    "blob_key": fname,
                    "bucket": BUCKET,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "text": chunk[:500],
                    "text_length": len(chunk),
                    "embedding": embedding,
                }
                client.insert(COLLECTION, doc)
                total_chunks += 1
            print(f"    {fname:<40} {len(chunks)} chunk(s)")

        embed_time = time.time() - t0
        test("All chunks embedded and stored", total_chunks > 0,
             f"{total_chunks} chunks in {embed_time:.1f}s")

        # ============================================================
        # TEST 5: Semantic search queries
        # ============================================================
        print("\n[5/7] Semantic search queries")

        queries = [
            ("TCP connection handshake protocol", "rfc793_tcp.pdf"),
            ("AES encryption symmetric cipher", "nist_aes.pdf"),
            ("JSON data format parsing", "rfc8259_json.pdf"),
            ("OAuth authorization access token", "rfc6749_oauth2.pdf"),
            ("income tax return filing", "irs_1040.pdf"),
            ("secure hash algorithm SHA digest", "nist_sha.pdf"),
            ("JWT authentication claims signing", "rfc7519_jwt.pdf"),
            ("CSV comma separated file format", "rfc4180_csv.pdf"),
            ("employee withholding payroll tax", "irs_w4.pdf"),
            ("digital ebook publishing history", "gutenberg_ebooks_history.pdf"),
        ]

        search_results = []
        for query, expected_file in queries:
            qvec = get_embedding(query)
            hits = client.vector_search(COLLECTION, FIELD, qvec, limit=5)
            top_file = hits[0].get("blob_key", "?") if hits else "?"
            top_score = hits[0].get("_similarity", 0) if hits else 0
            top3_files = [h.get("blob_key", "?") for h in hits[:3]]
            in_top1 = top_file == expected_file
            in_top3 = expected_file in top3_files
            search_results.append((query, expected_file, top_file, top_score, in_top1, in_top3))

            rank = "TOP-1" if in_top1 else ("TOP-3" if in_top3 else "MISS")
            print(f"    [{rank}] \"{query}\"")
            print(f"           expected={expected_file}  got={top_file} (score={top_score:.4f})")

        top1_hits = sum(1 for r in search_results if r[4])
        top3_hits = sum(1 for r in search_results if r[5])
        test(f"Top-1 accuracy", top1_hits >= 5,
             f"{top1_hits}/{len(queries)} queries hit expected file at rank 1")
        test(f"Top-3 accuracy", top3_hits >= 5,
             f"{top3_hits}/{len(queries)} queries hit expected file in top 3")

        # ============================================================
        # TEST 6: Cross-domain search (should NOT match wrong topics)
        # ============================================================
        print("\n[6/7] Cross-domain relevance")

        cross_queries = [
            ("Christmas stories and literature", ["dickens_christmas.pdf", "gutenberg_ebooks_history.pdf"]),
            ("network protocol IP addressing", ["rfc791_ip.pdf", "rfc768_udp.pdf", "rfc793_tcp.pdf"]),
            ("password derivation cryptography", ["nist_pbkdf.pdf", "nist_aes.pdf", "nist_sha.pdf"]),
        ]

        for query, acceptable_files in cross_queries:
            qvec = get_embedding(query)
            hits = client.vector_search(COLLECTION, FIELD, qvec, limit=3)
            top_file = hits[0].get("blob_key", "?") if hits else "?"
            top_score = hits[0].get("_similarity", 0) if hits else 0
            is_relevant = top_file in acceptable_files
            test(f"Cross-domain: \"{query[:40]}...\"", is_relevant,
                 f"top={top_file} (score={top_score:.4f})")

        # ============================================================
        # TEST 7: Performance and stats
        # ============================================================
        print("\n[7/7] Stats")
        docs = client.find(COLLECTION, {})
        doc_count = len(docs) if isinstance(docs, list) else 0
        test("Document count matches chunks", doc_count == total_chunks,
             f"{doc_count} docs, {total_chunks} chunks")

        total_chars = sum(len(t) for t in extracted.values())
        print(f"\n    PDFs extracted:    {len(extracted)}")
        print(f"    Total text chars:  {total_chars:,}")
        print(f"    Total chunks:      {total_chunks}")
        print(f"    Embedding time:    {embed_time:.1f}s")
        print(f"    Avg chunk time:    {embed_time / max(total_chunks, 1):.2f}s")

    finally:
        client.close()

    # ============================================================
    # REPORT
    # ============================================================
    print("\n" + "=" * 70)
    print(f"  RESULTS: {passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 70)

    if failed > 0:
        print("\nFailed tests:")
        for status, name, detail in results:
            if status == "FAIL":
                print(f"  - {name}: {detail}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
