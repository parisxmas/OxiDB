"""
Large PDF Vector Search Stress Test
====================================
Tests OxiDB with 50+ large PDF files (5-40MB each, ~360MB total).
Uses server-side extract_text + Ollama nomic-embed-text embeddings.
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
BUCKET = "large_pdfs"
COLLECTION = "large_pdf_vectors"
FIELD = "embedding"
CHUNK_SIZE = 2000
PDF_DIR = "/tmp/test_pdfs_large"


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


def main():
    client = OxiDbClient(OXIDB_HOST, OXIDB_PORT)
    passed = 0
    failed = 0
    test_results = []

    def test(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            passed += 1
            test_results.append(("PASS", name, detail))
            print(f"  PASS  {name}" + (f" -- {detail}" if detail else ""))
        else:
            failed += 1
            test_results.append(("FAIL", name, detail))
            print(f"  FAIL  {name}" + (f" -- {detail}" if detail else ""))

    try:
        # ============================================================
        # PHASE 1: Setup
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 1: Setup")
        print("=" * 70)
        try:
            client.create_bucket(BUCKET)
        except OxiDbError:
            pass
        try:
            client.create_vector_index(COLLECTION, FIELD, EMBED_DIM, metric="cosine")
        except OxiDbError:
            pass

        # Clean old data
        try:
            client.delete(COLLECTION, {})
        except OxiDbError:
            pass

        # Filter to PDFs under 10MB (base64 encoding adds ~33%, must stay under 16MiB protocol limit)
        MAX_FILE_SIZE = 10 * 1024 * 1024
        all_pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])
        pdf_files = []
        skipped = []
        for f in all_pdfs:
            fsize = os.path.getsize(os.path.join(PDF_DIR, f))
            if fsize <= MAX_FILE_SIZE:
                pdf_files.append(f)
            else:
                skipped.append((f, fsize / 1048576))
        if skipped:
            print(f"  Skipped {len(skipped)} files over 10MB protocol limit:")
            for name, mb in skipped:
                print(f"    {name} ({mb:.1f} MB)")
        test("Found PDF files", len(pdf_files) >= 30, f"{len(pdf_files)} under 10MB, {len(skipped)} skipped")

        # ============================================================
        # PHASE 2: Upload all PDFs as blobs
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Upload blobs")
        print("=" * 70)
        t_upload_start = time.time()
        uploaded = 0
        total_bytes = 0
        for fname in pdf_files:
            path = os.path.join(PDF_DIR, fname)
            fsize = os.path.getsize(path)
            data = open(path, "rb").read()
            try:
                client.put_object(BUCKET, fname, data, content_type="application/pdf")
            except (ConnectionError, BrokenPipeError, OSError):
                # Reconnect and retry once
                client.close()
                client = OxiDbClient(OXIDB_HOST, OXIDB_PORT)
                client.put_object(BUCKET, fname, data, content_type="application/pdf")
            uploaded += 1
            total_bytes += fsize
            mb = fsize / 1048576
            print(f"  [{uploaded:2d}/{len(pdf_files)}] {fname:<45} {mb:6.1f} MB")

        t_upload = time.time() - t_upload_start
        total_mb = total_bytes / 1048576
        test("All PDFs uploaded", uploaded == len(pdf_files),
             f"{uploaded} files, {total_mb:.0f} MB in {t_upload:.1f}s")

        # ============================================================
        # PHASE 3: Extract text from all PDFs (server-side)
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Server-side text extraction")
        print("=" * 70)
        t_extract_start = time.time()
        extracted = {}
        extract_failures = []
        for i, fname in enumerate(pdf_files):
            try:
                text = client.extract_text(BUCKET, fname)
                extracted[fname] = text
                chars = len(text)
                print(f"  [{i+1:2d}/{len(pdf_files)}] {fname:<45} {chars:>8,} chars")
            except (ConnectionError, BrokenPipeError, OSError) as e:
                # Response too large for protocol, reconnect and skip
                extract_failures.append((fname, f"response too large: {e}"))
                print(f"  [{i+1:2d}/{len(pdf_files)}] {fname:<45} SKIPPED (response > 16MiB)")
                try:
                    client.close()
                except Exception:
                    pass
                client = OxiDbClient(OXIDB_HOST, OXIDB_PORT)
            except OxiDbError as e:
                extract_failures.append((fname, str(e)))
                print(f"  [{i+1:2d}/{len(pdf_files)}] {fname:<45} FAILED: {e}")

        t_extract = time.time() - t_extract_start
        total_chars = sum(len(t) for t in extracted.values())
        test("Text extraction", len(extracted) >= len(pdf_files) * 0.8,
             f"{len(extracted)}/{len(pdf_files)} succeeded, {len(extract_failures)} failed, "
             f"{total_chars:,} total chars in {t_extract:.1f}s")

        # ============================================================
        # PHASE 4: Chunk, embed, and store vectors
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Embedding and indexing")
        print("=" * 70)
        t_embed_start = time.time()
        total_chunks = 0
        for i, (fname, text) in enumerate(extracted.items()):
            chunks = chunk_text(text)
            for ci, chunk in enumerate(chunks):
                embedding = get_embedding(chunk)
                doc = {
                    "blob_key": fname,
                    "bucket": BUCKET,
                    "chunk_index": ci,
                    "total_chunks": len(chunks),
                    "text": chunk[:500],
                    "text_length": len(chunk),
                    "embedding": embedding,
                }
                client.insert(COLLECTION, doc)
                total_chunks += 1
            print(f"  [{i+1:2d}/{len(extracted)}] {fname:<45} {len(chunks):>4} chunks  (total: {total_chunks})")

        t_embed = time.time() - t_embed_start
        test("Embedding complete", total_chunks > 0,
             f"{total_chunks} chunks in {t_embed:.1f}s ({t_embed/max(total_chunks,1):.3f}s/chunk)")

        # ============================================================
        # PHASE 5: Semantic search tests
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 5: Semantic search")
        print("=" * 70)

        queries = [
            ("transformer attention mechanism self-attention", ["arxiv_attention_is_all_you_need.pdf"]),
            ("AES encryption block cipher symmetric key", ["nist_sp800_53r5.pdf", "nist_sp800_53ar5.pdf", "nist_fips_140_3.pdf"]),
            ("climate change global warming temperature rise", ["ipcc_ar6_spm.pdf", "ipcc_ar6_full.pdf", "ipcc_wg1_ch1.pdf", "ipcc_wg1_ch3.pdf", "ipcc_wg1_ch4.pdf", "ipcc_wg2_ch2.pdf", "ipcc_wg3_ch6.pdf"]),
            ("world economic growth GDP forecast", ["imf_weo_2024.pdf", "imf_weo_apr2024.pdf", "imf_weo_oct2023.pdf"]),
            ("deep learning convolutional neural network image classification", ["arxiv_resnet.pdf", "arxiv_yolo.pdf", "arxiv_vit.pdf"]),
            ("financial stability banking risk systemic", ["fed_financial_stability_2024.pdf", "fed_svb_review.pdf", "imf_gfsr_2024.pdf", "imf_gfsr_apr2024.pdf"]),
            ("large language model GPT pretraining", ["arxiv_gpt2.pdf", "arxiv_llama2.pdf", "arxiv_llm_survey.pdf", "arxiv_bert.pdf"]),
            ("object detection bounding box real-time", ["arxiv_yolo.pdf", "arxiv_sam.pdf"]),
            ("cybersecurity access control vulnerability assessment", ["nist_cybersecurity_framework.pdf", "nist_sp800_53r5.pdf", "nist_sp800_82r3.pdf", "nist_sp800_115.pdf"]),
            ("generative adversarial network image synthesis", ["arxiv_gan.pdf", "arxiv_stable_diffusion.pdf", "arxiv_diffusion.pdf"]),
            ("sustainable development poverty reduction", ["un_sdg_2024.pdf"]),
            ("BERT masked language model pretraining NLP", ["arxiv_bert.pdf", "arxiv_nlp_pretrain_survey.pdf"]),
            ("silicon valley bank failure collapse", ["fed_svb_review.pdf"]),
            ("fiscal deficit government spending budget", ["imf_fiscal_monitor_2024.pdf", "imf_fiscal_apr2024.pdf"]),
            ("reinforcement learning policy gradient reward", ["arxiv_rl_survey.pdf"]),
        ]

        top1_hits = 0
        top3_hits = 0
        for query, acceptable in queries:
            t0 = time.time()
            qvec = get_embedding(query)
            hits = client.vector_search(COLLECTION, FIELD, qvec, limit=5)
            search_ms = (time.time() - t0) * 1000

            top_file = hits[0].get("blob_key", "?") if hits else "?"
            top_score = hits[0].get("_similarity", 0) if hits else 0
            top3_files = [h.get("blob_key", "?") for h in hits[:3]]

            in_top1 = top_file in acceptable
            in_top3 = any(f in acceptable for f in top3_files)
            if in_top1:
                top1_hits += 1
            if in_top3:
                top3_hits += 1

            rank = "TOP-1" if in_top1 else ("TOP-3" if in_top3 else "MISS")
            print(f"  [{rank:5}] \"{query[:55]}...\"")
            print(f"          -> {top_file} (score={top_score:.4f}, {search_ms:.0f}ms)")

        test(f"Top-1 accuracy", top1_hits >= len(queries) * 0.4,
             f"{top1_hits}/{len(queries)} ({100*top1_hits/len(queries):.0f}%)")
        test(f"Top-3 accuracy", top3_hits >= len(queries) * 0.5,
             f"{top3_hits}/{len(queries)} ({100*top3_hits/len(queries):.0f}%)")

        # ============================================================
        # PHASE 6: Performance benchmarks
        # ============================================================
        print("\n" + "=" * 70)
        print("PHASE 6: Performance")
        print("=" * 70)

        # Search latency benchmark
        qvec = get_embedding("test query for benchmarking search latency")
        latencies = []
        for _ in range(10):
            t0 = time.time()
            client.vector_search(COLLECTION, FIELD, qvec, limit=10)
            latencies.append((time.time() - t0) * 1000)

        avg_lat = sum(latencies) / len(latencies)
        p50 = sorted(latencies)[len(latencies)//2]
        p99 = sorted(latencies)[-1]
        test(f"Search latency (10 runs)", avg_lat < 5000,
             f"avg={avg_lat:.0f}ms, p50={p50:.0f}ms, p99={p99:.0f}ms")

        # Check data size
        doc_count = len(client.find(COLLECTION, {}))
        test("Document count", doc_count == total_chunks,
             f"{doc_count} docs = {total_chunks} chunks")

    finally:
        client.close()

    # ============================================================
    # FINAL REPORT
    # ============================================================
    print("\n" + "=" * 70)
    print("FINAL REPORT")
    print("=" * 70)
    print(f"  PDFs:            {uploaded}/{len(pdf_files)}")
    print(f"  Total blob size: {total_mb:.0f} MB")
    print(f"  Upload time:     {t_upload:.1f}s")
    print(f"  Extracted:       {len(extracted)}/{len(pdf_files)} PDFs -> {total_chars:,} chars")
    print(f"  Extract time:    {t_extract:.1f}s")
    print(f"  Chunks:          {total_chunks}")
    print(f"  Embed time:      {t_embed:.1f}s ({t_embed/max(total_chunks,1):.3f}s/chunk)")
    print(f"  Search accuracy: top1={top1_hits}/{len(queries)}, top3={top3_hits}/{len(queries)}")
    print(f"  Search latency:  avg={avg_lat:.0f}ms, p50={p50:.0f}ms, p99={p99:.0f}ms")
    print(f"  Tests:           {passed} passed, {failed} failed")
    print("=" * 70)

    if failed > 0:
        print("\nFailed tests:")
        for status, name, detail in test_results:
            if status == "FAIL":
                print(f"  - {name}: {detail}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
