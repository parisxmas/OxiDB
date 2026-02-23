"""
RAG (Retrieval-Augmented Generation) with OxiDB + Ollama
=========================================================
Full RAG pipeline: store files as blobs, extract text (PDF, DOCX, HTML...),
generate embeddings, semantic vector search, and LLM-powered answers.

Requirements:
    pip install requests
    ollama pull nomic-embed-text
    ollama pull gemma3:27b          # or any chat model

Usage:
    # Index files
    python semantic_blob_search.py index report.pdf slides.pptx notes.txt

    # Search (vector similarity only)
    python semantic_blob_search.py search "quarterly revenue projections"

    # Ask a question (RAG: retrieve + generate answer)
    python semantic_blob_search.py ask "What are the key findings about climate change?"

    # Interactive RAG chat
    python semantic_blob_search.py chat

    # List indexed documents
    python semantic_blob_search.py list

    # Interactive search (no generation)
    python semantic_blob_search.py interactive
"""

import sys
import os
import json
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

# --- Config ---
OXIDB_HOST = "127.0.0.1"
OXIDB_PORT = 4444
OLLAMA_BASE = "http://localhost:11434"
OLLAMA_URL = OLLAMA_BASE + "/api/embeddings"
EMBED_MODEL = "nomic-embed-text"
CHAT_MODEL = "gemma3:27b"
EMBED_DIM = 768
BUCKET = "documents"
COLLECTION = "doc_embeddings"
FIELD = "embedding"
CHUNK_SIZE = 2000  # characters per chunk
RAG_TOP_K = 10     # number of chunks to retrieve for RAG


def get_embedding(text):
    """Get embedding vector from Ollama."""
    resp = requests.post(OLLAMA_URL, json={
        "model": EMBED_MODEL,
        "prompt": text,
    })
    resp.raise_for_status()
    return resp.json()["embedding"]


def chunk_text(text, size=CHUNK_SIZE):
    """Split text into chunks for embedding. Short texts stay as one chunk."""
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
    """Guess content type from file extension."""
    ext = os.path.splitext(path)[1].lower()
    types = {
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".html": "text/html",
        ".htm": "text/html",
        ".csv": "text/csv",
        ".json": "application/json",
        ".xml": "text/xml",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }
    return types.get(ext, "application/octet-stream")


def cmd_index(client, files):
    """Index files: upload as blobs, extract text, embed, store vectors."""
    # Ensure bucket exists
    try:
        client.create_bucket(BUCKET)
    except OxiDbError:
        pass  # already exists

    # Ensure vector index exists
    try:
        client.create_vector_index(COLLECTION, FIELD, EMBED_DIM, metric="cosine")
        print(f"Created vector index on '{COLLECTION}.{FIELD}' ({EMBED_DIM}-dim, cosine)")
    except OxiDbError as e:
        if "already exists" in str(e).lower():
            pass
        else:
            print(f"Vector index: {e}")

    for path in files:
        if not os.path.isfile(path):
            print(f"  Skipping {path} (not found)")
            continue

        filename = os.path.basename(path)
        content_type = guess_content_type(path)
        file_size = os.path.getsize(path)
        print(f"\n--- Indexing: {filename} ({file_size:,} bytes, {content_type}) ---")

        # 1. Upload blob
        data = open(path, "rb").read()
        client.put_object(BUCKET, filename, data, content_type=content_type)
        print(f"  Uploaded to bucket '{BUCKET}'")

        # 2. Extract text using OxiDB's built-in extractor (handles PDF, DOCX, HTML, etc.)
        try:
            text = client.extract_text(BUCKET, filename)
        except OxiDbError as e:
            print(f"  Could not extract text: {e}, skipping")
            continue

        if not text.strip():
            print(f"  Empty content, skipping")
            continue

        # 3. Chunk and embed
        chunks = chunk_text(text)
        print(f"  Extracted {len(text):,} chars -> {len(chunks)} chunk(s)")

        # Remove old chunks for this file
        try:
            client.delete(COLLECTION, {"blob_key": filename})
        except OxiDbError:
            pass

        for i, chunk in enumerate(chunks):
            print(f"  Embedding chunk {i + 1}/{len(chunks)}...", end=" ", flush=True)
            embedding = get_embedding(chunk)
            doc = {
                "blob_key": filename,
                "bucket": BUCKET,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "text": chunk,  # full chunk text for RAG context
                "text_length": len(chunk),
                "content_type": content_type,
                "file_size": file_size,
                "embedding": embedding,
            }
            client.insert(COLLECTION, doc)
            print("done")

    print(f"\nIndexing complete.")


def cmd_search(client, query, limit=5):
    """Search indexed documents by semantic similarity."""
    print(f"Query: \"{query}\"")
    print(f"Embedding query...", end=" ", flush=True)
    query_vec = get_embedding(query)
    print("done")

    results = client.vector_search(COLLECTION, FIELD, query_vec, limit=limit)

    if not results:
        print("No results found.")
        return

    print(f"\n{'#':<4} {'Score':<8} {'File':<30} {'Chunk':<8} Preview")
    print("-" * 90)

    for i, doc in enumerate(results):
        sim = doc.get("_similarity", 0)
        key = doc.get("blob_key", "?")
        chunk = f"{doc.get('chunk_index', 0) + 1}/{doc.get('total_chunks', 1)}"
        preview = doc.get("text", "")[:60].replace("\n", " ")
        print(f"{i + 1:<4} {sim:<8.4f} {key:<30} {chunk:<8} {preview}...")


def cmd_list(client):
    """List all indexed documents."""
    try:
        docs = client.find(COLLECTION, {})
    except OxiDbError:
        print("No documents indexed yet.")
        return

    if not docs:
        print("No documents indexed yet.")
        return

    # Group by blob_key
    files = {}
    for doc in docs:
        key = doc.get("blob_key", "?")
        if key not in files:
            files[key] = {
                "chunks": 0,
                "total_chars": 0,
                "content_type": doc.get("content_type", "?"),
                "file_size": doc.get("file_size", 0),
            }
        files[key]["chunks"] += 1
        files[key]["total_chars"] += doc.get("text_length", 0)

    print(f"\n{'File':<30} {'Type':<20} {'Size':<12} {'Chunks':<8} {'Chars'}")
    print("-" * 80)
    for key, info in sorted(files.items()):
        size = f"{info['file_size']:,}" if info['file_size'] else "?"
        print(f"{key:<30} {info['content_type']:<20} {size:<12} {info['chunks']:<8} {info['total_chars']:,}")

    print(f"\nTotal: {len(files)} file(s), {sum(f['chunks'] for f in files.values())} chunk(s)")


def retrieve_context(client, query, top_k=RAG_TOP_K):
    """Retrieve relevant chunks for a query. Returns (context_str, sources).

    Fetches top_k chunks via vector search, groups by file, and for each
    top file also fetches neighboring chunks to provide richer context.
    """
    query_vec = get_embedding(query)
    results = client.vector_search(COLLECTION, FIELD, query_vec, limit=top_k)
    if not results:
        return "", []

    # Group hits by file, keeping best score per file
    file_hits = {}
    for doc in results:
        key = doc.get("blob_key", "?")
        if key not in file_hits:
            file_hits[key] = {"doc": doc, "score": doc.get("_similarity", 0)}

    chunks = []
    sources = []
    for key, info in sorted(file_hits.items(), key=lambda x: -x[1]["score"]):
        doc = info["doc"]
        sim = info["score"]
        chunk_idx = doc.get("chunk_index", 0)
        total = doc.get("total_chunks", 1)

        # Fetch the matched chunk plus its neighbors for more context
        neighbor_indices = [i for i in range(max(0, chunk_idx - 1), min(total, chunk_idx + 2))]
        neighbor_docs = []
        for ni in neighbor_indices:
            try:
                found = client.find(COLLECTION, {"blob_key": key, "chunk_index": ni})
                if found:
                    neighbor_docs.append(found[0])
            except OxiDbError:
                pass

        # Build context from neighbors (sorted by chunk_index)
        neighbor_docs.sort(key=lambda d: d.get("chunk_index", 0))
        file_text = "\n".join(d.get("text", "") for d in neighbor_docs)
        if file_text.strip():
            chunks.append(f"[Source: {key}]\n{file_text}")

        chunk_info = f"{chunk_idx + 1}/{total}"
        sources.append({"file": key, "chunk": chunk_info, "score": sim})

        # Limit to top 5 files to keep context manageable
        if len(sources) >= 5:
            break

    context = "\n\n---\n\n".join(chunks)
    return context, sources


def generate_answer(question, context, stream=True):
    """Generate an answer using Ollama chat with retrieved context."""
    prompt = f"""You are a helpful assistant. Answer the question based ONLY on the provided context.
If the context doesn't contain enough information to answer, say so.
Be concise and cite which document(s) your answer comes from.

Context:
{context}

Question: {question}

Answer:"""

    resp = requests.post(
        OLLAMA_BASE + "/api/generate",
        json={"model": CHAT_MODEL, "prompt": prompt, "stream": stream},
        stream=stream,
    )
    resp.raise_for_status()

    if stream:
        full_response = []
        for line in resp.iter_lines():
            if line:
                data = json.loads(line)
                token = data.get("response", "")
                full_response.append(token)
                print(token, end="", flush=True)
                if data.get("done", False):
                    break
        print()
        return "".join(full_response)
    else:
        result = resp.json()
        answer = result.get("response", "")
        print(answer)
        return answer


def cmd_ask(client, question, top_k=RAG_TOP_K):
    """RAG: retrieve relevant chunks and generate an answer."""
    print(f"Question: \"{question}\"\n")

    # R - Retrieve
    print("Retrieving relevant context...", end=" ", flush=True)
    context, sources = retrieve_context(client, question, top_k)
    print(f"found {len(sources)} source(s)\n")

    if not context:
        print("No relevant documents found. Index some files first.")
        return

    # Show sources
    print("Sources:")
    for s in sources:
        print(f"  [{s['score']:.4f}] {s['file']} (chunk {s['chunk']})")
    print()

    # G - Generate
    print(f"Answer ({CHAT_MODEL}):")
    print("-" * 50)
    generate_answer(question, context, stream=True)
    print("-" * 50)


def cmd_chat(client):
    """Interactive RAG chat mode."""
    print(f"RAG Chat (model: {CHAT_MODEL}, type 'quit' to exit)\n")
    while True:
        try:
            question = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not question or question.lower() in ("quit", "exit", "q"):
            break
        print()
        cmd_ask(client, question)
        print()


def cmd_interactive(client):
    """Interactive search mode (no generation)."""
    print("Semantic Blob Search (type 'quit' to exit)\n")
    while True:
        try:
            query = input("search> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not query or query.lower() in ("quit", "exit", "q"):
            break
        cmd_search(client, query)
        print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    client = OxiDbClient(OXIDB_HOST, OXIDB_PORT)

    try:
        if command == "index":
            if len(sys.argv) < 3:
                print("Usage: python semantic_blob_search.py index <file1> [file2] ...")
                sys.exit(1)
            cmd_index(client, sys.argv[2:])

        elif command == "search":
            if len(sys.argv) < 3:
                print("Usage: python semantic_blob_search.py search \"your query\"")
                sys.exit(1)
            query = " ".join(sys.argv[2:])
            limit = 5
            cmd_search(client, query, limit)

        elif command == "list":
            cmd_list(client)

        elif command == "ask":
            if len(sys.argv) < 3:
                print("Usage: python semantic_blob_search.py ask \"your question\"")
                sys.exit(1)
            question = " ".join(sys.argv[2:])
            cmd_ask(client, question)

        elif command == "chat":
            cmd_chat(client)

        elif command == "interactive":
            cmd_interactive(client)

        else:
            print(f"Unknown command: {command}")
            print("Commands: index, search, ask, chat, list, interactive")
            sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
