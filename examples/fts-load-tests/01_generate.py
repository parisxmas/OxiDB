#!/usr/bin/env python3
"""
Fetch three public-domain books from Project Gutenberg, split each into
chunks, and emit 100 minimal-but-valid .docx files into ./data.

Each .docx contains exactly one file: word/document.xml. That is enough
for OxiDB's extract_docx() because it only reads that path from the ZIP
(see src/fts.rs:484-498). No python-docx dependency required.
"""
import io
import os
import re
import sys
import urllib.request
import xml.sax.saxutils as sax
import zipfile
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

BOOKS = [
    ("alice", "https://www.gutenberg.org/cache/epub/11/pg11.txt"),
    ("pride", "https://www.gutenberg.org/cache/epub/1342/pg1342.txt"),
    ("sherlock", "https://www.gutenberg.org/cache/epub/1661/pg1661.txt"),
]

CHUNKS_PER_BOOK = [34, 33, 33]  # totals 100


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "ftstests/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace")


def strip_gutenberg_header_footer(text: str) -> str:
    """Drop the Project Gutenberg license/preamble before *** START *** and
    after *** END ***."""
    start = re.search(r"\*\*\*\s*START OF.*?\*\*\*", text, re.IGNORECASE | re.DOTALL)
    end = re.search(r"\*\*\*\s*END OF.*?\*\*\*", text, re.IGNORECASE | re.DOTALL)
    if start:
        text = text[start.end():]
    if end:
        text = text[: end.start()]
    return text.strip()


def split_into_chunks(body: str, n_chunks: int) -> list[str]:
    """Split body into roughly equal n_chunks pieces along paragraph
    boundaries so each piece reads as a coherent passage."""
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", body) if p.strip()]
    if len(paragraphs) < n_chunks:
        # Pad: split largest paragraphs into halves until we have enough.
        while len(paragraphs) < n_chunks:
            paragraphs.sort(key=len, reverse=True)
            head = paragraphs.pop(0)
            mid = len(head) // 2
            paragraphs.append(head[:mid].strip())
            paragraphs.append(head[mid:].strip())
    per_chunk = max(1, len(paragraphs) // n_chunks)
    chunks: list[str] = []
    for i in range(n_chunks):
        start = i * per_chunk
        end = (i + 1) * per_chunk if i < n_chunks - 1 else len(paragraphs)
        chunk = "\n\n".join(paragraphs[start:end])
        chunks.append(chunk)
    return chunks


DOCX_TEMPLATE_HEAD = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
    "<w:body>"
)
DOCX_TEMPLATE_FOOT = "</w:body></w:document>"


def make_docx_xml(text: str) -> str:
    paragraphs = [p for p in text.split("\n\n") if p.strip()]
    body = "".join(
        f"<w:p><w:r><w:t xml:space=\"preserve\">{sax.escape(p)}</w:t></w:r></w:p>"
        for p in paragraphs
    )
    return DOCX_TEMPLATE_HEAD + body + DOCX_TEMPLATE_FOOT


def write_docx(path: Path, xml_text: str) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("word/document.xml", xml_text)
    path.write_bytes(buf.getvalue())


def main() -> int:
    total = 0
    for (name, url), n in zip(BOOKS, CHUNKS_PER_BOOK):
        print(f"[fetch] {name}: {url}", flush=True)
        try:
            raw = fetch(url)
        except Exception as e:
            print(f"  failed: {e}", file=sys.stderr)
            return 1
        body = strip_gutenberg_header_footer(raw)
        chunks = split_into_chunks(body, n)
        for i, chunk in enumerate(chunks, 1):
            fname = DATA_DIR / f"{name}_{i:03d}.docx"
            write_docx(fname, make_docx_xml(chunk))
            total += 1
        print(f"  wrote {n} chunks ({sum(len(c) for c in chunks):,} chars total)")
    print(f"[done] {total} .docx files in {DATA_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
