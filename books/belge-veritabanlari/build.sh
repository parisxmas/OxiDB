#!/usr/bin/env bash
#
# Kitabı tek bir dosyada derler. Bölüm dosyaları ad sırasına göre (00, 01, 02…)
# birleştirilir, bu yüzden yeni bölüm eklerken numara önekine dikkat edin.
#
# Gereksinim: pandoc + bir LaTeX dağıtımı (xelatex). macOS: `brew install pandoc
# basictex` (sonra `sudo tlmgr install xetex`), ya da `brew install pandoc
# mactex`. Linux: `apt install pandoc texlive-xetex texlive-fonts-recommended`.
#
# LaTeX yoksa, LaTeX gerektirmeyen Chrome tabanlı yol kullanılabilir:
#   ./build.sh pdf-html   # pandoc -> HTML (print.css + kapak) -> Chrome -> PDF
# Bu yol yalnızca pandoc + Google Chrome ister (xelatex gerekmez).
#
# Kullanım:
#   ./build.sh            # belge-veritabanlari.pdf üretir (xelatex gerektirir)
#   ./build.sh pdf-html   # belge-veritabanlari.pdf üretir (Chrome ile, LaTeX'siz)
#   ./build.sh epub       # belge-veritabanlari.epub üretir
#   ./build.sh html       # tek dosyalık belge-veritabanlari.html üretir
set -euo pipefail
cd "$(dirname "$0")"

fmt="${1:-pdf}"
chapters=( $(ls [0-9][0-9]-*.md | sort) )
out="belge-veritabanlari"

case "$fmt" in
  pdf)
    # Kapak: kapak.png varsa ilk sayfaya tam-sayfa görsel olarak eklenir.
    cover_opt=()
    if [[ -f kapak.png ]]; then
      # Metin başlık sayfasını iptal et + kapak görselini ilk sayfa olarak ekle.
      cover_opt=(--include-in-header=kapak-header.tex --include-before-body=kapak.tex)
    else
      echo "uyarı: kapak.png bulunamadı; kapaksız üretiliyor." >&2
    fi
    # Yazı tipleri: sistemde kurulu, Türkçe gliflerini içeren tipler. macOS'ta
    # Georgia + Menlo her zaman vardır ve kitabın HTML tasarımıyla eşleşir.
    # Linux'ta BOOK_MAINFONT/BOOK_MONOFONT ile (örn. "DejaVu Serif") değiştirin.
    # SVG şekiller için `rsvg-convert` (librsvg) gerekir; xelatex SVG basamaz.
    mainfont="${BOOK_MAINFONT:-Georgia}"; monofont="${BOOK_MONOFONT:-Menlo}"
    pandoc metadata.yaml "${chapters[@]}" \
      --toc --number-sections --top-level-division=chapter \
      --pdf-engine=xelatex \
      --include-in-header=unicode-duzelt.tex \
      "${cover_opt[@]}" \
      -V classoption=oneside \
      -V mainfont="$mainfont" \
      -V monofont="$monofont" \
      -o "${out}.pdf"
    echo "yazıldı: ${out}.pdf"
    ;;
  epub)
    # EPUB kapağı metadata.yaml'daki cover-image (kapak.png) ile gelir.
    cover_opt=()
    [[ -f kapak.png ]] && cover_opt=(--epub-cover-image=kapak.png)
    pandoc metadata.yaml "${chapters[@]}" --toc --top-level-division=chapter \
      "${cover_opt[@]}" -o "${out}.epub"
    echo "yazıldı: ${out}.epub"
    ;;
  html)
    pandoc metadata.yaml "${chapters[@]}" --toc --standalone --number-sections -o "${out}.html"
    echo "yazıldı: ${out}.html"
    ;;
  pdf-html)
    # LaTeX'siz yol: pandoc ile gömülü (kapak + CSS) tek HTML üret, Chrome ile bas.
    local_cover=()
    [[ -f kapak.png ]] && local_cover=(--include-before-body=cover.html)
    pandoc metadata.yaml "${chapters[@]}" \
      --standalone --embed-resources --toc --toc-depth=2 --number-sections \
      -c print.css "${local_cover[@]}" \
      -o kitap.html
    chrome="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    [[ -x "$chrome" ]] || chrome="$(command -v google-chrome || command -v chromium || true)"
    if [[ -z "$chrome" ]]; then
      echo "Google Chrome bulunamadı; kitap.html üretildi, elle PDF'e basabilirsiniz." >&2
      exit 1
    fi
    "$chrome" --headless=new --disable-gpu --no-pdf-header-footer \
      --print-to-pdf="${out}.pdf" "file://$PWD/kitap.html"
    rm -f kitap.html
    echo "yazıldı: ${out}.pdf (Chrome ile)"
    ;;
  *)
    echo "bilinmeyen format: $fmt (pdf|pdf-html|epub|html)" >&2
    exit 1
    ;;
esac
