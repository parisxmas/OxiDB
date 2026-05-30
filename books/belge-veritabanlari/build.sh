#!/usr/bin/env bash
#
# Kitabı tek bir dosyada derler. Bölüm dosyaları ad sırasına göre (00, 01, 02…)
# birleştirilir, bu yüzden yeni bölüm eklerken numara önekine dikkat edin.
#
# Gereksinim: pandoc + bir LaTeX dağıtımı (xelatex). macOS: `brew install pandoc
# basictex` (sonra `sudo tlmgr install xetex`), ya da `brew install pandoc
# mactex`. Linux: `apt install pandoc texlive-xetex texlive-fonts-recommended`.
#
# Kullanım:
#   ./build.sh            # belge-veritabanlari.pdf üretir
#   ./build.sh epub       # belge-veritabanlari.epub üretir
#   ./build.sh html       # tek dosyalık belge-veritabanlari.html üretir
set -euo pipefail
cd "$(dirname "$0")"

fmt="${1:-pdf}"
chapters=( $(ls [0-9][0-9]-*.md | sort) )
out="belge-veritabanlari"

case "$fmt" in
  pdf)
    pandoc metadata.yaml "${chapters[@]}" \
      --toc --number-sections --top-level-division=chapter \
      --pdf-engine=xelatex \
      -V mainfont="DejaVu Serif" \
      -V monofont="DejaVu Sans Mono" \
      -o "${out}.pdf"
    echo "yazıldı: ${out}.pdf"
    ;;
  epub)
    pandoc metadata.yaml "${chapters[@]}" --toc --top-level-division=chapter -o "${out}.epub"
    echo "yazıldı: ${out}.epub"
    ;;
  html)
    pandoc metadata.yaml "${chapters[@]}" --toc --standalone --number-sections -o "${out}.html"
    echo "yazıldı: ${out}.html"
    ;;
  *)
    echo "bilinmeyen format: $fmt (pdf|epub|html)" >&2
    exit 1
    ;;
esac
