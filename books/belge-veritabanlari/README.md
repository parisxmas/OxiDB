# OxiDB Doküman Veritabanı

**Ölçeklenebilir, NoSQL Doküman Tabanlı Çözümler Rehberi** — Barış AKIN ve
Anthropic (2026).

### 📖 [PDF'i indir](https://github.com/parisxmas/OxiDB/raw/master/books/belge-veritabanlari/belge-veritabanlari.pdf)

Kitabın derlenmiş tam hali (PDF, ~116 sayfa, kapak + 27 diyagram dahil) bu
dizinde `belge-veritabanlari.pdf` olarak bulunur. Yukarıdaki bağlantı dosyayı
doğrudan indirir. (GitHub PDF'leri dosya listesinde önizlemez; ya bu bağlantıyı
kullanın ya da dosyaya tıklayıp **"Download"** deyin.)

Bu dizin, belge (doküman) veritabanlarını **sıfırdan** anlatan, ardından
OxiDB'nin nasıl çalıştığını **adım adım** açıklayan Türkçe bir kitabın
kaynağıdır. Kitap düz metindir (örnek kod içermez); O'Reilly tarzında,
kavramları temelden kurarak ilerler.

## Kapak

Kapak görseli, bu dizinde **`kapak.png`** olarak bulunmalıdır. Dosya mevcutsa
`build.sh` onu otomatik kullanır: PDF'te tam-sayfa ilk sayfa olarak
(`kapak.tex` aracılığıyla), EPUB/HTML'de kapak görseli olarak. Dosya yoksa kitap
kapaksız üretilir (uyarı verilir). Kapak/başlık meta bilgisi `metadata.yaml`
içindedir.

## Biçim ve düzen

- Her bölüm ayrı bir Markdown dosyasıdır. Dosyalar **iki haneli numara**
  önekiyle adlandırılır (`00-`, `01-`, …) ve PDF derlenirken bu ada göre
  sıralanıp birleştirilir.
- `metadata.yaml` başlık, yazar, dil (tr-TR) ve PDF ayarlarını tutar.
- `build.sh` bölümleri tek bir PDF/EPUB/HTML'e dönüştürür.

Yeni bölüm eklemek: uygun numarayla bir `NN-ad.md` dosyası oluşturun, dosyanın
başına `# Bölüm Başlığı` koyun. Numaralandırma ve içindekiler otomatiktir.

## PDF'e (veya EPUB/HTML'e) çevirme

```sh
./build.sh           # belge-veritabanlari.pdf  (pandoc + xelatex gerektirir)
./build.sh pdf-html  # belge-veritabanlari.pdf  (pandoc + Google Chrome; LaTeX'siz)
./build.sh epub      # belge-veritabanlari.epub
./build.sh html      # belge-veritabanlari.html
```

Gereksinim: `pandoc`. Varsayılan `pdf` yolu **xelatex** (LaTeX) kullanır ve
kaynak künyelerini **sayfa altında gerçek dipnotlar** olarak basar; bunun için
bir TeX dağıtımı (örn. TinyTeX ya da TeX Live; Türkçe için `babel-turkish`),
SVG şekilleri çevirmek için `rsvg-convert` (librsvg) ve Türkçe gliflerini içeren
bir yazı tipi (macOS'ta Georgia + Menlo; Linux'ta `BOOK_MAINFONT`/`BOOK_MONOFONT`
ile DejaVu) gerekir. LaTeX istemiyorsanız `./build.sh pdf-html` yolu yalnızca
pandoc + Google Chrome ister (dipnotlar sayfa altı yerine belge sonunda toplanır).
Depodaki `belge-veritabanlari.pdf`, varsayılan **xelatex** yoluyla üretilmiştir
(~124 sayfa, kapak ilk sayfada, sayfa-altı dipnotlu).

## İçindekiler (plan)

Kitap üç kısımdan oluşur. Tamamlanan bölümlerin yanında ✅ vardır.

**Önsöz** ✅

### Kısım I — Temeller
1. Veri, veritabanı ve veritabanı yönetim sistemleri ✅
2. Veri modelleri: hiyerarşikten belgeye ✅
3. İlişkisel modelden belge modeline: neden ve ne zaman ✅
4. Belge modeli derinlemesine: JSON, şema esnekliği, gömme ve referans ✅

### Kısım II — Bir Belge Veritabanı İçeride Nasıl Çalışır
5. Depolama motorları: sayfa tabanlı, append-only, LSM ve B-ağaçları ✅
6. Dayanıklılık: write-ahead log, fsync ve çökmeden kurtarma ✅
7. İndeksleme: B-ağacı indeksleri, bileşik indeksler, ters indeks ✅
8. Sorgu işleme: ayrıştırma, planlama, indeks seçimi ✅
9. Toplama (aggregation): pipeline modeli, gruplama, pencere fonksiyonları ✅
10. İşlemler: ACID, izolasyon, kilitleme, MVCC ve OCC ✅
11. Eşzamanlılık ve tutarlılık ✅
12. Ölçeklendirme: replikasyon, konsensüs ve sharding ✅
13. Bellek, önbellek ve disk ödünleşimi ✅
14. Güvenlik: kimlik doğrulama, yetkilendirme, şifreleme, denetim ✅

### Kısım III — OxiDB Adım Adım
15. OxiDB'ye genel bakış ve mimari ✅
16. Depolama katmanı: in-RAM ve disk-first, mmap, .bdat / .btree ✅
17. WAL, dayanıklılık ve kurtarma; katı ve gevşek senkronizasyon ✅
18. İndeksler: alan, bileşik ve mmap tabanlı disk indeksleri ✅
19. Sorgu motoru: operatörler, indeks destekli yollar, byte düzeyinde filtreleme ✅
20. Toplama pipeline'ı: gruplama, $facet, pencere fonksiyonları ✅
21. İşlemler: iyimser eşzamanlılık ve üç fazlı commit ✅
22. Sıkıştırma (compaction): ölü alan ve otomatik tetikleme ✅
23. Tam metin arama, blob depolama, şifreleme ve zaman-noktasına kurtarma ✅
24. Sunucu: OxiWire protokolü, kimlik doğrulama, RBAC, denetim ✅
25. Ölçeklendirme: Raft kümesi ve OxiPool ile sharding ✅
26. Uyumluluk katmanları ve istemciler ✅
27. Bellek optimizasyonu ve karşılaştırmalı değerlendirme ✅

**Ek A** — Sözlük ✅ · **Ek B** — Kaynaklar ✅

---

**Durum:** Kitabın tamamı yazıldı — Önsöz + 27 bölüm + 2 ek. `./build.sh` ile
(kapak `kapak.png` dizinde olduğunda) tek bir PDF/EPUB/HTML olarak derlenebilir.
