# Belge Veritabanları — Temellerden OxiDB'ye

Bu dizin, belge veritabanlarını **sıfırdan** anlatan, ardından OxiDB'nin nasıl
çalıştığını **adım adım** açıklayan Türkçe bir kitabın kaynağıdır. Kitap düz
metindir (örnek kod içermez); O'Reilly tarzında, kavramları temelden kurarak
ilerler.

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
./build.sh          # belge-veritabanlari.pdf
./build.sh epub     # belge-veritabanlari.epub
./build.sh html     # belge-veritabanlari.html
```

Gereksinim: `pandoc` ve `xelatex` (Türkçe karakterler için). Kurulum örnekleri
`build.sh` başında.

## İçindekiler (plan)

Kitap üç kısımdan oluşur. Tamamlanan bölümlerin yanında ✅ vardır.

**Önsöz** ✅

### Kısım I — Temeller
1. Veri, veritabanı ve veritabanı yönetim sistemleri ✅
2. Veri modelleri: hiyerarşikten belgeye ✅
3. İlişkisel modelden belge modeline: neden ve ne zaman ✅
4. Belge modeli derinlemesine: JSON, şema esnekliği, gömme ve referans

### Kısım II — Bir Belge Veritabanı İçeride Nasıl Çalışır
5. Depolama motorları: sayfa tabanlı, append-only, LSM ve B-ağaçları
6. Dayanıklılık: write-ahead log, fsync ve çökmeden kurtarma
7. İndeksleme: B-ağacı indeksleri, bileşik indeksler, ters indeks
8. Sorgu işleme: ayrıştırma, planlama, indeks seçimi
9. Toplama (aggregation): pipeline modeli, gruplama, pencere fonksiyonları
10. İşlemler: ACID, izolasyon, kilitleme, MVCC ve OCC
11. Eşzamanlılık ve tutarlılık
12. Ölçeklendirme: replikasyon, konsensüs ve sharding
13. Bellek, önbellek ve disk ödünleşimi
14. Güvenlik: kimlik doğrulama, yetkilendirme, şifreleme, denetim

### Kısım III — OxiDB Adım Adım
15. OxiDB'ye genel bakış ve mimari
16. Depolama katmanı: in-RAM ve disk-first, mmap, .bdat / .btree
17. WAL, dayanıklılık ve kurtarma; katı ve gevşek senkronizasyon
18. İndeksler: alan, bileşik ve mmap tabanlı disk indeksleri
19. Sorgu motoru: operatörler, indeks destekli yollar, byte düzeyinde filtreleme
20. Toplama pipeline'ı: gruplama, $facet, pencere fonksiyonları
21. İşlemler: iyimser eşzamanlılık ve üç fazlı commit
22. Sıkıştırma (compaction): ölü alan ve otomatik tetikleme
23. Tam metin arama, blob depolama, şifreleme ve zaman-noktasına kurtarma
24. Sunucu: OxiWire protokolü, kimlik doğrulama, RBAC, denetim
25. Ölçeklendirme: Raft kümesi ve OxiPool ile sharding
26. Uyumluluk katmanları ve istemciler
27. Bellek optimizasyonu ve karşılaştırmalı değerlendirme

**Ek A** — Sözlük · **Ek B** — Kaynaklar
