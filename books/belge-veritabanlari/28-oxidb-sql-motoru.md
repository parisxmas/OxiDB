# OxiDB'nin İkinci Motoru: SQL

Buraya kadar okuduğunuz her şey tek bir motoru anlatıyordu: belgeleri saklayan,
JSON tabanlı sorgularla süzen, toplama boru hattıyla dönüştüren belge motorunu.
Üçüncü bölümde ilişkisel modelden belge modeline geçişin gerekçelerini
tartışmış, ikisinin farklı sorulara farklı yanıtlar verdiğini söylemiştik. Şimdi
o tartışmanın bıraktığı yerden devam ediyoruz: OxiDB, aynı süreç içinde, aynı
portun ve aynı kimlik doğrulamanın arkasında, **ikinci bir motor** barındırır.
Bu motor ilişkiseldir; ilişkisel bir katalog, ilişkisel bir satır deposu, kendi
günlüğü ve kendi işlem yöneticisiyle gelir. Konuştuğu dil SQL'dir.

Bu bölüm o motoru anlatıyor: neden var olduğunu, belge motoruyla nasıl yan yana
yaşadığını, hangi SQL yüzeyini desteklediğini, sorguları nasıl hızlandırdığını
ve hangi istemcilerden konuşulabildiğini. İkinci kısımda kurduğumuz genel
ilkeler — depolama, günlük, indeks, sorgu işleme, işlem — burada da geçerlidir;
yalnızca somutlaştıkları biçim değişir.

## Neden ikinci bir motor?

OxiDB'nin geçmişinde bir SQL yüzeyi vardı ve bilinçli olarak **kaldırıldı**.
Nedeni öğreticidir: o yüzey gerçek bir motor değil, belge motorunun üzerine
serpiştirilmiş bir **SQL lehçesiydi**. Aynı depolamayı, aynı sorgu ağacını, aynı
işlem makinesini kullanıyordu. Sonuç, ikisinin de en kötü yanıydı: ilişkisel
semantiği (sabit şema, tip zorlaması, birleştirme) ilişkisel olmayan bir depoya
zorla giydirmek, hem SQL'i eksik hem belge motorunu karmaşık bırakıyordu.

İkinci girişimin çıkış noktası bambaşkaydı: **iki motor, tek örnek, farklı
dosyalar**. SQL motoru belge veritabanının dosyalarına dokunmaz; belge motoru
SQL tablolarını görmez. Paylaştıkları tek şey süreç, ağ portu, kimlik doğrulama
ve dış yaşam döngüsüdür. Bir koleksiyon adı ile bir tablo adı asla çakışmaz,
çünkü ikisi ayrı ad uzaylarında yaşar. Bu karar, projede ADR-0010 olarak
kayıtlıdır.^[Mimari Karar Kaydı (Architecture Decision Record): bir tasarım
kararının bağlamını, seçeneklerini ve gerekçesini kalıcılaştıran kısa belge.
OxiDB'nin kararları `docs/decisions/` altında numaralandırılmıştır.]

Bu ayrılığın pratik bir sonucu daha vardır: SQL motoru **varsayılan olarak
kapalıdır**. Sunucu, `OXIDB_SQL` ortam değişkeni doğru bir değere ayarlanmadıkça
motoru hiç kurmaz — ne dosya açar, ne bellek ayırır, ne iş parçacığı başlatır.
Kullanmadığınız şeyin bedelini ödemezsiniz; on üçüncü bölümde savunduğumuz
"isteğe bağlı yüzey" ilkesinin bir örneği daha.

```bash
# SQL motorunu açarak sunucuyu başlatın. Kapalıyken maliyeti sıfırdır.
export OXIDB_SQL=1
export OXIDB_DATA=./oxidb_data          # belge motorunun kökü
export OXIDB_SQL_DATA=./oxidb_data/sql  # SQL motorunun kendi dizini (varsayılan)
oxidb-server
```

Sunucu açıldığında iki motor da aynı bağlantı üzerinden konuşulabilir. İstek
üzerindeki `engine` alanı yolu belirler: alan yoksa ya da `"doc"` ise istek
belge motoruna, `"sql"` ise SQL motoruna gider. Eski istemciler tek bir bayt
bile değişiklik görmez.

```json
{
  "engine": "sql",
  "cmd": "sql",
  "sql": "SELECT ad FROM musteriler WHERE id = $1",
  "params": [7]
}
```

Bu istek, uzunluk önekli JSON protokolünün (yirmi dördüncü bölüm) sıradan bir
mesajıdır; yanıt da öyledir. Yetkilendirme tarafında `sql` komutu **ReadWrite**
rolüyle korunur; salt-okunur `Read` rolüne sahip bir oturum yalnızca `SELECT` ve
`SHOW` çalıştırabilir, veri değiştiren her ifade reddedilir.

Çok veritabanlı kurulumda (ADR-0012) SQL motoru da veritabanı başınadır:
varsayılan veritabanının tabloları `${OXIDB_DATA}/sql` altında, adlandırılmış her
veritabanınınkiler `${OXIDB_DATA}/<ad>/sql` altında yaşar. Yani iki veritabanının
`siparisler` tablosu birbirinden habersizdir.

## Mimari: katalog, satır deposu, günlük, denetim noktası

SQL motorunun iç yapısı, beşinci ve altıncı bölümlerde anlattığımız depolama ve
dayanıklılık ilkelerinin ilişkisel bir yeniden yorumudur.

**Katalog**, şemanın kendisidir: tablolar, sütunlar, tipler, kısıtlar, indeks
tanımları, görünümler ve saklı yordamlar. Belge motorunda şema yoktu — belgeler
kendi yapılarını taşırdı. Burada şema merkezîdir, çünkü satırların ikili
düzenini katalog belirler. Bir sütun tipi bilinmeden bir satırı kodlamak ya da
çözmek mümkün değildir.

**Satır deposu**, tablo başına bir `.rdat` dosyasıdır: sabit şemalı, tipli
hücrelerden oluşan satırların ikili gösterimi. Desteklenen tipler `INT`,
`DOUBLE`, `TEXT`, `BOOL`, `TIMESTAMP`, `BLOB` ve `DECIMAL`'dır. Zaman damgaları,
belge motoruyla aynı sözleşmeyi izler: epoch milisaniye olarak `i64`. `DECIMAL`
ise onluk tabanda **kesin** sabit noktalı aritmetiktir — para hesaplarının
kayan noktaya emanet edilmemesi gerektiği için vardır.

**Günlük** (WAL), SQL motorunun kendi yazma-öncesi günlüğüdür. Altıncı bölümde
gördüğümüz sözleşme aynen geçerlidir: bir değişiklik, veri dosyasına yansımadan
önce günlüğe dayanıklı biçimde yazılır; çökme sonrasında açılış, son denetim
noktasındaki anlık görüntüyü yükleyip günlüğün kuyruğunu yeniden oynatarak
durumu kurtarır.

**Denetim noktası** (checkpoint), günlüğü `.rdat` anlık görüntülerine katlayıp
kısaltır. Varsayılan olarak günlük 64 MiB'ı aştığında otomatik tetiklenir;
`OXIDB_SQL_CHECKPOINT_BYTES` ile ayarlanır, `0` verilirse yalnızca elle çağrılır.

Son olarak **disk-öncelikli mod** vardır. Varsayılanda motor tüm satırları
bellekte tutar; `OXIDB_SQL_DISK_FIRST` açıldığında ise satırların gövdesi son
denetim noktasının bellek eşlemli (`mmap`) anlık görüntüsünden okunur ve yalnızca
denetim noktasından sonraki değişiklikler bellekte bir örtü olarak durur. On
üçüncü bölümdeki bellek-disk ödünleşimi burada tek bir ortam değişkenine iner:
hız mı, ayak izi mi. İki mod aynı dosya biçimini paylaşır, yani bir veritabanı
istediğiniz modda yeniden açılabilir.

## SQL yüzeyi: şemadan sorguya

En baştan başlayalım. Tablo yaratmak, sütun tipleri, birincil anahtar ve otomatik
artan kimlik — hepsi tanıdık biçimdedir.

```sql
-- Şema: birincil anahtar, otomatik artan kimlik, NOT NULL, zaman damgası.
CREATE TABLE musteriler (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  ad     TEXT NOT NULL,
  sehir  TEXT,
  puan   INT,
  kayit  TIMESTAMP
);

CREATE TABLE siparisler (
  id       INT PRIMARY KEY AUTO_INCREMENT,
  musteri  INT,
  tutar    DECIMAL(12, 2),   -- para: kesin onluk aritmetik
  durum    INT,
  olusma   TIMESTAMP
);
```

Katalog, sorgulanabilir. Şemayı keşfetmek için ayrı bir araca gerek yoktur; bu
yüzeyi ileride EF Core'un iskele çıkarma (scaffolding) desteği de kullanacak.

```sql
SHOW TABLES;                  -- tablo adları
SHOW VIEWS;                   -- görünümler
SHOW INDEXES FROM siparisler; -- bir tablonun indeksleri
DESCRIBE musteriler;          -- sütunlar, tipler, null'lanabilirlik
```

Veri değiştirme ifadeleri de standarttır. Tek bir `INSERT` birden çok satır
alabilir ve — önemlisi — **atomiktir**: satırlardan biri birincil anahtarı ihlal
ederse hiçbiri yazılmaz.

```sql
-- Çok satırlı ekleme; AUTO_INCREMENT sütunu listeye yazılmayabilir.
INSERT INTO musteriler (ad, sehir, puan) VALUES
  ('ali', 'ankara', 10),
  ('ayse', 'izmir', 25),
  ('veli', 'ankara', NULL);

UPDATE musteriler SET puan = puan + 5 WHERE sehir = 'ankara';
DELETE FROM musteriler WHERE puan IS NULL;
```

Sorgu tarafında bağlama parametreleri iki biçimi de kabul eder: soru işareti
(`?`) soldan sağa sırayla, dolar-numara (`$1`, `$2`) konumsal olarak. Sekizinci
bölümde değindiğimiz gibi, parametre bağlama yalnızca kolaylık değil, aynı
zamanda enjeksiyona karşı yapısal savunmadır: değer hiçbir zaman metne
gömülmez.

```sql
-- İki parametre biçimi de geçerlidir.
SELECT ad, puan FROM musteriler WHERE sehir = ? AND puan > ?;
SELECT ad, puan FROM musteriler WHERE sehir = $1 AND puan > $2;
```

`SELECT`, beklediğiniz her şeyi yapar: yıldız ya da açık sütun listesi, ifade
projeksiyonu, takma adlar, projeksiyonda olmayan bir sütuna göre sıralama,
`LIMIT` ve `OFFSET` ile sayfalama.

```sql
-- İfade projeksiyonu + takma ad + çıktı takma adına göre sıralama + sayfalama.
SELECT id * 100 + puan AS skor, ad
FROM musteriler
ORDER BY skor DESC
LIMIT 10 OFFSET 20;
```

## Birleştirmeler ve gruplama

Birleştirme, ilişkisel modelin ayırt edici işlemidir; üçüncü bölümde belge
modelinin bu işlemi gömme ve referansla nasıl takas ettiğini tartışmıştık. SQL
motoru dört birleştirme türünü de destekler: `INNER`, `LEFT`, `RIGHT` ve `FULL`
— artı `CROSS JOIN`. Kendisiyle birleştirme ve çok tablolu zincirler de
çalışır. Yalnızca virgülle yazılan eski usul birleştirme (`FROM a, b`) bilinçli
olarak reddedilir; niyet açıkça yazılmalıdır.

```sql
-- Siparişi olmayan müşteriler: LEFT JOIN + sağ tarafta NULL denetimi
-- (klasik "anti-join" deseni).
SELECT m.ad
FROM musteriler m
LEFT JOIN siparisler s ON m.id = s.musteri
WHERE s.id IS NULL;
```

Motor, eşitlik koşullu birleştirmeleri karma (hash) birleştirmeyle çalıştırır;
çok sütunlu anahtarlar (`ON a.k1 = b.k1 AND a.k2 = b.k2`) ve eşitlik dışı artık
koşullar (`... AND b.tutar > 150`) doğru biçimde ele alınır. `NULL` anahtarlar
hiçbir zaman eşleşmez — bu, `LEFT JOIN` semantiğinin sessizce bozulabildiği
klasik tuzaktır ve testlerle çivilenmiştir.

Gruplama ve `HAVING` de tam anlamıyla oradadır. Toplama işlevlerinin `NULL`
davranışı standarda uyar: `COUNT(*)` satırları sayar, `COUNT(sütun)` `NULL`
olmayanları; `SUM`, `MIN`, `MAX`, `AVG` `NULL`'ları yok sayar; boş küme üzerinde
`COUNT` sıfır, diğerleri `NULL` verir ama yine de **tek bir satır** döner.

```sql
-- Şehir başına ciro, yalnızca ikiden çok siparişi olan şehirler.
SELECT m.sehir,
       COUNT(*)      AS siparis_sayisi,
       SUM(s.tutar)  AS ciro
FROM musteriler m
JOIN siparisler s ON m.id = s.musteri
GROUP BY m.sehir
HAVING COUNT(*) > 2
ORDER BY ciro DESC;
```

Alt sorgular üç bağlamda çalışır: skaler alt sorgu (`WHERE v = (SELECT MAX(v)
...)` — birden çok satır dönerse hata), `IN` alt sorgusu ve `EXISTS`.
`EXISTS`/`NOT EXISTS`, dış sorgunun sütunlarına **korelasyonlu** olabilir; motor
tek eşitlikli korelasyonları bir yarı-birleştirme kümesine dönüştürerek (yani
dekorele ederek) her dış satır için alt sorguyu yeniden çalıştırmaktan kurtulur.

```sql
-- 100 birimden büyük siparişi olan müşteriler (korelasyonlu EXISTS).
SELECT m.ad
FROM musteriler m
WHERE EXISTS (
  SELECT 1 FROM siparisler s
  WHERE s.musteri = m.id AND s.tutar > 100
);
```

Korelasyon, motorun en çok emek verilen köşelerinden biridir: dış referanslar
yalnızca bir seviye değil, iç içe geçmiş alt sorguların, türetilmiş tabloların ve
`VALUES` listelerinin **herhangi bir derinliğinden** dışarıyı görebilir; ara
kapsamlar adları doğru biçimde gölgeler.

## Analitik yüzey

Basit sorgular her motorun yaptığı iştir; bir SQL motorunu ayıran şey analitik
yüzeyidir. OxiDB'nin SQL motoru burada şaşırtıcı ölçüde geniştir.

**Pencere işlevleri** ile sıralama ve bölüm-içi toplama yapılır. Bir pencere,
`PARTITION BY` ile bölünür ve `ORDER BY` ile çalışan (kümülatif) hale gelir.

```sql
-- Bölüm içi sıralama: eşitler RANK'te aynı, ROW_NUMBER'da farklı numara alır.
SELECT departman, puan,
       ROW_NUMBER() OVER (PARTITION BY departman ORDER BY puan) AS sira,
       RANK()       OVER (PARTITION BY departman ORDER BY puan) AS rutbe,
       DENSE_RANK() OVER (PARTITION BY departman ORDER BY puan) AS yogun_rutbe
FROM personel
ORDER BY departman, puan;

-- Bölümün tamamı üzerinde toplam ve kümülatif (çalışan) toplam:
SELECT g, v,
       SUM(v) OVER (PARTITION BY g)          AS bolum_toplami,
       SUM(v) OVER (PARTITION BY g ORDER BY v) AS kumulatif
FROM olcumler;
```

İki sınırı açıkça söylemek gerekir: pencere işlevleri yalnızca **projeksiyonda**
kullanılabilir (`WHERE` içinde ya da toplama işlevleriyle aynı `SELECT`'te
karıştırılarak değil) ve açık çerçeve tanımı (`ROWS BETWEEN ...`) desteklenmez.
Pencere sonucunu süzmek istiyorsanız klasik çözüm işe yarar: bir görünüm ya da
türetilmiş tablo üzerinden filtreleyin.

```sql
-- Pencere sonucunu süzmenin yolu: görünüm (ya da türetilmiş tablo).
CREATE VIEW siralanan AS
  SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM olcumler;

SELECT v FROM siralanan WHERE rn = 1;
```

**`DISTINCT ON`**, PostgreSQL'den tanıdık "argmax" kestirmesidir: bir anahtarın
her değeri için, `ORDER BY`'ın ilk satırını tutar. Gruplamayla birleştiğinde,
"her müşterinin en çok harcadığı kategori" gibi soruları tek ifadeye indirir.

```sql
-- Her müşteri için baskın kategori: önce (müşteri, kategori) toplamları,
-- sonra müşteri başına en yüksek toplamı taşıyan satır.
SELECT DISTINCT ON (musteri) musteri, kategori, SUM(harcama) AS toplam
FROM satislar
GROUP BY musteri, kategori
ORDER BY musteri, SUM(harcama) DESC;

-- Sıralı-küme toplaması: grubun en sık görülen değeri (eşitlikte küçük olan).
SELECT g, mode() WITHIN GROUP (ORDER BY kategori) AS en_sik
FROM olaylar GROUP BY g;
```

**Küme işlemleri** tam takımdır: `UNION`, `EXCEPT`, `INTERSECT` — her biri `ALL`
çeşidiyle. `ALL` olmayan biçim tekilleştirir; `ALL` çanta (bag) semantiğini
korur, yani kopyalar sayılır. Öncelik standarttır: `INTERSECT` daha sıkı bağlar.

```sql
-- Çanta semantiği: soldaki her kopyayı sağdaki bir kopya götürür.
SELECT x FROM a EXCEPT ALL SELECT x FROM b ORDER BY x;

-- INTERSECT, UNION'dan sıkı bağlar: a UNION (b INTERSECT c).
SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c;
```

**`WITH`** (ortak tablo ifadeleri) sorguyu adlandırılmış parçalara böler.
Özyinelemesiz `WITH`, ayrıştırma anında türetilmiş tabloya açılır; sütun adı
listeleri (`t(a, b)`) desteklenir. Asıl ilginç olan **`WITH RECURSIVE`**'dir:
bir hiyerarşiyi ya da grafı, sabit noktaya ulaşana dek yinelemeyle dolaşır.
Yapı zorunludur: `çapa UNION [ALL] adım`, ve adım kolu kendi adına referans
verir.

```sql
-- Sayı üreteci: çapa 1'i verir, adım kolu bir öncekinin üstüne ekler.
WITH RECURSIVE t(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM t WHERE n < 10
)
SELECT count(*), sum(n) FROM t;   -- 10, 55
```

Aynı makine, gerçek veriyi de dolaşır. Bir organizasyon şemasında bir yöneticinin
altındaki herkesi derinlikleriyle çıkarmak, tek bir ifadeye sığar.

```sql
-- Geçişli kapanış: CTO'nun altındaki herkes, derinlik bilgisiyle.
WITH RECURSIVE alt(id, ad, derinlik) AS (
  SELECT id, ad, 0 FROM personel WHERE id = 2          -- çapa: CTO'nun kendisi
  UNION ALL
  SELECT p.id, p.ad, a.derinlik + 1                    -- adım: bir kat aşağısı
  FROM personel p JOIN alt a ON p.yonetici = a.id
)
SELECT ad, derinlik FROM alt ORDER BY derinlik, ad;
```

Döngülü graflarda `UNION` (yani `ALL` olmayan biçim) sonlanmayı sağlar: daha önce
üretilmiş satırlar elenir, dolayısıyla bir çevrim sonsuza dek dönmez. Buna
karşın `UNION ALL` ile yazılmış, sonlanma koşulu olmayan bir özyineleme
sonsuza kadar çalışmaz — motorun 1 milyon yineleme ve 10 milyon satır
korkulukları devreye girip anlaşılır bir hata döndürür.^[Sonsuza dek asılı kalan
bir sorgu, hata veren bir sorgudan çok daha kötüdür: birincisi kaynağı yer ve
teşhis edilmesi zordur; ikincisi kendini söyler.]

```sql
-- Döngülü graf: UNION (tekilleştiren) yinelemeyi sonlandırır.
WITH RECURSIVE erisim(dugum) AS (
  SELECT 1
  UNION
  SELECT k.hedef FROM kenar k JOIN erisim e ON k.kaynak = e.dugum
)
SELECT dugum FROM erisim ORDER BY dugum;
```

**`LATERAL` birleştirme**, sağdaki türetilmiş tablonun soldaki satıra
başvurmasına izin verir; yani "her grup için ilk N" sorusunu doğrudan yanıtlar.
Bu, EF Core'un koleksiyon projeksiyonlarını çevirdiği şekildir de.

```sql
-- Her blog için en yüksek puanlı 2 yazı (grup başına top-N).
SELECT b.id, y.baslik
FROM bloglar b
JOIN LATERAL (
  SELECT baslik FROM yazilar
  WHERE yazilar.blog_id = b.id
  ORDER BY puan DESC LIMIT 2
) y ON TRUE
ORDER BY b.id, y.baslik;
```

`LEFT JOIN LATERAL` boş dönen gövdeleri `NULL` ile doldurur; `CROSS JOIN
LATERAL`, `ON TRUE` ile aynı şeydir. `RIGHT`/`FULL` LATERAL ise reddedilir —
anlamı belirsiz olduğu için.

**Tarih/zaman** yüzeyi de EF Core ihtiyaçlarının itmesiyle olgunlaştı:
`NOW()`/`CURRENT_TIMESTAMP`, `EXTRACT(part FROM ts)` ve eşdeğeri
`date_part('part', ts)`, `date_trunc('part', ts)` (UTC takvim aritmetiği, ISO
haftaları, PostgreSQL'in hafta-günü numaralandırması), `INTERVAL` sabitleri ve
zaman damgası aritmetiği.

```sql
-- Aylık kova + hafta günü; INTERVAL sabitleri milisaniyeye katlanır.
SELECT date_trunc('month', olusma) AS ay,
       COUNT(*)                    AS adet
FROM siparisler
WHERE olusma >= NOW() - INTERVAL '30 days'
GROUP BY date_trunc('month', olusma)
ORDER BY ay;
```

Bir sınırı burada da dürüstçe söyleyelim: `INTERVAL` yalnızca **sabit** birimler
için (gün, saat, dakika, saniye, milisaniye) tanımlıdır; ay ve yıl gibi takvimsel
birimler reddedilir, çünkü sabit bir milisaniye karşılıkları yoktur. Ay eklemek
için takvim-doğru `add_months(ts, n)` işlevi vardır (ayın son günü taşarsa
kırpar). Skaler işlev dağarcığı bunun ötesinde geniştir: `FLOOR`/`CEILING`
(`DECIMAL` üzerinde kesin), `POWER`, `SQRT`, `%`/`MOD`, `POSITION`/`STRPOS`,
`LPAD`/`RPAD`, `LEAST`/`GREATEST`, tam kayan nokta matematiği ve düzenli ifade
eşleştirmesi (`regexp_like`).

## İndeksler ve sorgu hızlandırma

Yedinci bölümde gördüğümüz indeks ödünleşimi — okumayı hızlandır, yazmayı biraz
yavaşlat, yer harca — burada da aynen geçerlidir; yalnızca indeksin üzerinde
durduğu şey artık bir belge alanı değil, bir tablo sütunudur.

En ucuz erişim yolu **birincil anahtar nokta aramasıdır**: motor, `PRIMARY KEY`
için değerden satır kimliğine bir eşleme tutar, dolayısıyla `WHERE id = ?`
taramaya değil doğrudan bir aramaya iner. **İkincil indeksler** açıkça yaratılır
ve `INSERT`/`UPDATE`/`DELETE` boyunca otomatik bakılır: güncellenen bir satır
indeksin bir kovasından diğerine taşınır, silinen satır düşürülür. İndeksler
kataloğa yazıldığı için yeniden açılışta hayatta kalır — denetim noktası alınmamış
olsa bile, günlük yeniden oynatılarak yeniden kurulurlar.

```sql
-- İkincil indeks: seçici eşitlik aramalarını taramadan kurtarır.
CREATE INDEX ix_siparis_musteri ON siparisler(musteri);
CREATE INDEX IF NOT EXISTS ix_musteri_sehir ON musteriler(sehir);

-- Artık bu sorgu tabloyu taramaz, indeksten adayları alır:
SELECT id, tutar FROM siparisler WHERE musteri = 4211;
```

Motorun sorgu hızlandırma tarafında dört ayrı mekanizma birlikte çalışır.

Birincisi, **işlem içi indeks kullanımıdır**. Uzun süre, bir işlemin (ya da saklı
yordamın) içindeki okumalar indeksleri hiç danışmıyor, her seferinde tam tarama
yapıyordu. Artık işlem, temel tablonun ikincil indeksini danışıp sonucu kendi
tamponlanmış yazma örtüsüyle birleştirir. Etkisi çarpıcıdır: yaklaşık 1500 nokta
araması yapan, 500 bin satırlık bir sahtekârlık taraması saklı yordamı **55
saniyeden 0,15 saniyeye** indi.

İkincisi, **indeks-iç-içe-döngü birleştirmesidir** (index-nested-loop join). Sol
tarafı küçük olan bir birleştirme, sağdaki büyük tabloyu taramak yerine onun
indeksini yoklar. Klasik "küçük ⋈ büyük" deseni — 759 satırlık bir seçim ile 500
bin satırlık işlem tablosu — böylece bildirimsel SQL'de saklı yordam hızına
ulaşır.

Üçüncüsü, **akışlı tarama** (streamed scan) ve beraberindeki iki eniyileştirme:
yüklem itmesi (predicate pushdown) — `WHERE` koşulu satırlar somutlaştırılmadan,
ödünç alınmış satırlar üzerinde değerlendirilir — ve tarama-içi top-N: `ORDER BY
... LIMIT n` yalnızca n satırlık bir yığın tutarak taramayı gezer, tüm tabloyu
sıralamaz. Bu eniyileştirmelerin altın kuralı **görünmez olmalarıdır**: sonuçlar,
somutlaştırılmış tam bir taramanın vereceğiyle bayt bayt aynı olmalıdır.
Örneğin `DISTINCT ... LIMIT 3`, "ilk 3 satırı al sonra tekilleştir" diye
kısaltılamaz — tekilleştirme `LIMIT`'ten **önce** gelir. Dış birleştirmelerde de
yüklem, `NULL` ile doldurulabilen tarafa itilemez; itilirse yukarıdaki anti-join
deseni sessizce yanlış cevap verir.

Dördüncüsü, **birleştirme yeniden sıralamasıdır**: yazılış sırası ne olursa
olsun, planlayıcı küçük tabloları öne alabilir. Dış birleştirmeler ve `LATERAL`
söz konusu olduğunda yeniden sıralama yapılmaz, çünkü sıra semantiğin parçasıdır.

Bir uyarıyı da yedinci bölümden hatırlatalım: indeks her sorgunun devası
değildir. Tablonun tamamını gezen bir toplama sorgusu indeksten yararlanmaz —
PostgreSQL'de olduğu gibi burada da tüm satırlar okunur. İndeksler seçici
erişimi ve küçük-büyük birleştirmeleri kurtarır; tam tarama işini ortadan
kaldırmaz.

## İşlemler

Onuncu bölümde işlemlerin ne vaat ettiğini tanımlamıştık: ya hep ya hiç,
tutarlılık, yalıtım, dayanıklılık. SQL motoru kendi işlem yöneticisini taşır ve
işlemler **oturum boyunca** açık kalabilir; yani `BEGIN` bir çağrıda, `COMMIT`
başka bir çağrıda gelebilir. Sunucu, açık işlemin kimliğini oturumla birlikte
park eder; bağlantı düşerse işlem güvenle geri alınır.

```sql
BEGIN;
UPDATE hesaplar SET bakiye = bakiye - 100 WHERE id = 1;
UPDATE hesaplar SET bakiye = bakiye + 100 WHERE id = 2;
-- Bu noktada, işlemin içinden okuyan biri yeni bakiyeleri görür
-- (kendi yazdığını okuma); dışarıdan otomatik-commit ile okuyan
-- biri hâlâ eski bakiyeleri görür (yalıtım).
COMMIT;
```

İşlem içinde **kayıt noktaları** (savepoint) vardır: bir alt bölümü, işlemin
tamamını feda etmeden geri almanın yolu. Kayıt noktası bir işlem bağlamı
gerektirir — açık işlem yokken `SAVEPOINT` çağırmak hata verir.

```sql
BEGIN;
INSERT INTO t VALUES (5, 'p');
SAVEPOINT a;
INSERT INTO t VALUES (6, 'q');
ROLLBACK TO SAVEPOINT a;   -- yalnızca 6 geri alınır
INSERT INTO t VALUES (7, 'r');
COMMIT;                    -- 5 ve 7 kalıcı olur
```

Birincil anahtar tekliği işlemlerin içinde de zorlanır: hem taahhüt edilmiş bir
satırla çakışma, hem aynı işlemin iki taahhüt edilmemiş satırı arasındaki
çakışma yakalanır. Silip aynı anahtarı yeniden eklemek ise geçerlidir.

## Saklı yordamlar: iki dil

Saklı yordamlar (ADR-0014) iki dilde yazılabilir ve ikisi de aynı `CALL` ile
çağrılır. Bir `CALL` **atomiktir**: üst seviyede örtük bir işlem açar, açık bir
işlem varsa ona katılır. Gövdedeki herhangi bir ifade patlarsa, o ana dek yapılan
her şey geri alınır.

Birinci dil **SQL metnidir**: gövde, adlandırılmış parametreleri olan bir DML/
SELECT toplu işidir. Hiçbir araç zinciri gerektirmez; her çağrıda yeniden
ayrıştırılır. `CALL`'ın sonucu, gövdedeki **son** ifadenin sonucudur.

```sql
-- SQL gövdeli saklı yordam: para yatır ve yeni bakiyeyi döndür.
CREATE PROCEDURE yatir(kime TEXT, tutar DOUBLE) AS BEGIN
  UPDATE hesap SET bakiye = bakiye + tutar WHERE ad = kime;
  SELECT bakiye FROM hesap WHERE ad = kime;
END;

CALL yatir('ali', 25);          -- 125.0
CALL yatir($1, $2);             -- çağıranın parametreleri de kullanılabilir
```

Bir ayrıntı bilinçlidir ve belgelenmiştir: ifade konumunda, bir parametre adıyla
aynı olan niteliksiz bir ad **parametreyi** ifade eder, sütunu değil. Sütuna
ulaşmak isterseniz tablo adıyla nitelendirin (`hesap.ad`).

İkinci dil **Cobra**'dır: derlenmiş bir bayt kodu, motorun içindeki bir sanal
makinede çalışır. Yordam gövdesi `.cobrac` dosyasının base64'üdür; dosya bir
`run(db, ...)` işlevi tanımlar, `db.query`/`db.execute` çağrıları `CALL`'ın
işlemine katılır, `print` çıktıları istemciye **bildirim** (notice) olarak
döner, ve yürütme 100 milyon komutluk bir **yakıt** sınırıyla korunur — sonsuz
döngü bir sunucuyu esir alamaz. Determinizm `CREATE` anında doğrulanır (eşzamansız
çağrılar, dış içe aktarma, giriş/çıkış reddedilir), böylece bir `CALL` küme
genelinde güvenle çoğaltılabilir.

```python
# Cobra kaynağı (derlenip base64 olarak CREATE PROCEDURE'e gömülür).
# Sorgular, döngüde toplama, bir bildirim ve sözlük dönüşü.
def run(db)
    let rows = db.query("SELECT name, age FROM people ORDER BY age")
    let total = 0
    let n = 0
    let oldest = ""
    for r in rows
        total = total + r["age"]
        n = n + 1
        oldest = r["name"]
    end
    print("stats over", n, "rows")          # -> istemciye bildirim
    return {"count": n, "total": total, "oldest": oldest}
end
```

Cobra'nın `try`/`catch`'i, kısıt ihlali gibi hataları yakalanabilir kılar; yordam
patlamak yerine bir yedek değer döndürebilir. Yordamlar birbirini çağırabilir
(aynı işlemi paylaşırlar) ve özyineleme bir derinlik korkuluğuyla sınırlıdır.

```sql
-- Derlenmiş Cobra bayt kodu, base64 olarak gömülür.
CREATE PROCEDURE stats() LANGUAGE COBRA AS '<base64 .cobrac>';
CALL stats();
SHOW PROCEDURES;
```

## İstemciler

En alt katman, gördüğümüz JSON isteğidir; her istemci kütüphanesi onun üzerine
oturur. Yanıt, ifade başına bir sonuç taşır: `SELECT` için `{"columns": [...],
"rows": [[...]]}`, DML için `{"affected": N}`, DDL için `{"ddl": true}`, işlem
ifadeleri için `{"transaction": true}`.

Python istemcisinde bu tek bir metottur.

```python
from oxidb import OxiDb

db = OxiDb(host="127.0.0.1", port=4444, username="admin", password="...")

db.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
db.sql("INSERT INTO users VALUES (?, ?)", [1, "ada"])

[sonuc] = db.sql("SELECT name FROM users WHERE id = $1", [1])
print(sonuc["columns"])   # ['name']
print(sonuc["rows"])      # [['ada']]
```

JavaScript SDK'sında aynı yüzey `sql(sql, params)` olarak durur ve söz (promise)
döndürür.

```javascript
import { OxiDb } from "oxidb";

const db = new OxiDb({ host: "127.0.0.1", port: 4444 });
await db.connect();

await db.sql("INSERT INTO users VALUES (?, ?)", [2, "grace"]);
const [r] = await db.sql("SELECT name FROM users ORDER BY id");
console.log(r.rows); // [['ada'], ['grace']]
```

.NET tarafında iş daha derindir. `OxiDb.Data`, gerçek bir **ADO.NET
sağlayıcısıdır**: `OxiDbConnection`, `OxiDbCommand`, `DbDataReader`. Bu, Dapper
gibi ADO.NET üstüne kurulu her şeyin doğrudan çalışması demektir.

```csharp
using Dapper;
using OxiDb.Data;

using var conn = new OxiDbConnection("Host=127.0.0.1;Port=4444");
conn.Open();

conn.Execute("""
    CREATE TABLE musteriler (
      id    INT PRIMARY KEY AUTO_INCREMENT,
      ad    TEXT NOT NULL,
      puan  INT
    )
    """);

// Dapper'ın adlandırılmış parametreleri, sağlayıcı tarafından bağlanır.
conn.Execute("INSERT INTO musteriler (ad, puan) VALUES (@Ad, @Puan)",
             new { Ad = "ali", Puan = 10 });

var iyiler = conn.Query<string>(
    "SELECT ad FROM musteriler WHERE puan > @Alt ORDER BY puan DESC",
    new { Alt = 5 });
```

## EF Core: sağlayıcı, göç, iskele

Bir adım yukarısı, Entity Framework Core sağlayıcısıdır (ADR-0013). LINQ
sorguları SQL'e çevrilir; `DateTime` üyeleri ve `AddX` metotları tarih
işlevlerine, `Math.*` çağrıları skaler işlevlere, `Contains`/`StartsWith` metin
işlevlerine, EF'in `CROSS`/`OUTER APPLY` yapıları `[LEFT] JOIN LATERAL`'a düşer.

```csharp
public sealed class ShopContext(string cs) : DbContext
{
    public DbSet<Musteri> Musteriler => Set<Musteri>();
    public DbSet<Siparis> Siparisler => Set<Siparis>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseOxiDb(cs);   // OxiDB sağlayıcısı
}

// Sıradan LINQ; motorun tarafında gruplu bir SQL sorgusuna çevrilir.
var enIyiSehirler = db.Siparisler
    .Join(db.Musteriler, s => s.MusteriId, m => m.Id, (s, m) => new { m.Sehir, s.Tutar })
    .GroupBy(x => x.Sehir)
    .Select(g => new { Sehir = g.Key, Toplam = g.Sum(x => x.Tutar) })
    .OrderByDescending(x => x.Toplam)
    .Take(5)
    .ToList();
```

Sağlayıcı yalnızca sorgu çevirmez. **Göçler** (migrations) çalışır: gerçek bir
`__EFMigrationsHistory` tablosuyla `Database.Migrate()`, sütun ekleme/silme/yeniden
adlandırma, indeks yaratma/düşürme. **İskele çıkarma** (`dotnet ef dbcontext
scaffold`) da çalışır; var olan bir şemadan sınıf üretmek için yukarıda gördüğümüz
`SHOW TABLES`/`DESCRIBE`/`SHOW INDEXES` yüzeyini kullanır.

Bu iddiaların ölçüsü nedir? En sert ölçü, Microsoft'un kendi yayımladığı **resmî
EF Core ilişkisel şartname testleridir**. OxiDB sağlayıcısı bu paketin on iki
Northwind takımının tamamını — `Where`, `Select`, `Functions`, `Join`, `GroupBy`,
`Include`, `Navigations`, `SetOperations` ve diğerleri — **3832 testin
3832'sinde** geçer. Geçmeyen bir avuç senaryo, testlerde açıkça belirtilmiş
sınırlardır (örneğin `DECIMAL`'in çift duyarlıklı hassasiyeti) ve çoğu SQLite
sağlayıcısının da atladığı testlerle örtüşür. Bu tür bir uyum, bir SQL motorunun
"gerçek" olup olmadığının en dürüst sınavıdır; çünkü testleri siz yazmazsınız.

## Performans

Sayılar bağlamsız anlamsızdır, o yüzden iki somut ölçümle bitirelim.

Birincisi, aynı EF Core sağlayıcısının PostgreSQL karşısındaki durumu. Aynı
model, aynı LINQ sorguları, aynı makine: birincil anahtar nokta araması, indeksli
yabancı anahtar süzmesi, `ORDER BY ... LIMIT 20`, yüklemli `COUNT`, süzülmüş
`SUM`, projeksiyonlu süzme, metin `Contains`, tekil ekleme, 100'lük toplu ekleme,
nokta güncelleme, nokta silme. Bu basit sorgu şekillerinin tamamında OxiDB
öndedir. Bu sürpriz değildir: gömülü bir motorun ağ ve süreç sınırı maliyeti
düşüktür ve satırların çoğu zaten bellektedir.

İkincisi, daha zorlu bir sınav. Üretimde çalışan, PostgreSQL için yazılmış bir
müşteri segmentasyonu işlevi — `WITH`, `DISTINCT ON`, `mode() WITHIN GROUP`,
`LEAST` içeren, 300 bin satır üzerinde çalışan bir sorgu — OxiDB'de **metni
harfi harfine aynı kalarak** çalıştırılabildi; çıktı PostgreSQL 15'inkiyle
birebir aynı, süre ise biraz daha kısaydı (~144 ms'ye karşı ~149 ms). Buradaki
asıl haber hız değil, **taşınabilirliktir**: sorguyu yeniden yazmak gerekmedi.

Buna karşılık, dürüst olalım: analitik iş yükünün ağır ucunda — tam tablo
taramalı büyük toplamalarda, karmaşık çok tablolu planlarda — olgun bir ilişkisel
motorun on yıllarca biriktirdiği planlayıcı zekâsı hâlâ öndedir. OxiDB'nin SQL
motoru bir veri ambarı motoru değildir; gömülü ve sunucu iş yüklerinde, uygulama
sorgularının büyük çoğunluğunu hızlı ve doğru biçimde karşılamak için tasarlandı.

## Sınırlar ve kapanış

Kapanışta, bu motorun bugün nerede durduğunu açıkça yazalım. Takvimsel `INTERVAL`
birimleri (ay, yıl) desteklenmez; pencere işlevlerinin açık çerçeveleri yoktur;
küme bazlı yabancı anahtar zorlaması, tetikleyiciler ve maddileşmiş görünümler
yoktur. SQL motoru v1'de düğüm-yereldir; belge motorunun küme yetenekleriyle
aynı olgunlukta değildir. Ve iki motor **henüz ortak bir işlem paylaşmaz**: bir
belgeyi ve bir tabloyu tek bir atomik işlemde güncellemek, ADR-0011 olarak
önerilmiş ama henüz gerçeklenmemiş bir yoldur.

Ama tabloyu bütün olarak görmek gerekir. Aynı ikili, aynı port, aynı kimlik
doğrulama; bir tarafta şemasız belgeler ve toplama boru hattı, öbür tarafta sabit
şemalı tablolar, birleştirmeler, pencere işlevleri, özyinelemeli sorgular, saklı
yordamlar ve resmî şartname testlerini geçen bir EF Core sağlayıcısı. Üçüncü
bölümde "ilişkisel mi, belge mi?" diye sormuştuk. OxiDB'nin yanıtı, bu sorunun
bir seçim olmak zorunda olmadığıdır — yeter ki iki motor birbirinin dosyalarına
karışmasın.
