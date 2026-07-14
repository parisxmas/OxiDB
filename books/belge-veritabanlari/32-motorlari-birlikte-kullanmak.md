# Motorları Birlikte Kullanmak: Tek Sunucu, Beş Yüzey, Tek Uygulama

Buraya kadar OxiDB'nin yüzeylerini teker teker tanıdık. Belge motorunu Kısım
III boyunca söktük taktık; ardından gelen dört bölümde ilişkisel SQL motorunu,
zaman serisi motorunu, anahtar-değer katmanı OxiMem'i ve nesne depolama
katmanını ayrı ayrı gördük. Her biri kendi başına anlamlıydı. Ama gerçek bir
uygulama hiçbir zaman tek bir veri şeklinden ibaret değildir; kullanıcı
profilleri, sipariş defterleri, fiyat akışları, fatura dosyaları ve oturum
belirteçleri aynı sistemde, aynı gün içinde yan yana yaşar.

Bu bölüm, o teker teker öğrendiklerimizi tek bir çalışan sistemde buluşturur.
Sorusu şudur: **hangi veri hangi motora gider ve neden?** Cevabı da bir liste
değil, baştan sona kurulmuş bir uygulamadır. Bir kripto para borsası kuracağız —
kitabın örnek deposundaki gerçek uygulamalara (canlı borsa ve kripto besleyici)
çok benzeyen, ama burada tek bir Python istemcisiyle, tek bir TCP bağlantısı
üzerinden anlatılan bir borsa. Tüm örnekler tek bir senaryonun evrimidir;
kopuk parçalar değil.

## Motor seçimi: veri şekli, doğru motoru söyler

Bir veriyi hangi motora koyacağınıza karar verirken sorulacak soru "hangisi daha
hızlı?" değildir. Doğru soru şudur: **bu verinin doğal şekli ne, ve ona ne
soracağım?** Motor seçimi, veri şeklinin bir sonucudur; bir zevk meselesi
değil.

| Veri şekli | Örnek | Doğru motor | Neden |
|---|---|---|---|
| Değişken şemalı, iç içe, kimlik odaklı | Kullanıcı profili, KYC kaydı, ürün kataloğu | **Belge** | Şema evrilir; kayıt tek parça okunur; iç içe alanlar bölünmeden durur |
| Sabit şemalı, ilişkisel, join'li, raporlanan | Emirler, işlemler, muhasebe defteri | **SQL** | Bütünlük kısıtları, join, GROUP BY, tarihsel analitik |
| Yüksek hacimli ölçüm akışı, zaman eksenli | Fiyat tick'leri, metrikler, sensörler | **TSDB** | Nokta başına ~0,3 bayt sıkıştırma; zaman kovaları; blok bazlı saklama süresi |
| Büyük, opak bayt yığını | Fatura PDF'i, ürün görseli, kimlik taraması | **S3 nesne** | Belge/satır içinde taşınmaz; akışla yazılır; içeriğinden metin çıkarılabilir |
| Sıcak, geçici, kısa ömürlü durum | Oturum belirteci, hız sınırı sayacı, son fiyat | **OxiMem** | Mikrosaniye erişim, TTL, dayanıklılık gerektirmez |

Bu tablonun altında yatan tek bir ilke vardır: **veriyi, ona soracağınız soruya
göre yerleştirin.** Fiyat tick'lerini bir belge koleksiyonuna da yazabilirsiniz —
çalışır. Ama günde on milyon tick'e ulaştığınızda, her tick'i bir JSON belgesi
olarak saklamanın bedelini (nokta başına onlarca bayt, indeks şişmesi, "son bir
saatin dakikalık mumları" sorusuna verilecek pahalı cevap) ödersiniz. Aynı veriyi
TSDB'ye yazdığınızda, o soru motorun doğal dilidir.

Ters yönü de doğrudur: kullanıcı profilini SQL'e sıkıştırmaya çalışmak, her yeni
alan için bir `ALTER TABLE` demektir; oysa KYC alanları ülkeye göre değişir.
Değişken şema, belge motorunun var oluş sebebidir.

## Aynı sunucu, ayrı isim uzayları

Bu beş yüzeyin hepsi **tek bir sunucu sürecinde** yaşar ve — belge, SQL ve TSDB
söz konusu olduğunda — **tek bir TCP bağlantısı** üzerinden konuşulur. Yine de
birbirlerinin ayağına basmazlar; çünkü aralarında paylaşılan hiçbir durum
yoktur.

Ayrımın ilk katmanı **dosya sistemidir**. Bir veritabanının veri dizini içinde,
belge koleksiyonları kendi dosyalarında, SQL motoru `sql/` altında, TSDB motoru
`tsdb/` altında, bloblar `_blobs/` altında yaşar:

```bash
# Tek bir veritabanının (varsayılan: oxidb) disk yerleşimi.
oxidb_data/
├── _auth/                 # sunucu geneli: kullanıcılar, roller
├── _audit/                # sunucu geneli: denetim günlüğü
└── oxidb/                 # "oxidb" veritabanı (ADR-0012: her veritabanı bir alt dizin)
    ├── users.dat          # belge motoru: koleksiyon başına append-only dosya
    ├── users.wal
    ├── _blobs/            # nesne depolama: kova/nesne
    │   └── kyc/
    ├── _fts/              # tam metin indeksi
    ├── sql/               # SQL motoru: kendi tabloları, kendi WAL'i
    └── tsdb/              # TSDB motoru: bloklar + MANIFEST + WAL
```

Bu yüzden `users` adında bir **koleksiyon** ile `users` adında bir **tablo** asla
çakışmaz: farklı motorlara, farklı dosyalara, farklı isim uzaylarına aittirler.
İkisi de var olabilir, ikisi de tamamen farklı veri tutabilir. Bu bir kaza değil,
bir tasarım kararıdır (ADR-0010): ikinci motor, birincisinin yanına *eklenir*,
içine karışmaz.

Ayrımın ikinci katmanı **tel protokolüdür**. Her istekte isteğe bağlı bir
`engine` alanı vardır; yokluğunda varsayılan `"doc"`tur. Yani eski istemciler
bayt bayt aynı şekilde çalışmaya devam eder:

```json
{"cmd": "insert", "collection": "users", "doc": {"email": "ada@ornek.com"}}
{"engine": "sql",  "cmd": "sql",  "sql": "SELECT * FROM orders WHERE id = ?", "params": [7]}
{"engine": "tsdb", "cmd": "tsdb", "op": "query", "measurement": "ticks", "field": "price"}
```

Üç mesaj, üç motor, tek bağlantı. Sunucu, isteği okuduğu anda `engine` alanına
bakar ve doğru motora yönlendirir; gerisi o motorun kendi dünyasında geçer.

OxiMem ve nesne depolama biraz farklıdır. OxiMem, Redis'in RESP protokolünü
konuştuğu için kendi dinleyicisinde (`OXIDB_OXIMEM_PORT`) durur — mevcut Redis
istemcileriyle konuşabilmesi için. Nesne depolama ise hem ana TCP protokolünden
(`put_object` / `get_object` komutlarıyla) hem de isteğe bağlı S3-uyumlu HTTP
kapısından (`OXIDB_S3_PORT`) erişilebilir.

## Sunucuyu ayağa kaldırmak

Kapalı bir motorun maliyeti sıfırdır: açılmayan bir motor için ne dosya açılır,
ne iş parçacığı başlatılır, ne bellek ayrılır. Bu yüzden ikinci ve üçüncü motorlar
varsayılan olarak **kapalıdır**; onları açmak bilinçli bir karardır.

```bash
# OxiTrade borsasının sunucusu: üç motor + iki ek yüzey açık.
export OXIDB_DATA=/var/lib/oxitrade            # tüm veritabanlarının kökü
export OXIDB_ADDR=0.0.0.0:4444                 # ana TCP protokolü (belge + SQL + TSDB)

export OXIDB_SQL=1                             # ikinci motor: SQL  (ADR-0010)
export OXIDB_TSDB=1                            # üçüncü motor: zaman serisi

export OXIDB_OXIMEM_PORT=6380                  # OxiMem: RESP dinleyicisi
export OXIDB_S3_PORT=9000                      # nesne depolama: S3-uyumlu HTTP kapısı

export OXIDB_SLOW_QUERY_MS=50                  # 50 ms'yi aşan komutları profille
export OXIDB_AUDIT=true                        # denetim günlüğü

oxidb-server
```

Bu beş satırın her biri bir yüzeyi açar. `OXIDB_SQL` ve `OXIDB_TSDB`
tanımlanmazsa, sunucu tam olarak eskiden olduğu gibi — sadece belge motoruyla —
çalışır ve o motorların tek bir baytlık disk izi bile olmaz.

## Uygulama: OxiTrade borsası

Şimdi uygulamayı kuralım. Senaryomuz bir kripto borsası: kullanıcılar kayıt olur,
KYC belgelerini yükler, emir verir; borsa fiyat akışını içeri alır, mumları
hesaplar ve gün sonunda rapor üretir.

Tek bir istemci nesnesi, üç motorun da kapısıdır:

```python
import oxidb

db = oxidb.OxiDb(host="127.0.0.1", port=4444)
print(db.ping())                    # 'pong' — sunucu ayakta

# Bu bağlantı üç motoru da görür: belge (varsayılan), SQL, TSDB.
print(db.list_collections())        # belge motoru: koleksiyonlar
print(db.sql("SHOW TABLES"))        # SQL motoru:   tablolar
```

TSDB için Python istemcisinde henüz hazır bir sarmalayıcı yok; tel mesajını
kendimiz kuracağız. Bu, aslında iyi bir şey: motorun protokolünün ne kadar basit
olduğunu doğrudan görürsünüz.

```python
def tsdb(db, **op):
    """TSDB motoruna tek bir istek gönderen ince yardımcı.

    Python istemcisinin ham istek/yanıt kanalını kullanır; tel biçimi
    {"engine": "tsdb", "cmd": "tsdb", "op": ...} kalıbından ibarettir.
    """
    resp = db._request({"engine": "tsdb", "cmd": "tsdb", **op})
    if not resp.get("ok"):
        raise oxidb.OxiDbError(resp.get("error", "bilinmeyen hata"))
    return resp.get("data")
```

### Adım 1 — Kullanıcılar: belge motoru

Kullanıcı kaydı, değişken şemanın klasik örneğidir. Türkiye'deki bir kullanıcının
KYC alanları ile Almanya'daki bir kullanıcınınki aynı değildir; üstelik bu alanlar
zamanla değişir. Belge motoru bunu doğal karşılar:

```python
db.create_collection("users")
db.create_unique_index("users", "email")     # e-posta tekil olsun
db.create_index("users", "status")           # duruma göre filtre hızlansın

db.insert("users", {
    "email": "ada@ornek.com",
    "ad": "Ada Lovelace",
    "status": "kyc_bekliyor",
    "created_at": "2026-07-14T09:00:00Z",
    "kyc": {                                  # iç içe, ülkeye göre değişken alanlar
        "ulke": "TR",
        "tc_kimlik_dogrulandi": False,
        "belgeler": []
    },
    "tercihler": {"dil": "tr", "bildirim": ["email", "push"]}
})

ada = db.find_one("users", {"email": "ada@ornek.com"})
print(ada["_id"], ada["status"])
```

Aynı koleksiyona, `kyc.ulke` alanı `"DE"` olan ve bambaşka alt alanlar taşıyan
bir kullanıcı eklemek için hiçbir şema değişikliği yapmanız gerekmez. Bu esneklik,
belge motorunun tam da vaadidir.

### Adım 2 — Emirler ve defter: SQL motoru

Emirler ve muhasebe defteri ise bunun tam zıddıdır. Şemaları sabittir, sayısal
bütünlükleri kritiktir, ve onlara soracağınız sorular join ve gruplama
içerir. Burası SQL motorunun yeridir.

```sql
-- OxiTrade'in ilişkisel çekirdeği: emirler, gerçekleşen işlemler, defter.
CREATE TABLE orders (
  id        INT PRIMARY KEY,
  user_id   TEXT NOT NULL,          -- belge motorundaki kullanıcının _id'si
  symbol    TEXT NOT NULL,          -- 'BTCTRY', 'ETHTRY', ...
  side      TEXT NOT NULL,          -- 'buy' | 'sell'
  price     DOUBLE NOT NULL,
  qty       DOUBLE NOT NULL,
  status    TEXT NOT NULL,          -- 'open' | 'filled' | 'cancelled'
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE trades (
  id        INT PRIMARY KEY,
  order_id  INT NOT NULL,
  price     DOUBLE NOT NULL,
  qty       DOUBLE NOT NULL,
  fee       DOUBLE NOT NULL,
  ts        TIMESTAMP NOT NULL
);

CREATE INDEX idx_orders_user   ON orders (user_id);
CREATE INDEX idx_orders_symbol ON orders (symbol);
CREATE INDEX idx_trades_order  ON trades (order_id);
```

Bu DDL'i istemciden çalıştırmak tek satırdır; `db.sql()` çağrısı, tel üzerinde
`engine: "sql"` etiketli bir mesaja dönüşür:

```python
DDL = open("schema.sql").read()          # yukarıdaki metnin tamamı
for stmt in DDL.split(";"):
    if stmt.strip():
        db.sql(stmt)                     # her ifade ayrı ayrı çalıştırılır

# Emir vermek: SQL motorunun kendi işlemi içinde, tek atomik birim.
db.sql("BEGIN")
db.sql(
    "INSERT INTO orders (id, user_id, symbol, side, price, qty, status, created_at) "
    "VALUES (?, ?, ?, ?, ?, ?, 'open', NOW())",
    [1001, ada["_id"], "BTCTRY", "buy", 2_450_000.0, 0.05],
)
db.sql("COMMIT")
```

Dikkat edin: `user_id` alanı, belge motorundaki kullanıcının kimliğini taşıyan bir
**metin alanıdır**; bir yabancı anahtar değildir ve olamaz. Motorlar arasında
referans bütünlüğü kısıtı yoktur. Kullanıcı kimliğini SQL tarafına yazarken
uygulamanız, o kimliğin belge tarafında gerçekten var olduğundan emin olmak
zorundadır. Bu, motorları birlikte kullanmanın ilk gerçek bedelidir ve bölümün
sonunda buna döneceğiz.

### Adım 3 — Fiyat akışı: TSDB motoru

Borsanın kalp atışı, fiyat tick'leridir. Saniyede yüzlerce, günde milyonlarca
nokta. Her birini bir belge yapmak israftır; TSDB motoru bunları
Gorilla sıkıştırmasıyla nokta başına yarım bayttan az yerde tutar.

```python
import time

def now_ms():
    return int(time.time() * 1000)

# Borsanın eşleştiricisi her gerçekleşen işlemde bir tick yazar.
tsdb(db, op="write", points=[
    {
        "measurement": "ticks",
        "tags": {"symbol": "BTCTRY", "venue": "oxitrade"},   # seri kimliği
        "fields": {"price": 2_450_000.0, "qty": 0.05},       # sayısal alanlar
        "ts": now_ms(),                                       # epoch ms
    }
])
```

Aynı yazma, tel üzerinde şu mesajdır — istemci sadece bunu sarmalar:

```json
{
  "engine": "tsdb",
  "cmd": "tsdb",
  "op": "write",
  "points": [
    {
      "measurement": "ticks",
      "tags": {"symbol": "BTCTRY", "venue": "oxitrade"},
      "fields": {"price": 2450000.0, "qty": 0.05},
      "ts": 1752480000000
    }
  ]
}
```

Dış borsalardan gelen akışı içeri almak için satır protokolü daha da pratiktir;
Influx ekosisteminin standart biçimini olduğu gibi kabul ederiz:

```python
# Bir dış besleyiciden gelen ham satırlar (InfluxDB line protocol).
lp = "\n".join([
    "ticks,symbol=BTCTRY,venue=binance price=2451300,qty=0.011 %d" % now_ms(),
    "ticks,symbol=ETHTRY,venue=binance price=131450,qty=0.4    %d" % now_ms(),
])
print(tsdb(db, op="write_lp", lp=lp))     # {'written': 2}
```

### Adım 4 — Mumlar: sürekli toplama (rollup)

Bir borsanın grafiği, ham tick'lerden değil **mumlardan** çizilir. Her istekte bir
milyon tick'i yeniden toplamak yerine, tamamlanmış zaman kovalarını bir kez
hesaplayıp türetilmiş bir ölçüme yazarız. TSDB motorunun rollup'ları tam olarak
bunu yapar; üstelik seri başına kalıcı bir su işareti tuttukları için yeniden
başlatmada aynı kovayı iki kez saymazlar.

```python
# 1 dakikalık mum kuralı: 'ticks' ölçümünün her sayısal serisini
# 60_000 ms'lik kovalara toplayıp 'ticks@1m' ölçümüne yaz.
tsdb(db, op="rollup_add",
     measurement="ticks",
     interval=60_000,
     label="1m",
     aggs=["first", "max", "min", "last", "sum", "count"])   # açılış/en yüksek/en düşük/kapanış...

# Kapanmış kovaları işle (bir zamanlayıcıdan periyodik olarak çağrılır).
print(tsdb(db, op="rollup_refresh"))   # {'written': N}  — sadece tamamlanmış kovalar
print(tsdb(db, op="rollups"))          # tanımlı kurallar
```

Rollup, `ticks@1m` adında türetilmiş bir ölçüm üretir; alan adları
`<alan>_<toplama>` kalıbındadır — `price_first`, `price_max`, `price_min`,
`price_last`. Yani klasik OHLCV mumu, doğrudan alan adlarında durur.^[Belge
motorunun toplama işlem hattındaki `$ohlcv` aşaması da aynı işi belge tarafında
yapar. Aynı fikir, iki motorun kendi diliyle ifade edilmiş halidir; hangisini
kullanacağınız, tick'lerin nerede durduğuna bağlıdır.]

```python
# Grafik ekranı: son 6 saatin BTCTRY dakikalık kapanışları.
end = now_ms()
start = end - 6 * 60 * 60 * 1000

kapanislar = tsdb(db, op="query",
                  measurement="ticks@1m",
                  field="price_last",
                  tags={"symbol": "BTCTRY"},
                  start=start, end=end,
                  agg="last")

for seri in kapanislar:
    print(seri["tags"], len(seri["points"]))
    for p in seri["points"][:3]:
        print("  ", p["ts"], p["value"])
```

Ham tick'lere de aynı sorgu diliyle inebiliriz; `interval` verdiğinizde motor,
kovaları epoch hizalı olarak kendisi keser:

```python
# Anlık gösterge paneli: son 1 saatin 5 dakikalık ortalama fiyatı ve p95'i.
end, start = now_ms(), now_ms() - 60 * 60 * 1000

ort = tsdb(db, op="query", measurement="ticks", field="price",
           tags={"symbol": "BTCTRY"}, start=start, end=end,
           interval=5 * 60 * 1000, agg="mean")

p95 = tsdb(db, op="query", measurement="ticks", field="price",
           tags={"symbol": "BTCTRY"}, start=start, end=end,
           interval=5 * 60 * 1000, agg="p95")     # yüzdelikler için kısayol
```

### Adım 5 — Belgeler ve görseller: nesne depolama

KYC için yüklenen kimlik taraması, PDF olarak gelen fatura, ürün görseli —
bunların hiçbiri bir belgenin ya da satırın içinde taşınmamalıdır. Bunlar
**opak bayt yığınlarıdır**; onları kovalara koyar, kayıtta yalnızca anahtarlarını
tutarız.

```python
db.create_bucket("kyc")
db.create_bucket("faturalar")

# Kullanıcının kimlik taramasını yükle.
with open("ada-kimlik.pdf", "rb") as f:
    icerik = f.read()

db.put_object("kyc", f"{ada['_id']}/kimlik.pdf", icerik,
              content_type="application/pdf",
              metadata={"user_id": ada["_id"], "tur": "kimlik"})

# Belge tarafında SADECE referansı tut — baytları değil.
db.update_one("users", {"_id": ada["_id"]}, {
    "$push": {"kyc.belgeler": {"bucket": "kyc",
                               "key": f"{ada['_id']}/kimlik.pdf",
                               "yuklendi": "2026-07-14T09:05:00Z"}},
    "$set":  {"status": "kyc_incelemede"}
})
```

Nesne depolamanın küçük ama etkili bir hediyesi vardır: yüklenen dosyaların
içeriğinden metin çıkarabilir ve o metni aranabilir kılar. Uyum ekibi, "bu
kullanıcının belgelerinde şu ad geçiyor mu?" sorusunu, PDF'i indirmeden sorar:

```python
# PDF'in içindeki metni çıkar (yerleşik çıkarıcılar: PDF, DOCX, HTML, XLSX...)
metin = db.extract_text("kyc", f"{ada['_id']}/kimlik.pdf")
print(metin[:200])

# Tüm kova genelinde tam metin araması.
for hit in db.search("Lovelace", bucket="kyc", limit=5):
    print(hit["bucket"], hit["key"], hit["score"])
```

### Adım 6 — Sıcak durum: OxiMem

Geriye bir tür veri kaldı: **kısa ömürlü, sıcak, kaybolması dert olmayan** durum.
Oturum belirteçleri, saniyedeki istek sayacı, ekranda gösterilen son fiyat. Bunlar
için diske yazan bir motor kullanmak, çiviyi buldozerle çakmaktır. OxiMem, RESP
protokolünü konuştuğu için standart bir Redis istemcisiyle konuşulur:

```python
import redis

mem = redis.Redis(host="127.0.0.1", port=6380, decode_responses=True)
mem.ping()

# 1) Oturum: giriş yapan kullanıcı için 30 dakikalık belirteç.
import secrets
token = secrets.token_urlsafe(24)
mem.setex(f"session:{token}", 1800, ada["_id"])     # TTL ile kendiliğinden silinir

# 2) Hız sınırı: kullanıcı başına dakikada en çok 60 emir.
def emir_verebilir_mi(user_id: str) -> bool:
    anahtar = f"rate:{user_id}:{int(time.time() // 60)}"
    sayac = mem.incr(anahtar)
    if sayac == 1:
        mem.expire(anahtar, 120)        # pencere geçince kendi kendine yok olsun
    return sayac <= 60
```

Sıcak fiyat önbelleği, motorların birlikte çalışmasının en güzel örneğidir: tick
akışı TSDB'ye **kalıcı** olarak yazılırken, aynı tick'in son değeri OxiMem'e
**geçici** olarak yazılır. Web sayfası, saniyede binlerce kez "BTCTRY kaç?" diye
sorduğunda cevabı TSDB'den değil, bellekten alır:

```python
def tick_isle(symbol: str, price: float, qty: float):
    """Bir tick geldiğinde: kalıcı seriye yaz, sıcak önbelleği tazele."""
    ts = now_ms()

    # (a) Kalıcı kayıt — kaynak-doğruluk burada.
    tsdb(db, op="write", points=[{
        "measurement": "ticks",
        "tags": {"symbol": symbol, "venue": "oxitrade"},
        "fields": {"price": price, "qty": qty},
        "ts": ts,
    }])

    # (b) Sıcak önbellek — kaybolursa (a)'dan yeniden üretilebilir.
    mem.hset(f"last:{symbol}", mapping={"price": price, "ts": ts})
    mem.expire(f"last:{symbol}", 300)

def son_fiyat(symbol: str):
    """Önce bellekten sor; yoksa kalıcı seriden tazele (cache-aside)."""
    h = mem.hgetall(f"last:{symbol}")
    if h:
        return float(h["price"])
    seri = tsdb(db, op="query", measurement="ticks", field="price",
                tags={"symbol": symbol},
                start=now_ms() - 60_000, end=now_ms(), agg="last")
    if not seri or not seri[0]["points"]:
        return None
    fiyat = seri[0]["points"][-1]["value"]
    mem.hset(f"last:{symbol}", mapping={"price": fiyat, "ts": now_ms()})
    return fiyat
```

Buradaki hiyerarşi, on üçüncü bölümde tanıdığımız bellek–önbellek–disk
piramidinin uygulama düzeyindeki yankısıdır: **kaynak-doğruluk kalıcı motordadır,
önbellek yalnızca bir hızlandırıcıdır.** OxiMem'i kaybetseniz uygulama yavaşlar
ama yanlış cevap vermez. Bu ayrımı korumak, çok motorlu bir sistemi ayakta tutan
disiplinlerin en önemlisidir.

### Adım 7 — Raporlama: SQL'in evinde

Gün sonu raporu, tam olarak SQL'in var olma sebebidir: iki tabloyu birleştir,
grupla, sırala.

```sql
-- Sembol bazında günlük hacim, komisyon geliri ve ortalama işlem büyüklüğü.
SELECT o.symbol,
       COUNT(*)                    AS islem_sayisi,
       SUM(t.qty * t.price)        AS hacim,
       SUM(t.fee)                  AS komisyon,
       AVG(t.qty * t.price)        AS ortalama_islem,
       COUNT(DISTINCT o.user_id)   AS aktif_kullanici
FROM trades t
JOIN orders o ON o.id = t.order_id
WHERE t.ts >= date_trunc('day', NOW())
GROUP BY o.symbol
HAVING SUM(t.qty * t.price) > 0
ORDER BY hacim DESC;
```

```python
[rapor] = db.sql(open("gunluk_rapor.sql").read())
print(rapor["columns"])                      # ['symbol', 'islem_sayisi', 'hacim', ...]
for satir in rapor["rows"]:
    print(satir)

# Raporun kendisi bir belgedir: geçmişi belge motorunda saklayalım.
db.insert("raporlar", {
    "tur": "gunluk_hacim",
    "tarih": "2026-07-14",
    "kolonlar": rapor["columns"],
    "satirlar": rapor["rows"],           # iç içe dizi — belge motoru için doğal
})
```

Şu son on satır, bölümün ana fikrinin küçük bir özetidir: rapor **SQL'de**
hesaplanır (çünkü join ve gruplama oranın işidir), ama **belge motorunda**
saklanır (çünkü bir raporun şekli değişkendir ve tek parça okunur). Her veri,
kendi sorusunun evine gider.

## Sınırlar: motorlar arası atomiklik yoktur

Şimdi dürüst konuşma vakti. OxiDB'nin üç motoru, tasarım gereği hiçbir durumu
paylaşmaz. Bunun bir güzel sonucu vardır (birbirlerini yavaşlatmazlar,
birbirlerinin hatasından etkilenmezler) ve bir de sert sonucu: **bir belge
koleksiyonu ile bir SQL tablosunu tek bir atomik işlemde güncelleyemezsiniz.**

Her motorun kendi içinde tam atomikliği vardır — belge motorunun üç fazlı OCC
commit'i, SQL motorunun tek fsync'lik toplu WAL kaydı. Ama iki WAL'i kapsayan
bir commit protokolü **henüz yoktur**. ADR-0011 bunu tasarlar (paylaşılan bir
commit saati ve iki fazlı bir commit ile), ama durumu "Önerildi"dir: tasarım var,
uygulama yok. Kitabın yazıldığı sürümde, iki motora yazan bir kod parçası
çöktüğünde birinin uygulanmış, diğerinin uygulanmamış olması mümkündür.

Bu, çözülemez bir sorun değildir; ama **görmezden gelinemez** bir sorundur. Üç
telafi deseni yeter:

**1. Kaynak-doğruluk motorunu seçin.** Her iş gerçeğinin tek bir sahibi olsun.
Emrin gerçeği SQL'dedir; kullanıcının gerçeği belge motorundadır; fiyatın gerçeği
TSDB'dedir. Diğer motorlardaki kopyalar *türev*dir ve kaynaktan yeniden
üretilebilir olmalıdır.

**2. Yazma sırasını, kaynak önce gelecek şekilde kurun ve idempotent yazın.**
Emir SQL'e yazılır (gerçek budur); ardından belge motoruna bir denetim izi
düşülür (türev budur). Aradaki çökme, sadece bir izin eksik kalması demektir —
düzeltilebilir bir durum. Ters sıra ise "olmayan bir emrin izi" üretirdi.

```python
def emir_ver(user_id: str, symbol: str, side: str, price: float, qty: float,
             istemci_ref: str):
    """İki motora yazan bir iş: kaynak SQL, türev belge. Idempotent."""
    if not emir_verebilir_mi(user_id):                 # OxiMem: hız sınırı
        raise RuntimeError("hız sınırı aşıldı")

    # (1) KAYNAK-DOĞRULUK: emrin kendisi SQL motorunda, kendi işlemi içinde.
    db.sql("BEGIN")
    try:
        db.sql(
            "INSERT INTO orders (id, user_id, symbol, side, price, qty, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'open', NOW())",
            [siradaki_id(), user_id, symbol, side, price, qty],
        )
        db.sql("COMMIT")
    except Exception:
        db.sql("ROLLBACK")
        raise

    # (2) TÜREV: denetim izi belge motorunda. Burada çökersek emir yine geçerlidir;
    #     iz eksik kalır ve mutabakat işi onu tamamlar. Aynı istemci referansıyla
    #     ikinci kez yazılırsa üzerine yazar (idempotent) — kopya üretmez.
    db.update_one("audit_orders", {"ref": istemci_ref}, {
        "$set": {"ref": istemci_ref, "user_id": user_id, "symbol": symbol,
                 "side": side, "price": price, "qty": qty,
                 "kaynak": "sql:orders", "ts": now_ms()}
    })
```

**3. Nihai mutabakat (reconciliation) işleri yazın.** Türev tarafın kaynaktan
sapıp sapmadığını periyodik olarak kontrol edin ve düzeltin. Bu, on birinci
bölümdeki nihai tutarlılık fikrinin uygulama düzeyindeki karşılığıdır:

```python
def mutabakat():
    """SQL'deki emirlerle belge motorundaki denetim izlerini karşılaştır.
    Eksik izleri tamamla. Günde bir kez, ya da bir zamanlayıcıyla çalışır."""
    [r] = db.sql(
        "SELECT id, user_id, symbol, side, price, qty FROM orders "
        "WHERE created_at >= NOW() - INTERVAL '1 day'"
    )
    for oid, uid, sym, side, price, qty in r["rows"]:
        ref = f"order:{oid}"
        if db.find_one("audit_orders", {"ref": ref}) is None:
            # Kaynakta var, türevde yok → türevi tamamla (yeniden üretilebilirlik).
            db.insert("audit_orders", {"ref": ref, "user_id": uid, "symbol": sym,
                                       "side": side, "price": price, "qty": qty,
                                       "kaynak": "sql:orders", "onarildi": True})
```

Aynı mantık, nesne depolamaya yüklenen bir dosya ile onun belge tarafındaki
referansı arasında da geçerlidir: **önce nesneyi yükleyin, sonra referansı
yazın.** Ters sıra, var olmayan bir dosyaya işaret eden bir kayıt üretir — ki bu,
sahipsiz kalmış bir nesneden çok daha kötüdür. Sahipsiz nesneler bir temizlik
işiyle toplanabilir; kırık referanslar kullanıcıya hata olarak döner.

Kural cümlesi şudur: **çökme, en fazla bir "fazlalık" bıraksın, asla bir
"eksiklik" bırakmasın.** Fazlalık toplanabilir; eksiklik, veri kaybıdır.

## İşletim: tek sunucu, ayrı ömürler

Motorlar durumu paylaşmadığı için ömürleri de ayrıdır ve bu, işletmeyi hem
kolaylaştırır hem de bir tuzak taşır.

Kolaylaştırır: her motorun bakım işi kendine aittir. TSDB'nin saklama süresini
uygularsınız, blok bazlı olduğu için süresi dolmuş bloklar bütünüyle düşer;
belge motorunun sıkıştırmasını çalıştırırsınız, silinmiş kayıtların yeri geri
alınır. Biri diğerini beklemez.

```python
# Gecelik bakım: her motor kendi işini görür.
kesim = now_ms() - 90 * 24 * 60 * 60 * 1000     # 90 günden eski tick'ler
print(tsdb(db, op="retention", cutoff=kesim))   # {'removed': N} — blok bazında düşer
print(tsdb(db, op="rollup_refresh"))            # kapanan mumları işle
print(tsdb(db, op="checkpoint"))                # bloklar + MANIFEST'i diske sabitle
print(tsdb(db, op="stats"))                     # {'series':.., 'points':.., 'bytes':..}

print(db.compact("audit_orders"))               # belge motoru: yeri geri kazan
```

Tuzak da tam burada: **yedekleme kapsamı**. Belge motorunun `backup()` çağrısı,
belge motorunun dosyalarını yedekler; SQL motorunun WAL'ini ve TSDB'nin bloklarını
değil. Üç motorlu bir sistemi yedeklemenin doğru yolu, veri dizininin tamamını —
tutarlı bir kesitte — almaktır:

```bash
# Doğru yedekleme kapsamı: veri dizininin TAMAMI, tek bir kesitte.
# (Motorların WAL'leri ayrıdır; birini alıp diğerini bırakmak tutarsız bir
#  yedek üretir: SQL'de var olan bir emrin belgesi eksik kalabilir.)
oxidb-cli tsdb checkpoint                      # TSDB'yi sabit bir noktaya çek
rsync -a --delete /var/lib/oxitrade/ /yedek/oxitrade-$(date +%F)/

# Zamanın bir noktasına dönüş (PITR) yalnızca belge motorunu kapsar:
export OXIDB_PITR=1                            # açıksa arşivleyici sealed WAL'leri kopyalar
```

Son bir işletim notu: kapalı motorun sıfır maliyeti, geri dönüşü olan bir karardır.
`OXIDB_TSDB` olmadan başlatılan bir sunucu, TSDB dizinini hiç oluşturmaz; sonradan
açtığınızda motor boş bir durumla doğar. Yani üçüncü motoru, ihtiyacınız olduğu
gün — mevcut verinize dokunmadan — açabilirsiniz. Bu, "her şeyi baştan doğru
seçmek zorundasınız" baskısını ortadan kaldırır; motor seçimi, uygulamanızla
birlikte evrilebilir.

## Bu bölümün bıraktığı yer

Kitabın başında bir soru sormuştuk: veri neye benzer, ve onu nasıl saklamalıyız?
Kısım I bu soruyu kuramsal olarak, Kısım II belge modelinin gözünden, Kısım III de
OxiDB'nin somut motorlarında yanıtladı. Bu bölüm, o yanıtların hepsini tek bir
çalışan sistemde bir araya getirdi: değişken şemalı kullanıcı belge motorunda,
sabit şemalı emir SQL'de, milyonlarca tick sıkıştırılmış zaman serisinde, PDF
nesne kovasında, oturum belleğin içinde — hepsi tek bir sunucu sürecinde, çoğu tek
bir TCP bağlantısında.

Bu birlikteliğin sırrı sofistike bir entegrasyon katmanı değil, tam tersidir:
motorların **birbirine hiç dokunmamasıdır**. Paylaşılmayan durum, bağımsız
ömür, ayrı dosyalar. Bedeli de açıktır ve saklamadık: motorlar arası atomik işlem
yoktur; onu, kaynak-doğruluk seçimi, idempotent yazma ve nihai mutabakat ile
uygulama katmanında telafi edersiniz. ADR-0011 o boşluğu kapatmayı önerir; o gün
geldiğinde, bu bölümün telafi desenlerinin çoğu gereksizleşecek — ama seçim
tablosunun tek bir satırı bile değişmeyecektir. Çünkü hangi verinin hangi motora
gideceği sorusu, bir commit protokolünün değil, verinin şeklinin sorusudur.

Ve bu, aslında bütün kitabın tek cümlelik özetidir: **veriyi, ona soracağınız
soruya benzeyen yere koyun.** Gerisi mühendisliktir.
