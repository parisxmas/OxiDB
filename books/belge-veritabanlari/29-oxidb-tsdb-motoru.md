# Zaman Serisi Motoru: oxidb-tsdb

OxiDB'nin hikâyesini buraya kadar iki motorla anlattık: belgeleri saklayan
çekirdek motor ve onun yanına, kendi dosyalarıyla, kendi tablolarıyla oturan
ilişkisel SQL motoru. Bu bölüm üçüncü bir motoru tanıtıyor: **oxidb-tsdb**, yani
zaman serisi motoru. Diğer ikisi gibi o da ayrı bir sandıkta yaşar — kendi
dizini, kendi dosya biçimi, kendi sorgu yüzeyi vardır — ve varsayılan olarak
**kapalıdır**; açmadığınız sürece tek bayt maliyeti yoktur.

Ama neden üçüncü bir motor? Bir belge veritabanı, sonuçta her şeyi saklayabilir;
bir sıcaklık ölçümünü de bir JSON belgesi olarak yazabilirsiniz. Bu bölümün asıl
savı şudur: **zaman serisi verisi, belge modelinin iyi olduğu her şeyde kötüdür**
ve tam da bu yüzden onun için ayrı bir depolama biçimi kurmak, bir milyon noktayı
yüz megabayt yerine üç yüz kilobayta indirir. Bu, mühendislikte nadiren
karşılaştığımız türden bir kazanç — bir yüzde otuz değil, elli altı kat. Bölümün
yıldızı da bu kazancı sağlayan şey olacak: **Gorilla sıkıştırması**.

## Zaman serisi verisinin doğası

Bir zaman serisi noktası şuna benzer: "cpu ölçümü, host=a etiketiyle,
usage=0.93, zaman damgası 1700000000000". Bir saniye sonra neredeyse aynısı
gelir: "cpu, host=a, usage=0.94, 1700000001000". Bir saniye sonra yine. Bu akış
günlerce, aylarca sürer.

Bu verinin üç ayırt edici özelliği vardır. Birincisi, **şekli hep aynıdır**: aynı
ölçüm adı, aynı etiketler, aynı alan adları, sonsuza kadar. İkincisi, **zaman
damgaları düzenlidir**: bir saniyelik bir toplayıcı, bir saniyelik aralıklarla
yazar; aralar milisaniye düzeyinde sapsa da yapı bozulmaz. Üçüncüsü, **değerler
yavaş değişir**: bir CPU kullanımı, bir sıcaklık, bir kuyruk uzunluğu, ardışık iki
ölçümde tamamen farklı olmaz; çoğu zaman ya aynıdır ya da ondalık basamağın
sonunda oynar. Üstelik veri neredeyse hep **ekleme-sadece**dir: geçmişteki bir
noktayı güncellemek istisnadır; yenisi hep sonuna gelir. Ve sorgular tekil
noktaları değil, **aralıkları ve özetleri** sorar: "son bir saatin dakikalık
ortalaması", "bugünün p95 gecikmesi".

Şimdi bu veriyi belge modeline koyalım ve dördüncü bölümde tanıdığımız o esnek,
şemasız yapının bedelini sayalım. Bir nokta, belge olarak yaklaşık şöyle durur:

```json
{
  "_id": "0f3c...",
  "measurement": "cpu",
  "tags": { "host": "a", "region": "eu" },
  "fields": { "usage": 0.93 },
  "ts": 1700000000000
}
```

Bu belge, JSON olarak yüz bayt civarındadır. Beşinci ve on altıncı bölümlerde
gördüğümüz depolama katmanı bunu bir kaydın içine yazar; her kaydın başında durum
baytı ve uzunluk alanı vardır, WAL'a bir kopyası daha düşer, indeksler kimliği
bir kez daha tutar. Bir milyon nokta, kabaca **yüz megabayt** eder. Oysa aynı
milyon noktanın taşıdığı gerçek bilgi, iki sayıdır: bir zaman damgası (`i64`) ve
bir değer (`f64`) — yani **16 bayt**, toplamda 16 MB. Geri kalan her şey —
"measurement", "tags", "host", alan adları, süslü parantezler — **bir milyon kez
tekrarlanan aynı sabittir**. Belge modeli, şeması değişebilen veriyi taşımak için
her belgeye kendi şemasını iliştirir; zaman serisinde şema hiç değişmediği için
bu, saf israftır.

Sütunsal depolama tam olarak bu israfı ortadan kaldırır. Beşinci bölümde satır
(kayıt) yönelimli depolamayı anlatırken, bir kaydın tüm alanlarının yan yana
durduğunu söylemiştik; sütunsal depolamada ise **aynı alanın ardışık değerleri**
yan yana durur. Alan adı bir kez, akışın kimliğinde yazılır; değerler ise adsız,
tek tip bir dizi olur. Aynı tipteki, birbirine benzeyen değerlerin yan yana
gelmesi ise sıkıştırmanın rüyasıdır.

## Seri kimliği: ölçüm × etiket kümesi × alan

oxidb-tsdb'nin veri modeli InfluxDB'nin modelidir. Bir **nokta** dört parçadan
oluşur: bir **ölçüm** (measurement) adı, sıfır ya da daha çok **etiket** (tag —
hepsi metin), bir ya da daha çok **alan** (field — asıl ölçülen değer) ve bir
milisaniye **zaman damgası**.

Etiketlerle alanlar arasındaki fark, motorun tüm iç yapısını belirler.
**Etiketler kimliktir**: hangi makinenin, hangi bölgenin, hangi sembolün verisi
olduğunu söylerler; sorguda onlara göre filtreleyip gruplarsınız. **Alanlar
veridir**: ölçülen sayının kendisi. Motor bu ayrımı radikal biçimde uygular:

> Bir seri = ölçüm × (sıralı) etiket kümesi × alan adı.

Yani `cpu,host=a,region=eu usage=0.9,temp=51` biçimindeki tek bir nokta, iki alan
taşıdığı için **iki ayrı seriye** dağılır: `cpu/host=a,region=eu/usage` ve
`cpu/host=a,region=eu/temp`. Her seri, kendi başına bir sütunsal akıştır: yalnızca
`(zaman, değer)` ikilileri. Etiketler sıralı tutulur ki aynı etiket kümesi, hangi
sırayla yazılırsa yazılsın, aynı seriye düşsün.

Bu ayrıştırmanın bedeli de vardır ve dürüst olalım: etiketlerden birine yüksek
kardinaliteli bir değer (örneğin kullanıcı kimliği) koyarsanız, milyonlarca seri
yaratırsınız; her seri kendi tamponunu ve bloklarını taşıdığı için bu, belleği
şişirir. Zaman serisi dünyasının klasik günahıdır bu; etiketler **sınırlı** bir
değer kümesinden gelmelidir.

## Gorilla: bir noktayı iki bite indirmek

Sıkıştırmanın kalbi `gorilla.rs`'te durur ve adını, Facebook'un 2015'teki bellek
içi zaman serisi veritabanından alır.^[T. Pelkonen ve ark., "Gorilla: A Fast,
Scalable, In-Memory Time Series Database," *Proceedings of the VLDB Endowment*
8(12), 2015.] Fikir iki bacaklıdır: zaman damgalarını **delta'nın delta'sıyla**,
değerleri ise **XOR** ile kodlamak.

### Zaman damgaları: delta-of-delta

Bir saniyelik toplayıcının zaman damgalarını düşünün:

```text
t:    1700000000000, 1700000001000, 1700000002000, 1700000003000
delta:                        1000,          1000,          1000
delta-of-delta:                  -,             0,             0
```

Ardışık farklar (delta) hep 1000'dir; farkların farkı (delta-of-delta) ise
**sıfır**. Sıfırı kodlamak için tek bir bit yeter. Motor tam da bunu yapar:
`dod == 0` ise akışa tek bir `0` biti yazılır. Sıfır değilse, değişken genişlikte
kovalara düşer — küçük sapmalar `10` öneki + 7 bit, biraz büyükleri `110` + 9 bit,
daha da büyükleri `1110` + 12 bit, geri kalan her şey `1111` + tam 64 bit. Yani
düzenli bir akış noktayı **1 bite**, ufak seğirmeleri (bir toplayıcının 3 ms geç
kalması gibi) 9 bite indirir; yalnızca gerçekten düzensiz sıçramalar tam bedeli
öder.

### Değerler: XOR

Kayan noktalı sayılar için hile daha zariftir. İki ardışık `f64` değerinin bit
desenlerini XOR'larsınız:

- Değer **hiç değişmediyse** XOR sıfırdır → akışa tek bir `0` biti yazılır. Bir
  milyon kez aynı sıcaklığı yazarsanız, milyon nokta bir megabit bile etmez.
- Değiştiyse, XOR sonucunun **anlamlı penceresi** (baştaki sıfırlardan sonrası,
  sondaki sıfırlardan öncesi) saklanır. Benzer büyüklükteki sayılar üsteli
  paylaştığı için XOR'un yalnızca son birkaç biti yanar; pencere dardır.
- Üstelik ardışık değerlerin pencereleri de birbirine benzediği için, motor
  önceki pencereyi yeniden kullanabiliyorsa (`lead` ve `trail` en az öncekiler
  kadarsa) pencere tanımını **hiç yazmaz**: `1` (değişti) + `0` (aynı pencere) +
  anlamlı bitler.

Bir blok kendi kendini betimler: `[u32 sayı][i64 t0][f64 v0][bit akışı]` — yani
20 baytlık bir başlık ve ardından bit düzeyinde paketlenmiş gövde. `bits.rs`
içindeki `BitWriter`/`BitReader` de zaten bunun için vardır: bayt sınırlarını
umursamadan tek tek bit yazıp okumak.

### Somut bit hesabı

Gerçekçi bir gösterge (gauge) metriğini alalım: saniyede bir örnek, değer
yuvarlanmış olduğu için bir süre sabit kalıyor, sonra bir basamak oynuyor. Bir
noktanın maliyeti:

| Bileşen | En iyi hâl | Tipik hâl |
|---|---|---|
| Zaman damgası (dod = 0) | 1 bit | 1 bit |
| Değer değişmemiş (XOR = 0) | 1 bit | — |
| Değer değişmiş, pencere yeniden kullanılıyor | — | 1 + 1 + ~10 bit |
| **Nokta başına** | **2 bit** | **~13 bit** |

Değerlerin dörtte üçünün sabit kaldığı gerçekçi bir akışta ortalama, nokta başına
2–3 bit civarında kalır; buna 1024 noktalık blok başına 20 baytlık başlığı
eklerseniz (nokta başına 0,16 bit) tablo değişmez. Ölçülen sonuç: **~0,28
bayt/nokta**. Karşılaştırma tablosu, bir milyon nokta için:

| Biçim | Nokta başına | 1M nokta |
|---|---|---|
| Belge (JSON, etiketler her belgede) | ~100 bayt | ~100 MB |
| Ham ikili (`i64` + `f64`) | 16 bayt | 16 MB |
| Gorilla (oxidb-tsdb) | ~0,28 bayt | **~280 KB** |

Ham ikiliye göre yaklaşık **56 kat**, belge biçimine göre iki kat daha fazla.
On üçüncü bölümde bellek/disk ödünleşimini anlatırken "çalışma kümesini belleğe
sığdırmak" demiştik; sıkıştırma, o kümeyi büyütmenin en ucuz yoludur. Bir yıllık
saniyelik metrik (31,5 milyon nokta), Gorilla ile bir seri başına ~9 MB'tır —
belleğe rahatça sığar.

Motorun testleri bu iddiayı doğrudan sınar: `oxidb-tsdb/tests/e2e.rs` içindeki
`compression_ratio_and_retention`, 100.000 noktalık gerçekçi bir gösterge yazar
ve sıkıştırılmış boyutun ham boyutun sekizde birinden küçük olmasını (yani en az
8×) şart koşar; pratikte gelen oran çok daha yüksektir.

## Bloklar, aktif tampon ve saklama

Bir seri iki parçadan oluşur: **mühürlenmiş bloklar** ve bir **aktif tampon**.
Gelen nokta önce tampona düşer (basit bir `(i64, f64)` vektörü). Tampon eşiğe
(varsayılan 1024 nokta) ulaşınca **mühürlenir**: içindekiler zamana göre
sıralanır, Gorilla ile kodlanır ve `min_ts`, `max_ts`, `count` bilgileriyle
birlikte bir bloğa dönüşür. Bloklar bir daha değişmez.

Bu tasarımın iki güzel sonucu vardır. Birincisi **sorgu budaması**: bir aralık
sorgusu, `[start, end)` ile kesişmeyen blokları hiç açmaz — `max_ts < start` ya
da `min_ts >= end` ise blok es geçilir, tek bir bit bile çözülmez. İkincisi
**saklamanın (retention) bedavaya gelmesi**: süresi dolan veriyi silmek, nokta
nokta bir yeniden yazma değil, **bütün bir bloğu listeden düşürmektir**. `max_ts`
değeri kesim noktasından eskiyse blok tümüyle atılır; hiçbir şey çözülmez, hiçbir
şey yeniden kodlanmaz. Zaman serisi motorlarının bloklu depolamayı sevmesinin asıl
nedeni budur.

Bu bloklu yapı sıkıştırma oranıyla saklama inceliği arasında bir ödünleşim
kurar: blok ne kadar büyükse sıkıştırma o kadar iyi, ama saklamanın ve budamanın
çözünürlüğü o kadar kaba olur. `with_block_points` bu ayarı açar; varsayılan 1024
makul bir orta noktadır.

## Kalıcılık: MANIFEST tek commit noktasıdır

Altıncı ve on yedinci bölümlerde WAL'ın temel sözleşmesini kurmuştuk: veriyi
kalıcı yapıya işlemeden önce niyeti günlüğe yaz. TSDB motoru aynı sözleşmeyi,
ama bloklu depolamaya uygun bir biçimde uygular. `persist.rs`, üç dosya tipiyle
çalışır:

```text
<dir>/MANIFEST            # {"generation": N} — yetkili nesil numarası
<dir>/blocks.<N>.tsb      # N. kontrol noktasındaki tam blok anlık görüntüsü
<dir>/wal.<N>.log         # N'den sonra yazılan noktalar
```

Her nokta önce WAL'a eklenir; sonra bellekteki seriye girer. **Kontrol noktası
(checkpoint)** şu sırayla işler: aktif tamponlar mühürlenir → `blocks.<N+1>.tsb`
yazılır ve fsync edilir → MANIFEST, geçici dosya + `rename` ile atomik olarak
N+1'e çevrilir → yeni `wal.<N+1>` açılır ve N nesline ait dosyalar silinir.

Buradaki tüm dayanıklılık argümanı **tek bir rename**'in iki yanında saklıdır.
Çökme rename'den **önce** olursa, MANIFEST hâlâ N der; kurtarma eski anlık
görüntüyü ve onun WAL'ını okur — o WAL, kontrol noktasından sonraki her noktayı
hâlâ içerdiği için hiçbir şey kaybolmaz. Çökme rename'den **sonra** olursa,
MANIFEST N+1 der; kurtarma yeni anlık görüntüyü okur ve eski WAL'a hiç bakmaz —
dolayısıyla hiçbir nokta **iki kez sayılmaz**. Arada bir üçüncü durum yoktur;
çünkü rename atomiktir. Saklama da bedavaya kalıcı olur: düşürülen bloklar,
bir sonraki anlık görüntüde zaten yoktur.

WAL 8 MiB'ı geçtiğinde motor kendiliğinden bir kontrol noktası alır; `checkpoint`
komutuyla elle de tetikleyebilirsiniz.

## Tipli alanlar

Alanlar tiplidir: **float**, **integer**, **boolean** ve **string**. Sayısal
üçlü aynı Gorilla yolundan geçer — hepsi `f64` olarak saklanır (tamsayılar 2^53'e
kadar tam, mantıksal değerler 0/1) — ama tip seri bazında hatırlanır ve sorgu
sonucunda `"type"` alanıyla geri bildirilir. Metin alanları ise ayrı bir yolda,
`(zaman, metin)` çiftleri olarak yaşar; onlarda anlamlı toplamalar `first`,
`last`, `count` ve `distinct`tır.

## Motoru açmak

Motor varsayılan olarak kapalıdır. İki ortam değişkeni yeter:

```bash
# TSDB motorunu aç; veri ${OXIDB_DATA}/tsdb altına yazılır.
export OXIDB_TSDB=1
export OXIDB_DATA=./oxidb_data
# İsteğe bağlı: varsayılan veritabanının TSDB dizinini başka yere al.
export OXIDB_TSDB_DATA=/var/lib/oxidb/tsdb
oxidb-server
```

SQL motoru gibi TSDB de **veritabanı başınadır**: `oxidb` varsayılan veritabanı
`${OXIDB_DATA}/tsdb`, adlandırılmış `metrics` veritabanı ise
`${OXIDB_DATA}/metrics/tsdb` altında yaşar.

Tel protokolü, yirmi dördüncü bölümdeki OxiWire'ın aynısıdır — uzunluk önekli
JSON — yalnızca isteğe bir `engine` alanı eklenir. Tüm TSDB istekleri `cmd:
"tsdb"` altında toplanır ve bir `op` alanı eylemi seçer: `write`, `write_lp`,
`query`, `stats`, `retention`, `checkpoint`, `rollup_add`, `rollup_refresh`,
`rollups`. RBAC açısından `tsdb` komutu ReadWrite rolüyle korunur; Read rolü
yalnızca sorgu çalıştırabilir.

## Yazmak

En basit hâl — tek bir nokta:

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "write",
  "points": [
    { "measurement": "cpu",
      "tags":   { "host": "a", "region": "eu" },
      "fields": { "usage": 0.93 },
      "ts": 1700000000000 }
  ]
}
```

Yanıt: `{"ok": true, "data": {"written": 1}}`.

Python istemcisinin henüz özel bir TSDB yardımcısı yok; ama alttaki istek
işlevini kullanmak yeterlidir. Küçük bir sarmalayıcı, bölümün geri kalanındaki
tüm örnekleri okunur kılar:

```python
from oxidb import OxiDbClient

db = OxiDbClient(host="127.0.0.1", port=4444)

def tsdb(op, **kw):
    """TSDB motoruna bir istek gönder; hata olursa istisna fırlatır."""
    return db._checked({"engine": "tsdb", "cmd": "tsdb", "op": op, **kw})

tsdb("write", points=[{
    "measurement": "cpu",
    "tags": {"host": "a", "region": "eu"},
    "fields": {"usage": 0.93},
    "ts": 1700000000000,
}])
```

Toplu yazma, aynı isteğe birden çok nokta koymaktır — bir noktada birden çok alan
da olabilir; her alan ayrı bir seriye gider. Alanların JSON tipi, saklama tipini
belirler: mantıksal değer → boolean, tam sayı → integer, metin → string, geri
kalan → float.

```python
base = 1_700_000_000_000
points = []
for i in range(600):                      # 10 dakikalık 1 sn'lik örnekler
    points.append({
        "measurement": "cpu",
        "tags": {"host": "a", "region": "eu"},
        "fields": {
            "usage":  round(0.4 + 0.1 * (i % 7), 2),  # float
            "cores":  8,                              # integer
            "up":     True,                           # boolean
            "state":  "healthy",                      # string (metin yolu)
        },
        "ts": base + i * 1000,
    })
print(tsdb("write", points=points))       # {'written': 600}
```

### InfluxDB satır protokolü

Metrik dünyasının ortak dili, InfluxDB'nin **satır protokolüdür** (line
protocol). Telegraf'tan collectd'ye kadar pek çok toplayıcı bu biçimde konuşur;
motor onu doğrudan yutar. Biçim:

```text
ölçüm[,etiket=değer,...] alan=değer[,alan=değer,...] [zaman_damgası]
```

Alan değerlerinin tipi son ekle belirlenir: `1.5` float, `10i` integer,
`t`/`true`/`f`/`false` boolean, `"..."` metin. Zaman damgası **milisaniyedir**;
yoksa sunucunun o anki saati kullanılır. Gerçek bir satır kümesi:

```text
cpu,host=a,region=eu usage=0.9,cores=8i,up=true 1700000000000
cpu,host=b,region=us usage=0.4,cores=16i,up=true 1700000000000
mem,host=a free=1024i 1700000000000
weather,location=us\ midwest temp=82
```

(Son satırda hem kaçışlı boşluk hem de zaman damgasının yokluğu var: "şimdi"
kabul edilir.) Bunu tek bir istekle göndermek:

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "write_lp",
  "lp": "cpu,host=a usage=0.9 1700000000000\ncpu,host=b usage=0.4 1700000000000"
}
```

Python'dan, bir toplayıcının çıktısını olduğu gibi aktarmak:

```python
lines = "\n".join(
    f"cpu,host={h} usage={u} {base + i*1000}"
    for i, (h, u) in enumerate([("a", 0.91), ("b", 0.42), ("a", 0.93)])
)
tsdb("write_lp", lp=lines)   # {'written': 3}
```

Aynı isteği ham TCP üzerinden, hiçbir istemci kütüphanesi olmadan da
gönderebilirsiniz; protokol, küçük-endian 4 baytlık uzunluk öneki + JSON'dur:

```javascript
import net from "node:net";

const sock = net.connect(4444, "127.0.0.1", () => {
  const body = Buffer.from(JSON.stringify({
    engine: "tsdb", cmd: "tsdb", op: "write_lp",
    lp: "cpu,host=a usage=0.97 1700000600000",
  }));
  const len = Buffer.alloc(4);
  len.writeUInt32LE(body.length);        // uzunluk öneki: küçük-endian u32
  sock.write(Buffer.concat([len, body]));
});
sock.on("data", (buf) => {
  console.log(buf.subarray(4).toString()); // {"ok":true,"data":{"written":1}}
  sock.end();
});
```

## Sorgulamak

Bir sorgu şunları söyler: hangi ölçüm, hangi alan, hangi etiket filtreleri, hangi
yarı-açık zaman aralığı `[start, end)`, isteğe bağlı olarak hangi genişlikte
zaman kovaları (`interval`), hangi etiketlere göre gruplama (`group_by`) ve hangi
toplama (`agg`).

En yalın hâli — bir saatlik pencerede tek bir ortalama:

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "query",
  "measurement": "cpu", "field": "usage",
  "tags": { "host": "a" },
  "start": 1700000000000, "end": 1700003600000,
  "agg": "mean"
}
```

Yanıtın gövdesi her zaman bir **grup listesidir**; her grubun etiketleri, alanın
tipi ve noktaları vardır:

```json
[
  { "tags": {}, "type": "float",
    "points": [ { "ts": 1700000000000, "value": 0.72 } ] }
]
```

`interval` verilmediğinde tüm aralık tek bir kovadır ve kovanın zaman damgası
`start` olur. Verildiğinde ise **downsample** devreye girer: kovalar epoch'a
hizalıdır (InfluxDB'deki gibi), yani 60.000 ms'lik kovalar tam dakika sınırlarına
oturur.

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "query",
  "measurement": "cpu", "field": "usage",
  "start": 1700000000000, "end": 1700003600000,
  "interval": 600000,
  "agg": "mean"
}
```

Bu, bir saati **on dakikalık altı ortalamaya** indirir — bir grafiğe çizilecek
şey tam olarak budur. Motorun `e2e.rs` testi de bu davranışı sabitler: bir saatlik
saniyelik veri, `interval = 600000` ile tam altı nokta döndürür.

Etiketlere göre gruplamak, tek sorguda birden çok çizgi üretir:

```python
res = tsdb("query",
           measurement="cpu", field="usage",
           start=base, end=base + 3_600_000,
           interval=60_000,          # dakikalık kovalar
           group_by=["host"],        # her host için ayrı çizgi
           agg="mean")
for series in res:
    host = series["tags"]["host"]
    print(host, series["type"], len(series["points"]), "kova")
```

Filtre (`tags`) ile gruplamayı (`group_by`) karıştırmamak gerekir: birincisi
serileri **eler**, ikincisi çıktıyı **böler**. İkisi birlikte de kullanılabilir —
"yalnızca eu bölgesi, ama host'a göre ayrı çizgiler" gibi.

### Toplama türleri

| Toplama | Anlamı |
|---|---|
| `mean` / `avg` | Kovadaki değerlerin ortalaması (varsayılan) |
| `sum` | Toplam |
| `min` / `max` | En küçük / en büyük |
| `count` | Nokta sayısı |
| `first` / `last` | Kovanın en eski / en yeni değeri |
| `distinct` | Kovadaki farklı değer sayısı |
| `rate` | Saniyedeki değişim: `(last - first) / süre_sn` — sayaçlar için |
| `percentile` (+ `p`) | `p`. yüzdelik; doğrusal aradeğerleme |
| `p95`, `p99`, `p50`… | `percentile` için kısayol |

Yüzdelikler, gecikme ölçümlerinin dilidir; ortalama gecikme yalan söyler, p95
söylemez:

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "query",
  "measurement": "http", "field": "latency_ms",
  "start": 1700000000000, "end": 1700003600000,
  "interval": 60000,
  "agg": "p95"
}
```

Aynı sorgu, uzun yazımıyla `"agg": "percentile", "p": 99.9` biçiminde de
yazılabilir. `rate` ise **sayaç** (monoton artan) serileri içindir: bir kovadaki
ilk ve son değerin farkını, aradaki süreye böler — "saniyede kaç istek" sorusunun
doğru cevabı budur:

```python
# İstek sayacı: saniyede kaç istek (dakikalık kovalarla)
rps = tsdb("query",
           measurement="http", field="requests_total",
           start=base, end=base + 3_600_000,
           interval=60_000, agg="rate")
```

Metin alanları da aynı sorgu yüzeyinden geçer; motor alanın metin olduğunu kendi
anlar ve `first`/`last`/`count`/`distinct` toplamalarını uygular:

```python
# Bir saatte kaç farklı durum görüldü? ("healthy", "degraded", ...)
tsdb("query", measurement="cpu", field="state",
     start=base, end=base + 3_600_000, agg="distinct")
# -> [{"tags": {}, "type": "string", "points": [{"ts": ..., "value": 3.0}]}]
```

## Saklama ve kontrol noktası

Saklama, tek bir kesim zamanıyla çalışır: bu andan eski **bloklar** düşürülür.

```python
import time
cutoff = int(time.time() * 1000) - 7 * 86_400_000   # 7 günden eskisi gitsin
print(tsdb("retention", cutoff=cutoff))              # {'removed': 6_048_000}

print(tsdb("stats"))
# {'series': 42, 'points': 3_601_234, 'bytes': 1_012_448}
```

`stats` üç sayı verir: seri sayısı, nokta sayısı ve sıkıştırılmış bayt. Bu üçünün
oranı, bölümün başındaki iddianın canlı kanıtıdır — noktaları bayta bölün,
0,3'e yakın bir sayı çıkacaktır (aktif tampondaki, henüz mühürlenmemiş noktalar
ham 16 bayt sayıldığı için, taze yazılmış bir veritabanında oran biraz daha
yüksek görünür; bir `checkpoint` sonrası gerçek değerine oturur).

```python
tsdb("checkpoint")   # aktif tamponları mühürle, anlık görüntü al, WAL'ı döndür
```

## Sürekli toplama: rollup

Bir yıllık saniyelik veriyi saklamak ucuzdur, ama her grafik çizişinde otuz bir
milyon noktayı taramak değildir. Zaman serisi dünyasının standart çözümü
**sürekli toplamadır** (continuous aggregate): ham veriyi bir kez tarayıp,
kapanmış zaman kovalarının özetini kalıcı olarak yazmak; grafikler artık özeti
okur.

oxidb-tsdb'de bir rollup kuralı şunu söyler: "`cpu` ölçümünün her sayısal
serisini, 60.000 ms'lik kovalarda `mean`, `max` ve `count` olarak topla."
Sonuç, türetilmiş bir ölçümdür: adı `<ölçüm>@<etiket>`, alanları
`<alan>_<toplama>`.

```json
{
  "engine": "tsdb", "cmd": "tsdb", "op": "rollup_add",
  "measurement": "cpu", "label": "1m", "interval": 60000,
  "aggs": ["mean", "max", "count"]
}
```

Bu kural, `cpu` ölçümündeki `usage` alanından `cpu@1m` ölçümünde `usage_mean`,
`usage_max`, `usage_count` alanlarını üretir — kaynak serinin etiketlerini
(host, region…) aynen taşıyarak. Kural `rollups.json`'a yazılır, yani yeniden
başlatmayı atlatır.

Kuralı çalıştırmak ayrı bir adımdır:

```python
tsdb("rollup_add", measurement="cpu", label="1m",
     interval=60_000, aggs=["mean", "max", "count"])

written = tsdb("rollup_refresh")     # "now" verilmezse sunucu saati
print(written)                       # {'written': 60}

print(tsdb("rollups"))
# [{'measurement': 'cpu', 'label': '1m', 'interval': 60000}]
```

`rollup_refresh` yalnızca **tümüyle kapanmış** kovaları işler; içinde bulunulan
dakikanın yarısı henüz gelmemiş olabilir, onu yazmak yanlış olurdu. Artımlılığı
ise bir **su işareti** (watermark) sağlar: her (seri, aralık) çifti için son
materyalize edilen kova başlangıcı kalıcı olarak tutulur. Yeniden başlatmadan
sonra yenileme çağırırsanız, motor kaldığı yerden devam eder — ve `rollup.rs`
testinin sabitlediği gibi, aynı kovayı **iki kez yazmaz**. Bu, sürekli toplamanın
en sinsi hatasıdır (çöküş sonrası çift sayım) ve su işareti tam olarak ona karşı
vardır.

Özet artık sıradan bir ölçümdür; onu da normal bir sorguyla okursunuz:

```python
tsdb("query", measurement="cpu@1m", field="usage_mean",
     start=base, end=base + 86_400_000,
     interval=3_600_000, group_by=["host"], agg="mean")
```

Buradaki incelik şu: `cpu@1m` üzerinde saatlik ortalama almak, dakikalık
ortalamaların ortalamasıdır — kovalar eşit doluysa doğru, değilse hafifçe
kaymıştır. Kesinlik gerekiyorsa `usage_sum` ve `usage_count` alanlarını da
materyalize edip oranı kendiniz kurun. Zincirleme (1s → 1m → 1h) motorda kendi
kendine olmaz; kuralları arka arkaya tanımlayıp sırayla yenilemek çağıranın
işidir.

## Gerçek kullanım: tick'ten muma

Kitabın yirminci bölümünde, belge motorunun toplama hattındaki `$ohlcv` aşamasıyla
tick verisini muma çevirmiştik. TSDB motoru aynı işi depolama tarafından çözer:
tick'ler ham seri, mumlar rollup'tır.

```python
# 1) Borsa akışını satır protokolüyle akıt (fiyat + hacim).
lp = "\n".join(
    f"trades,symbol=BTCUSDT price={p},qty={q} {ts}"
    for ts, p, q in stream_of_trades()      # (epoch_ms, fiyat, miktar)
)
tsdb("write_lp", lp=lp)

# 2) Dakikalık mum kuralı: açılış/en yüksek/en düşük/kapanış = first/max/min/last
tsdb("rollup_add", measurement="trades", label="1m", interval=60_000,
     aggs=["first", "max", "min", "last", "count"])
tsdb("rollup_refresh")

# 3) Mumları oku: trades@1m ölçümünde price_first = açılış, price_last = kapanış
candles = tsdb("query", measurement="trades@1m", field="price_last",
               tags={"symbol": "BTCUSDT"},
               start=base, end=base + 86_400_000,
               interval=60_000, agg="last")
```

Hacim (`qty_count` yerine gerçek toplam) istiyorsanız kural listesine `sum`
eklemeniz yeter: `qty_sum` alanı doğrudan mumun hacmi olur.

Sunucu metrikleri tarafında da kalıp aynıdır: Telegraf'ın satır protokolü akışını
`write_lp` ile içeri alın, panoların çizdiği çözünürlükte (`1m`, `5m`) bir rollup
tanımlayın, ham veriyi yedi günlük bir `retention` ile budayın, özeti aylarca
tutun. Ham veri pahalı olan değil — pahalı olan onu **her sorguda yeniden
taramaktır**.

## Sınırlar ve motorun yeri

Dürüst bir kapanış için sınırları da yazalım. TSDB motoru **düğüm-yereldir**:
Raft ile çoğaltılmaz. Etiket filtreleri yalnızca eşitliktir (regex ya da `OR`
yoktur). Aralıklar milisaniye cinsindendir ve takvim birimleri (ay, yıl) yoktur
— tıpkı belge motorunun `$densify` aşamasında olduğu gibi, sabit süreli birimlerle
çalışılır. Metin alanları henüz sıkıştırılmaz; ham `(zaman, metin)` çiftleri
olarak yaşarlar. Ve kardinalite disiplini çağıranın sorumluluğundadır.

Yine de, bu bölümün asıl dersi mimaridir: OxiDB, "her veri için tek bir depolama"
demek yerine, verinin şekli radikal biçimde farklılaştığında **onun için ayrı bir
motor** açtı. Belge motoru esnek şemalı veriyi, SQL motoru ilişkisel veriyi, TSDB
motoru da düzenli, ekleme-sadece, sonsuz akan ölçümleri saklıyor — ve her biri,
diğerlerinin dosyalarına dokunmadan, aynı sunucunun aynı bağlantısı üzerinden
konuşuyor. On beşinci bölümde "tek çekirdek, çok yüz" demiştik; bu bölümden sonra
söylemesi gereken bir cümle daha var: **tek sunucu, çok motor.**
