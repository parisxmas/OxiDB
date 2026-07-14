# S3 Uyumlu Nesne Depolama: Büyük Baytların Yeri

Yirmi üçüncü bölümde, OxiDB'nin çekirdeğin etrafındaki ek yüzeylerinden söz ederken
blob depolamaya kısaca değinmiştik: büyük ikili nesneler belgelerin içine
gömülmez, kovalar halinde düzenlenmiş ayrı bir nesne deposunda tutulur; her nesne
bir veri dosyası ile onu betimleyen bir üst veri dosyasından oluşur. Orada
kavramı kurduk. Bu bölümde onu sonuna kadar açıyoruz: nesne deposunun disk
üzerindeki düzeni nedir, hangi S3 operasyonları gerçekten uygulanmıştır, kimlik
doğrulama nasıl çalışır, `aws-cli` ve `boto3` gibi hazır araçlar neden hiçbir
uyarlama olmadan çalışır, MinIO ile karşılaştırıldığında ne kadar hızlıdır ve —
en önemlisi — hangi veriyi belgeye, hangisini nesne deposuna koymalısınız.

## Bir veritabanı neden nesne depolar?

Dördüncü ve beşinci bölümlerin ortak dersini hatırlayalım. Belge modeli, birlikte
okunan veriyi birlikte saklamak üzerine kuruludur; bir belge, tek bir okumada
diske bir kez gidilerek getirilen, kendi içinde tutarlı bir birimdir. Bu yapının
verimliliği, belgelerin **küçük ve yapılı** kalmasına bağlıdır. Beşinci bölümde
gördüğümüz ekle-yalnızca depolama, bir belgeyi güncellediğinizde onun yeni bir
kopyasını dosyanın sonuna yazar; eski kopya ölü alan olarak kalır ve yirmi ikinci
bölümdeki sıkıştırma (compaction) onu ancak sonradan temizler.

Şimdi bir megabaytlık bir profil fotoğrafını, o kullanıcının belgesine base64
olarak gömdüğünüzü düşünün. Üç şey aynı anda bozulur. Birincisi, **okuma
maliyeti**: kullanıcının yalnızca adını ve e-postasını isteyen bir sorgu bile, o
bir megabaytı diskten çekip JSON olarak ayrıştırmak zorunda kalır — üstelik base64,
baytları yaklaşık üçte bir oranında şişirir. İkincisi, **yazma büyütmesi**:
kullanıcının son giriş zamanını güncelleyen küçücük bir `$set`, ekle-yalnızca
düzende belgenin **tamamının** — fotoğraf dahil — yeniden yazılmasına yol açar; bir
zaman damgası için bir megabayt. Üçüncüsü, **parçalanma**: her güncelleme bir
megabaytlık ölü alan bırakır, sıkıştırma işi katlanarak artar, önbellek büyük ve
opak baytlarla dolar ve gerçekten sorgulanan sıcak veri önbellekten atılır.

Buradaki asıl gözlem şudur: bir görüntü, bir video ya da bir fatura PDF'i,
veritabanının **hiçbir işine yaramayan** baytlardan oluşur. Veritabanı onları
indeksleyemez, üzerinde karşılaştırma yapamaz, bir toplama işlemine sokamaz. Onlar
için tek yaptığı şey taşımaktır. O halde onları, taşımayı ucuzlatan bir yapıya
koymak gerekir: içeriğe göre sorgulanmayan, anahtarla adreslenen, akış halinde
okunup yazılan, sabit boyutlu bir depo. Bu, tam olarak bir **nesne deposudur**.

OxiDB'nin cevabı, bu ihtiyacı ne dışarıya havale etmek ne de belge motorunun
içine tıkıştırmaktır. Aynı sunucu süreci, ayrı bir dinleyici üzerinde S3 uyumlu
bir nesne depolama API'si sunar. Böylece uygulamanız iki ayrı altyapı bileşeni
işletmek zorunda kalmadan, doğru veriyi doğru motora koyabilir.

## Kova ve nesne modeli

Model, S3'ten devralınmıştır ve sadeliği kasıtlıdır. **Kova** (bucket), bir isim
uzayıdır; adı bir DNS etiketinin kurallarına uyar (küçük harf, rakam, tire; nokta
ve büyük harf yok). **Nesne** (object), bir kova içinde bir **anahtarla** (key)
adreslenen bayt dizisidir. Anahtar, düz bir dizedir; içindeki eğik çizgiler
klasör görüntüsü verir ama gerçek bir dizin ağacı yoktur — `faturalar/2026/07/inv-991.pdf`,
içinde eğik çizgi bulunan tek bir anahtardır. Listeleme sırasında `delimiter`
parametresiyle bu düz isim uzayına bir klasör yanılsaması giydirebilirsiniz; motor
ortak önekleri (`CommonPrefixes`) hesaplayıp döndürür.

Nesnenin kendisi opaktır, ama etrafındaki üst veri zengindir: boyut, içerik türü
(MIME), oluşturulma zamanı, bütünlük damgası (ETag) ve **kullanıcı tanımlı üst
veri** — S3'te `x-amz-meta-*` başlıklarıyla taşınan serbest anahtar/değer çiftleri.

## Disk üzerindeki düzen: `.data` ve `.meta`

Her nesne, veri dizininin altındaki `_blobs/<kova>/` klasöründe **iki dosyaya**
ayrılır: baytları taşıyan `<id>.data` ve onu betimleyen `<id>.meta`. Buradaki `id`,
kova içinde artan bir sayaçtan gelir; anahtar → id eşlemesi bellekte tutulur.
Bu ayrımın nedeni, üst verinin küçük, içeriğin ise büyük olmasıdır: `HEAD` ve
`LIST` gibi yalnızca üst veri isteyen çağrılar, koca `.data` dosyasına hiç
dokunmaz — hatta üst veri bellekteki önbellekten karşılanır ve diske hiç gidilmez.

`.meta` dosyası bir JSON belgesidir ve şuna benzer:

```json
{
  "key": "faturalar/2026/07/inv-991.pdf",
  "bucket": "belgeler",
  "size": 184320,
  "content_type": "application/pdf",
  "etag": "3f2a91c0d4e5b6a7889900aabbccddee",
  "created_at": "2026-07-14T09:12:44Z",
  "metadata": { "musteri": "acme-ltd", "donem": "2026-07" },
  "storage_compression": "zstd",
  "stored_size": 121804,
  "format_version": 1
}
```

Üç alan özel dikkat ister. `etag`, içeriğin SHA-256 özetinin ilk 16 baytının
onaltılık gösterimidir — yani 32 karakterlik bir bütünlük damgası. Nesneyi
indirdikten sonra baytların özetini yeniden hesaplayıp bu damgayla
karşılaştırarak, verinin yolda ya da diskte sessizce bozulmadığını doğrulayabilirsiniz.^[S3'ün
kendi ETag'i tek parçalı yüklemelerde içeriğin MD5'idir; OxiDB SHA-256 kesmesi
kullanır. Damganın anlamı aynıdır — aynı baytlar aynı damgayı üretir — ama
istemci tarafında "ETag == MD5" varsayan kod, doğal olarak, çalışmaz.]
`storage_compression` ve `stored_size`, yirmi üçüncü bölümde değindiğimiz seçici
sıkıştırmanın izidir: metin, JSON ya da HTML gibi sıkışabilir türler zstd ile
sıkıştırılıp saklanır, JPEG/MP4/ZIP gibi zaten doygun-entropili türler olduğu gibi
yazılır. `format_version` ise ileriye dönük emniyet supabıdır: eski bir motor,
tanımadığı daha yeni bir üst veri sürümünü okumayı reddeder.

Yazma yolu, altıncı bölümün dayanıklılık disiplinini izler. Baytlar önce geçici
dosyalara yazılır, sonra yerlerine `rename` edilir — ve sıra önemlidir: **önce
`.data`, sonra `.meta`**. Çünkü kurtarmada gerçeğin kaynağı `.meta` dosyasıdır;
sahipsiz bir `.data` zararsızdır (bir sonraki açılışta süpürülür), ama karşılığı
olmayan bir `.meta` hayalet okumaya yol açardı. `OXIDB_BLOB_SYNC=1` ile bu iki
`rename` arasına dizin `fsync`'i girer ve başarılı bir dönüş, "baytlar diskte"
anlamına gelir.

## Sunucuyu S3 için açmak

Nesne depolama HTTP yüzeyi, isteğe bağlı bir derleme özelliğidir ve varsayılan
olarak kapalıdır; açmadığınızda hiçbir bedeli yoktur.

```bash
# S3 özelliğiyle derle ve S3 dinleyicisini 9000 portunda aç.
# Kimlik bilgileri verilmezse sunucu uyarır ve API herkese açık kalır!
OXIDB_DATA=/var/lib/oxidb \
OXIDB_S3_PORT=9000 \
OXIDB_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE \
OXIDB_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
OXIDB_BLOB_SYNC=1 \
OXIDB_BLOB_COMPRESS=1 \
cargo run --release -p oxidb-server --features s3
```

Birden çok kullanıcıya ayrı anahtar çiftleri vermek isterseniz tek bir değişken
yeter; dururken şifreleme de aynı şekilde açılır:

```bash
# Çok kullanıcılı kimlik bilgileri: "erisim:gizli" çiftleri, virgülle ayrılmış
export OXIDB_S3_CREDENTIALS="app1:s3cret-one,app2:s3cret-two"

# Sunucu tarafı şifreleme (SSE-S3) için 32 baytlık anahtar (onaltılık)
export OXIDB_S3_ENCRYPTION_KEY="$(openssl rand -hex 32)"
# Her nesne, istemci istemese bile şifrelensin:
export OXIDB_S3_DEFAULT_ENCRYPTION=true

# Tarayıcıdan doğrudan erişim için CORS kaynağı (varsayılan: *)
export OXIDB_S3_CORS_ORIGIN="https://uygulamam.example.com"
```

Dinleyici, sabit bir iş parçacığı havuzuyla (256 çalışan, 1024 derinlikte kuyruk)
çalışır, HTTP/1.1 kalıcı bağlantılarını (keep-alive) destekler ve tek bir `PUT`
gövdesinde 5 GiB'a kadar kabul eder.

## Kimlik doğrulama: AWS Signature V4

Uyumluluğun bel kemiği imza şemasıdır. OxiDB, AWS'in **Signature Version 4**
protokolünü doğrular; hem başlık tabanlı imzayı (`Authorization: AWS4-HMAC-SHA256 ...`)
hem de **önceden imzalanmış URL'leri** (`X-Amz-Signature` sorgu parametresiyle,
`X-Amz-Expires` süre sınırı denetlenerek) tanır. Sunucu, istemcinin kanonik
isteğini yeniden kurar, kendi tarafındaki gizli anahtarla imzalama anahtarını
türetir ve iki imzayı **sabit zamanlı** karşılaştırır — zamanlama saldırısına
kapı bırakmadan.

İki pratik sonuç: imza kapsamındaki bölge (region) istemcinin bildirdiği kapsamdan
okunur, dolayısıyla istemci hangi bölgeyi seçerse seçsin, kendi imzasıyla tutarlı
olduğu sürece çalışır. Ve OxiDB **yalnızca yol tarzı** (path-style) adreslemeyi
destekler: `http://sunucu:9000/kova/anahtar`. Sanal konak tarzı (`kova.sunucu`)
yoktur; istemcinizde `force_path_style` benzeri seçeneği açmanız gerekir.

## Hangi S3 operasyonları uygulanmış?

İşte kaynaktan doğrulanmış tam liste:

| Alan | Operasyonlar |
|---|---|
| Servis | `ListBuckets` |
| Kova | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListObjectsV2` (`prefix`, `max-keys`, `delimiter` + `CommonPrefixes`, `continuation-token`) |
| Nesne | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `CopyObject` (kovalar arası; `COPY`/`REPLACE` üst veri yönergesi) |
| Kısmi/koşullu | `Range` (`bytes=a-b`, açık uçlu, sonek), `If-Match` (412), `If-None-Match` (304), kopyada `x-amz-copy-source-if-[none-]match` |
| Çok parçalı | `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload` (≤10.000 parça, ≤5 GiB toplam, 24 saat sonra terk edilenler toplanır) |
| Toplu | `DeleteObjects` (`POST /kova?delete`) |
| Etiketleme | `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging` |
| Yaşam döngüsü | `PutBucketLifecycleConfiguration` / `Get` / `Delete` (yalnızca `Expiration → Days`; 5 dakikada bir süpürücü) |
| Şifreleme | SSE-S3 (`x-amz-server-side-encryption: AES256`), SSE-C (istemci anahtarı) |
| Erişim | SigV4 (başlık + önceden imzalı URL), CORS ön-uçuş (`OPTIONS`) |

Ve dürüst olmak gerekirse, **olmayanlar**: nesne sürümleme (versioning), ACL ve
kova politikaları, `ListMultipartUploads`/`ListParts`, çapraz-bölge replikasyon,
`S3 Select`, sanal konak tarzı adresleme. Bu listeye güvenin; bir istemcinin
"desteklenmiyor" hatası alması, çoğu zaman buradaki bir satırdır.

## Hazır ekosistem: uyumluluğun asıl getirisi

S3 API'sini uygulamanın ödülü, yazılmayan koddur. `aws-cli`, `boto3`, AWS SDK'ları,
MinIO istemcisi, `s3fs`, yedekleme araçları — hepsi hiçbir uyarlama olmadan
çalışır. Önce komut satırı:

```bash
# Kimlik bilgilerini bir profile yaz
aws configure set aws_access_key_id     AKIAIOSFODNN7EXAMPLE      --profile oxidb
aws configure set aws_secret_access_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY --profile oxidb
aws configure set region                us-east-1                 --profile oxidb

E=http://127.0.0.1:9000   # OxiDB S3 uç noktası

aws --endpoint-url $E --profile oxidb s3 mb s3://belgeler          # kova oluştur
aws --endpoint-url $E --profile oxidb s3 ls                         # kovaları listele
```

Yükleme, listeleme, indirme ve silme — yani günlük iş:

```bash
# Yükle (içerik türü otomatik tahmin edilir)
aws --endpoint-url $E --profile oxidb s3 cp fatura.pdf s3://belgeler/faturalar/2026/07/inv-991.pdf

# Önekle listele (anahtarlar düz bir isim uzayıdır; / yalnızca bir karakterdir)
aws --endpoint-url $E --profile oxidb s3 ls s3://belgeler/faturalar/2026/07/

# Bir dizini özyinelemeli yükle (arka planda çok sayıda PutObject)
aws --endpoint-url $E --profile oxidb s3 cp ./raporlar s3://belgeler/raporlar/ --recursive

# İndir ve sil
aws --endpoint-url $E --profile oxidb s3 cp s3://belgeler/faturalar/2026/07/inv-991.pdf ./indirilen.pdf
aws --endpoint-url $E --profile oxidb s3 rm s3://belgeler/faturalar/2026/07/inv-991.pdf
```

Alt seviye `s3api` komutları da geçerlidir; etiketleme ve yaşam döngüsü kuralı
gibi işler buradan yapılır:

```bash
# Nesneye etiket bas (kova genelinde ucuz sınıflandırma)
aws --endpoint-url $E --profile oxidb s3api put-object-tagging \
    --bucket belgeler --key faturalar/2026/07/inv-991.pdf \
    --tagging 'TagSet=[{Key=ortam,Value=uretim},{Key=gizlilik,Value=yuksek}]'

# 90 gün sonra otomatik sil: yaşam döngüsü kuralı (yalnızca Expiration/Days desteklenir)
aws --endpoint-url $E --profile oxidb s3api put-bucket-lifecycle-configuration \
    --bucket gecici --lifecycle-configuration \
    '{"Rules":[{"ID":"tmp","Status":"Enabled","Expiration":{"Days":90}}]}'
```

Ham HTTP de bir seçenektir; `curl` ile imzalayarak konuşmak, protokolün gerçekten
düz bir REST yüzeyi olduğunu görmenin en kısa yoludur:

```bash
# aws-cli'nin imzalayıcısını ödünç al (--request-signer), gövdeyi curl ile gönder
curl -X PUT --data-binary @avatar.png \
     -H "Content-Type: image/png" \
     -H "x-amz-meta-yukleyen: u-42" \
     --aws-sigv4 "aws:amz:us-east-1:s3" \
     --user "AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
     "$E/avatarlar/u-42/asil.png"

# Yanıt başlığındaki ETag, içeriğin bütünlük damgasıdır:
#   ETag: "3f2a91c0d4e5b6a7889900aabbccddee"

# Yalnızca ilk 512 baytı iste (kısmi okuma → 206 Partial Content)
curl -r 0-511 --aws-sigv4 "aws:amz:us-east-1:s3" \
     --user "AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
     "$E/avatarlar/u-42/asil.png" -o ilk-parca.bin
```

## boto3 ile: istemcinin kurulumu ve tam bir tur

Python tarafında tek dikkat edilecek nokta, yol tarzı adresleme ve SigV4'tür:

```python
import boto3, hashlib
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",           # OxiDB S3 uç noktası
    aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
    aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",                    # OxiDB yalnızca SigV4 doğrular
        s3={"addressing_style": "path"},             # sanal konak tarzı YOK
    ),
)
```

Kova oluştur, içerik türü ve kullanıcı üst verisiyle yükle, damgayı doğrula:

```python
s3.create_bucket(Bucket="belgeler")

with open("fatura.pdf", "rb") as f:
    icerik = f.read()

s3.put_object(
    Bucket="belgeler",
    Key="faturalar/2026/07/inv-991.pdf",
    Body=icerik,
    ContentType="application/pdf",
    Metadata={"musteri": "acme-ltd", "donem": "2026-07"},  # → x-amz-meta-*
)

# Üst veriyi indirmeden oku (HEAD: .data dosyasına hiç dokunulmaz)
head = s3.head_object(Bucket="belgeler", Key="faturalar/2026/07/inv-991.pdf")
print(head["ContentLength"], head["ContentType"], head["Metadata"])

# Bütünlük denetimi: indirilen baytların özeti sunucunun ETag'ini vermeli
obj = s3.get_object(Bucket="belgeler", Key="faturalar/2026/07/inv-991.pdf")
baytlar = obj["Body"].read()
beklenen = hashlib.sha256(baytlar).hexdigest()[:32]     # SHA-256'nın ilk 16 baytı
assert beklenen == head["ETag"].strip('"'), "nesne bozulmuş!"
```

Listeleme, önek ve ayraçla; ayraç, düz anahtar uzayına klasör görüntüsü verir:

```python
# Öneke göre listele, sayfa başına en fazla 100 anahtar
r = s3.list_objects_v2(Bucket="belgeler", Prefix="faturalar/2026/", MaxKeys=100)
for o in r.get("Contents", []):
    print(o["Key"], o["Size"], o["ETag"])

# Ayraçla "klasör" görünümü: faturalar/2026/07/, faturalar/2026/08/ ...
r = s3.list_objects_v2(Bucket="belgeler", Prefix="faturalar/2026/", Delimiter="/")
for p in r.get("CommonPrefixes", []):
    print("klasör:", p["Prefix"])

# Sayfalama: NextContinuationToken'ı bir sonraki çağrıya geri ver
token = r.get("NextContinuationToken")
if token:
    r2 = s3.list_objects_v2(Bucket="belgeler", Prefix="faturalar/2026/",
                            ContinuationToken=token)
```

Kısmi ve koşullu okuma — büyük dosyalarda bant genişliği tasarrufunun tamamı
buradadır:

```python
# Yalnızca ilk 1 KiB'i çek: 206 Partial Content
onizleme = s3.get_object(Bucket="belgeler",
                         Key="faturalar/2026/07/inv-991.pdf",
                         Range="bytes=0-1023")["Body"].read()

# Sondan 512 bayt (sonek aralığı da desteklenir)
kuyruk = s3.get_object(Bucket="belgeler",
                       Key="faturalar/2026/07/inv-991.pdf",
                       Range="bytes=-512")["Body"].read()

# Önbellek doğrulama: elimizdeki sürüm hâlâ güncel mi?
etag = head["ETag"]
try:
    s3.get_object(Bucket="belgeler", Key="faturalar/2026/07/inv-991.pdf",
                  IfNoneMatch=etag)      # değişmediyse 304 → gövde indirilmez
except s3.exceptions.ClientError as e:
    if e.response["ResponseMetadata"]["HTTPStatusCode"] == 304:
        print("değişmemiş, yerel kopya geçerli")
```

Büyük dosyalar için çok parçalı yükleme; parçalar bağımsız yüklenir, tamamlanma
anında birleştirilir:

```python
mpu = s3.create_multipart_upload(Bucket="belgeler", Key="videolar/tanitim.mp4",
                                 ContentType="video/mp4",
                                 Metadata={"kaynak": "kamera-3"})
upload_id, parcalar = mpu["UploadId"], []

with open("tanitim.mp4", "rb") as f:
    n = 1
    while (chunk := f.read(8 * 1024 * 1024)):        # 8 MiB'lik parçalar
        r = s3.upload_part(Bucket="belgeler", Key="videolar/tanitim.mp4",
                           UploadId=upload_id, PartNumber=n, Body=chunk)
        parcalar.append({"ETag": r["ETag"], "PartNumber": n})
        n += 1

s3.complete_multipart_upload(Bucket="belgeler", Key="videolar/tanitim.mp4",
                             UploadId=upload_id,
                             MultipartUpload={"Parts": parcalar})
# Vazgeçilirse: s3.abort_multipart_upload(...) — yarım parçalar bellekten silinir.
# Terk edilen yüklemeler 24 saat sonra arka planda zaten toplanır.
```

Ve önceden imzalanmış URL: sunucunun gizli anahtarını istemciye vermeden, süreli
bir indirme (ya da yükleme) bağlantısı üretmek:

```python
# 15 dakika geçerli, doğrudan tarayıcıya verilebilecek indirme bağlantısı
url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": "belgeler", "Key": "faturalar/2026/07/inv-991.pdf"},
    ExpiresIn=900,
)
# URL'nin tek bir karakteri bile oynatılırsa sunucu imzayı reddeder → 403.
```

## Diğer diller: .NET, JavaScript, Go

.NET tarafında `OxiDb.Client.S3` paketi, AWS SDK'sının üzerine doğru varsayılanları
(yol tarzı, SigV4, bölge sabitleme) geçiren ince bir sarmalayıcıdır; geri
döndürdüğü şey standart bir `IAmazonS3`'tür:

```csharp
using OxiDb.Client.S3;
using Amazon.S3.Model;

var s3 = OxiDbS3ClientFactory.Create(new OxiDbS3Options
{
    Endpoint  = "http://127.0.0.1:9000",
    AccessKey = "AKIAIOSFODNN7EXAMPLE",
    SecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    Region    = "us-east-1",
});

await s3.PutBucketAsync("avatarlar");

await s3.PutObjectAsync(new PutObjectRequest
{
    BucketName  = "avatarlar",
    Key         = "u-42/asil.png",
    FilePath    = "avatar.png",
    ContentType = "image/png",
    // Sunucu tarafı şifreleme (OXIDB_S3_ENCRYPTION_KEY ile yönetilen anahtar)
    ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256,
});

var resp = await s3.GetObjectAsync("avatarlar", "u-42/asil.png");
await using var fs = File.Create("indirilen.png");
await resp.ResponseStream.CopyToAsync(fs);   // akış halinde indir
```

JavaScript tarafında ayrı bir paket gerekmez; resmî AWS SDK v3 doğrudan çalışır:

```javascript
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({
  endpoint: "http://127.0.0.1:9000",
  region: "us-east-1",
  forcePathStyle: true,                     // OxiDB yalnızca yol tarzını bilir
  credentials: { accessKeyId: "AKIAIOSFODNN7EXAMPLE",
                 secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" },
});

await s3.send(new PutObjectCommand({
  Bucket: "avatarlar",
  Key: "u-42/asil.png",
  Body: dosyaBaytlari,
  ContentType: "image/png",
  Metadata: { yukleyen: "u-42" },           // → x-amz-meta-yukleyen
}));

const obj = await s3.send(new GetObjectCommand({ Bucket: "avatarlar",
                                                 Key: "u-42/asil.png" }));
const baytlar = await obj.Body.transformToByteArray();
```

Go tarafında da aynı hikâye — kıyaslama koşumumuz zaten AWS SDK for Go v2 ile
yazılmıştır:

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider(ak, sk, "")),
)
// Yol tarzı adresleme + özel uç nokta: OxiDB ve MinIO için aynı istemci kodu
cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
    o.UsePathStyle = true
})

_, err := cli.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String("bench"),
    Key:    aws.String("dosya-0001.bin"),
    Body:   bytes.NewReader(veri),
})
```

## Yerel yüzey: wire komutları

HTTP yüzeyi tek yol değildir. Belge motoruyla konuştuğunuz aynı TCP protokolü
üzerinden de blob deposuna erişebilirsiniz; bu, aynı bağlantı ve aynı oturum
içinde hem belge hem nesne işlemi yapmak istediğinizde kullanışlıdır. İçerik
base64 olarak taşınır:

```json
{"cmd": "create_bucket", "bucket": "avatarlar"}

{"cmd": "put_object", "bucket": "avatarlar", "key": "u-42/asil.png",
 "data": "iVBORw0KGgoAAAANSUhEUgAA...",
 "content_type": "image/png",
 "metadata": {"yukleyen": "u-42", "kaynak": "mobil"}}

{"cmd": "head_object", "bucket": "avatarlar", "key": "u-42/asil.png"}
{"cmd": "list_objects", "bucket": "avatarlar", "prefix": "u-42/", "limit": 50}
{"cmd": "delete_object", "bucket": "avatarlar", "key": "u-42/asil.png"}
```

Python istemcisi bu komutları yöntem olarak sarar — ve buradan, iki motorun
birlikte çalıştığı asıl desene geçebiliriz.

## Asıl desen: belgede üst veri, nesne deposunda baytlar

Şu kuralı bir cümlede söyleyelim: **sorguladığınız her şey belgede, sorgulamadığınız
her şey nesne deposunda.** Bir fatura sisteminde belge motoru "hangi müşterinin,
hangi dönemde, ne kadarlık, hangi durumda faturası var" sorusunu yanıtlar; PDF'in
baytları ise nesne deposunda, belgeden yalnızca kova ve anahtarla işaret edilerek
durur.

```python
import oxidb, boto3
from botocore.config import Config

db = oxidb.Client("127.0.0.1", 4444, username="app", password="...")
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000",
                  aws_access_key_id="app1", aws_secret_access_key="s3cret-one",
                  config=Config(signature_version="s3v4",
                                s3={"addressing_style": "path"}))

def fatura_kaydet(musteri_id, donem, tutar, pdf_baytlari):
    anahtar = f"faturalar/{donem}/{musteri_id}.pdf"

    # 1) Opak baytlar → nesne deposu. Damgayı sunucu üretir.
    meta = s3.put_object(Bucket="belgeler", Key=anahtar, Body=pdf_baytlari,
                         ContentType="application/pdf",
                         Metadata={"musteri": musteri_id, "donem": donem})

    # 2) Sorgulanabilir üst veri → belge motoru. Belge KÜÇÜK kalır.
    db.insert("faturalar", {
        "musteri_id": musteri_id,
        "donem":      donem,
        "tutar":      tutar,
        "durum":      "odenmedi",
        "dosya": {                       # baytlara REFERANS, baytların kendisi değil
            "bucket": "belgeler",
            "key":    anahtar,
            "etag":   meta["ETag"].strip('"'),
            "boyut":  len(pdf_baytlari),
        },
    })

# Sorgu tamamen belge motorunda döner; tek bir PDF baytı diskten okunmaz.
borclular = db.find("faturalar", {"durum": "odenmedi", "tutar": {"$gt": 10000}})

# Kullanıcı bir faturayı gerçekten indirmek istediğinde — ve yalnızca o zaman —
# süreli bir bağlantı üretilir; baytlar sunucu belleğinden hiç geçmez.
def indirme_baglantisi(fatura):
    d = fatura["dosya"]
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": d["bucket"], "Key": d["key"]},
                                     ExpiresIn=900)
```

Desenin kazancı ölçülebilir: `faturalar` koleksiyonundaki belge birkaç yüz bayttır,
tümüyle bellekte durur, indeksleri küçüktür ve "ödenmemiş, 10.000'den büyük"
sorgusu tek bir PDF'e dokunmadan yanıtlanır. Aynı veri belgelere gömülseydi,
koleksiyon gigabaytlara çıkar, her `$set` bir PDF'i yeniden yazar ve önbellek
sorgulanamayan baytlarla dolardı. Avatar örneği de aynıdır: kullanıcı belgesinde
`{"avatar": {"bucket": "avatarlar", "key": "u-42/asil.png", "etag": "..."}}`,
gerçek PNG ise nesne deposunda.

Bir bonus daha var: yirmi üçüncü bölümdeki tam metin arama motoru, nesne
deposundaki dosyaların **içinden** metin çıkarabilir. Yani PDF baytları belge
motorunda değildir, ama içeriği yine de aranabilir:

```python
# Nesne deposundaki bir PDF/DOCX/HTML'den metin çıkar (aynı wire protokolü)
metin = db.extract_text("belgeler", "faturalar/2026-07/acme-ltd.pdf")

# Kova genelinde tam metin arama: TF-IDF puanıyla sıralı sonuçlar
sonuclar = db.search("gecikme faizi", bucket="belgeler", limit=10)
for s in sonuclar:
    print(s["bucket"], s["key"], s["score"])
```

## MinIO ile karşılaştırma

Uyumluluk bir şeydir, performans başka. `tests/s3-benchmark-go` koşumu, aynı Go
programını (AWS SDK v2) hem OxiDB'ye hem MinIO'ya karşı çalıştırır: 5.000 dosya,
her biri 10–250 KB, 100 eşzamanlı goroutine, beş faz. Sonuçlar:

| Faz | OxiDB | MinIO | Oran |
|---|---|---|---|
| Yükleme | 1101 ms, 577 MB/s | 3730 ms, 170 MB/s | 3,39× OxiDB |
| İndirme (+MD5 doğrulama) | 316 ms, 2010 MB/s | 2489 ms, 255 MB/s | 7,88× OxiDB |
| HEAD | 69 ms, 73K op/s | 208 ms, 24K op/s | 3,03× OxiDB |
| Silme | 329 ms, 15K op/s | 321 ms, 16K op/s | 0,98× (berabere) |
| Listeleme | 6,0 ms | 19,5 ms | 3,25× OxiDB |
| **Toplam** | **1814 ms** | **6748 ms** | **3,72× OxiDB** |

Farkın nereden geldiği tesadüf değildir ve tam da bu bölümde anlattığımız tasarım
kararlarının doğrudan sonucudur. İndirmedeki büyük fark, üst verinin bellekte
önbelleklenmesi ve verinin tek bir `.data` dosyasından doğrudan okunmasıdır. HEAD
ve listelemedeki fark daha da nettir: bu çağrılar diske hiç gitmez, bellekteki üst
veri haritasından karşılanır — `.data`/`.meta` ayrımının ödülü budur. Silmedeki
beraberlik ise `.meta`'nın eşzamanlı, `.data`'nın arka planda silinmesinden gelir:
istemci `.meta` kaldırılır kaldırılmaz yanıt alır. Yüklemedeki üstünlük, kilit
tutulan sürenin kısalığındandır — özet alma, sıkıştırma, şifreleme ve geçici dosya
yazımı kova kilidinin **dışında** yapılır; kilit yalnızca kimlik tahsisi ve nihai
kayıt için, milisaniyenin altında tutulur.

Bunlar tek makinede, `tmpfs` üzerinde ölçülmüş sayılardır; MinIO'nun kümelenme,
erasure coding ve sürümleme gibi yetenekleri bu ölçüme hiç girmez. Karşılaştırmanın
söylediği şey şudur: uygulamanızın tipik nesne yükünü — yüz kilobaytlık dosyalar,
yüksek eşzamanlılık — OxiDB'nin nesne deposu, ayrı bir nesne depolama altyapısı
işletmek zorunda kalmadan, en az onun kadar iyi taşır.

## Karar rehberi: ne zaman blob, ne zaman belge?

| Durum | Nereye |
|---|---|
| Üzerinde sorgu, indeks ya da toplama yapılacak alanlar | Belge |
| Birkaç kilobayttan küçük, her okumada zaten gereken veri | Belge |
| Sık güncellenen alanlar (ekle-yalnızca yazma büyütmesi!) | Belge |
| Görüntü, video, ses, PDF, ofis dosyası, yedek arşivi | Nesne deposu |
| İçeriği veritabanı için opak olan her şey | Nesne deposu |
| Akış halinde ya da kısmi (Range) okunacak büyük veri | Nesne deposu |
| Tarayıcıya doğrudan, süreli bağlantıyla verilecek dosya | Nesne deposu |
| Hem sorgulanacak hem büyük olan veri | İkisi: üst veri belgede, baytlar nesnede |

Pratik eşik basittir: belge, birkaç yüz kilobaytı geçmeye başlıyorsa ve büyümenin
kaynağı tek bir opak alansa, o alan bir nesnedir. Ve tersi de doğrudur —
üç yüz baytlık bir küçük resmi (thumbnail) nesne deposuna koymak, her okumada bir
HTTP gidiş-dönüşü eklemekten başka bir işe yaramaz; onu belgede tutun.

## Özet

Bu bölümde, büyük ikili verinin belge motorunda neden yıkıcı olduğunu — okuma
maliyeti, yazma büyütmesi ve parçalanma üçlüsünü — kurduk ve OxiDB'nin buna
verdiği cevabı ayrıntılandırdık: aynı süreç içinde, ayrı bir portta yaşayan,
S3 uyumlu bir nesne deposu. Nesnelerin diskte `.data` ve `.meta` olarak
ayrıldığını, üst verinin bellekte önbelleklendiğini ve bunun HEAD/LIST çağrılarını
diske hiç gitmeden karşıladığını; ETag'in SHA-256 tabanlı bir bütünlük damgası
olduğunu; yazmanın önce `.data` sonra `.meta` sırasıyla dayanıklı hale getirildiğini
gördük. S3 uyumluluğunun asıl getirisinin yazılmayan kod olduğunu — `aws-cli`,
`boto3`, AWS SDK'ları ve önceden imzalı URL'lerin hiçbir uyarlama olmadan
çalıştığını — örnekledik; SigV4 doğrulamasının hem başlık hem sorgu-parametresi
biçimini tanıdığını, adreslemenin yalnızca yol tarzı olduğunu belirttik. MinIO'ya
karşı ölçümde toplam 3,72× üstünlüğün, anlattığımız tasarım kararlarının doğrudan
sonucu olduğunu gösterdik. Ve hepsinden önemlisi, iki motoru birlikte kullanma
desenini kurduk: sorguladığınız her şey belgede, sorgulamadığınız her şey nesne
deposunda — belgede yalnızca kova, anahtar ve damgadan oluşan bir referans.
