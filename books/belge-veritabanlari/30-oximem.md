# OxiMem: Redis'in Telini Konuşan Bellek-İçi Anahtar-Değer Katmanı

Bir veritabanı motorunun içinde neden bir anahtar-değer katmanı bulunsun? On
üçüncü bölümde, belleğin diskten binlerce kat hızlı olduğunu ve her ciddi
veritabanının, aslında, "hangi veriyi bellekte tutayım" sorusuna verilmiş bir
cevap olduğunu görmüştük. OxiMem, bu sorunun bir başka biçimine verilmiş
cevaptır: **bazı veriler diske hiç inmemeli.**

Bir uygulamanın veri kümesi tek tip değildir. Bir tarafta, kaybolmaması gereken
veri vardır: siparişler, işlemler, kullanıcı kayıtları. Bunlar belge motorunun
işidir; yazma-öncesi günlüğe yazılır, indekslenir, işlemler içinde korunur, ve
sunucu çökse bile geri gelirler. Ama aynı uygulamanın öteki tarafında, bambaşka
karakterde bir veri kümesi vardır: bir oturum belirteci, on beş dakika sonra
zaten geçersiz olacaktır. Bir sayfa görüntülenme sayacı, saniyede yüzlerce kez
artar ve kaybolursa kimse ölmez. Bir hız sınırlama penceresi, tanımı gereği
altmış saniye yaşar. Bir iş kuyruğu, birkaç saniyeliğine dolar ve boşalır. Bir
emir defterinin en iyi alış fiyatı, milisaniyede bir değişir.

Bu ikinci kümeye belge motorunun tüm makinesini uygulamak — JSON'a serileştirmek,
WAL'a yazmak, fsync beklemek, indeksleri güncellemek — israftır. Dayanıklılığın
bedelini, dayanıklılığa ihtiyacı olmayan veri için ödemek anlamına gelir. OxiMem,
tam olarak bu israfı ortadan kaldırmak için vardır: **JSON yok, WAL yok, indeks
yok**; yalnızca hash tabloları, çift uçlu kuyruklar ve dengeli ağaçlar üzerinde
çalışan, RESP telini konuşan bir bellek-içi katman.^[Kaynakta bu, tek cümleyle
şöyle özetlenir: "commands operate on raw `HashMap`/`VecDeque`/`HashSet`
structures for maximum throughput (no JSON, no WAL, no indexes)".]

## Neden Redis'in telini konuşmak?

Yeni bir bellek-içi depo yazmanın en kolay kısmı depodur; en zor kısmı, onu
dünyaya bağlamaktır. İstemci kütüphaneleri, bağlantı havuzları, izleme araçları,
komut satırı istemcileri, çerçeve entegrasyonları — bunların hepsi yıllar süren
ekosistem emeğidir. Kendi protokolünüzü icat ederseniz, bu emeği baştan
harcamanız gerekir.

OxiMem bu yolu seçmez. Bunun yerine, sektörde fiilen standart hale gelmiş bir
teli — **RESP**'i (Redis Serialization Protocol) — konuşur. Bunun sonucu şudur:
`redis-cli` ile OxiMem'e bağlanabilirsiniz; Python'da `redis-py`, Node'da
`node-redis`, Go'da `go-redis` — hiçbirinde tek satır değişiklik gerekmez. Var
olan bütün ekosistem, bedavaya gelir. Protokolü uyarlamak, ekosistemi
kopyalamaktan kat kat ucuzdur.

RESP'in kendisi şaşırtıcı derecede basittir; sekiz yıllık bir protokolün bu kadar
sade kalabilmesi, iyi tasarımın kanıtıdır. Her mesaj, ilk baytıyla türünü
bildirir: `+` basit dize, `-` hata, `:` tamsayı, `$` toplu dize (uzunluk önekli),
`*` dizi. Bir istemci komutu, her zaman toplu dizelerden oluşan bir dizidir.
`GET oturum:42` komutunun hattaki tam karşılığı şudur:

```text
*2\r\n$3\r\nGET\r\n$8\r\noturum:42\r\n
```

Yani: "iki elemanlı bir dizi geliyor; birincisi üç baytlık bir dize (`GET`),
ikincisi sekiz baytlık bir dize (`oturum:42`)". Sunucunun cevabı da aynı
dilbilgisiyle kodlanır:

```text
$5\r\nahmet\r\n        (değer bulundu: "ahmet")
$-1\r\n                (anahtar yok: null toplu dize)
:42\r\n                (bir sayaç sonucu)
+OK\r\n                (başarı)
-ERR unknown command   (hata)
*-1\r\n                (null dizi — birazdan göreceğiz: iptal edilmiş işlem)
```

Uzunluk önekli olması, çerçeveleme sorununu — yirmi dördüncü bölümde gördüğümüz
o "bir mesaj nerede biter" sorusunu — çözer. OxiMem'in çözümleyicisi, kendi ikili
protokolünde olduğu gibi bir üst sınır uygular: on altı mebibaytı aşan bir uzunluk
bildirimi, tek bayt bellek ayrılmadan reddedilir. Gerçek Redis burada daha
cömerttir, ama cömertlik, kimliği doğrulanmamış bir istemcinin sunucuyu bellek
ayırmaya zorlaması demektir.^[Bu sınır, bir bulanık test (fuzzing) çalışmasının
bulgusudur: uzunluk alanı saldırgan denetimindedir, ve `vec![0u8; len]` gibi masum
bir satır, hizmet engelleme açığına dönüşür.]

## Açmak: bir port

OxiMem varsayılan olarak **kapalıdır**; kullanmadığınız şeyin bedelini
ödemezsiniz. Açmak için tek bir ortam değişkeni yeter:

```bash
# RESP dinleyicisini 6380 portunda aç
export OXIDB_OXIMEM_PORT=6380
./oxidb-server

# Artık standart redis-cli ile bağlanabilirsiniz
redis-cli -p 6380 PING
# PONG
```

Buradan sonrası, alışkın olduğunuz her şeydir.

## Komut ailesi

OxiMem, Redis'in komut yüzeyinin bir alt kümesini uygular — ama pratikte
kullanılan çekirdeğin neredeyse tamamını. Aşağıdaki tablo, motorun fiilen
tanıdığı komutları aileleriyle listeler; bu listede olmayan bir komut,
`unknown command` hatasıyla döner.

| Aile | Komutlar |
|---|---|
| Bağlantı | `PING` `ECHO` `QUIT` `SELECT` `AUTH` `HELLO` `CLIENT` `COMMAND` |
| Dize | `SET` `GET` `GETSET` `SETNX` `SETEX` `PSETEX` `MSET` `MGET` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `GETDEL` `GETEX` |
| Sayaç | `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` `DECRBYFLOATGE` |
| Anahtar / TTL | `DEL` `EXISTS` `TYPE` `KEYS` `SCAN` `RANDOMKEY` `RENAME` `COPY` `DBSIZE` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` `FLUSHDB` `FLUSHALL` |
| Hash | `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HDEL` `HEXISTS` `HKEYS` `HVALS` `HLEN` `HINCRBY` `HRANDFIELD` `HSCAN` |
| Liste | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LTRIM` `LMOVE` `RPOPLPUSH` `LMPOP` |
| Küme | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SINTER` `SUNION` `SDIFF` `SINTERSTORE` `SUNIONSTORE` `SDIFFSTORE` `SSCAN` |
| Sıralı küme | `ZADD` `ZREM` `ZSCORE` `ZCARD` `ZCOUNT` `ZINCRBY` `ZRANK` `ZREVRANK` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZPOPMIN` `ZPOPMAX` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZUNIONSTORE` `ZINTERSTORE` `ZMPOP` `ZSCAN` |
| Bit | `SETBIT` `GETBIT` `BITCOUNT` |
| Engelleyen | `BLPOP` `BRPOP` `BZPOPMIN` `BLMOVE` `BRPOPLPUSH` |
| Yayın/abonelik | `PUBLISH` `SUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` |
| İşlem | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |
| Betik | `EVAL` `EVALSHA` `SCRIPT` |
| Sunucu | `INFO` `CONFIG` |

Bu tablodaki tek yabancı isim `DECRBYFLOATGE`'dir; o, Redis'te bulunmayan,
OxiMem'e özgü bir eklemedir ve biraz sonra sırası gelecek.

En basitinden başlayalım. Dize komutları, anahtar-değer fikrinin en saf halidir:

```bash
redis-cli -p 6380

SET kullanici:42:ad "Ayşe"     # OK
GET kullanici:42:ad            # "Ayşe"
EXISTS kullanici:42:ad         # (integer) 1
STRLEN kullanici:42:ad         # (integer) 5
APPEND kullanici:42:ad " Y."   # (integer) 8
SETNX kullanici:42:ad "Başka"  # (integer) 0  — zaten var, yazmaz
DEL kullanici:42:ad            # (integer) 1
GET kullanici:42:ad            # (nil)
```

Sayaçlar, anahtar-değerin en çok işe yarayan yüzüdür; çünkü tek bir komutta
oku-değiştir-yaz üçlüsünü **atomik** biçimde yaparlar. Bir web isteği içinde
"oku, bire ekle, geri yaz" yazarsanız, iki eşzamanlı istek birbirinin artışını
yutar; `INCR` ise böyle bir yarışa yer bırakmaz:

```bash
INCR sayfa:anasayfa:goruntulenme      # (integer) 1
INCR sayfa:anasayfa:goruntulenme      # (integer) 2
INCRBY sayfa:anasayfa:goruntulenme 10 # (integer) 12
DECR sayfa:anasayfa:goruntulenme      # (integer) 11

# Hash alanları da atomik olarak artırılabilir
HINCRBY istatistik:2026-07 istek 1    # (integer) 1
HINCRBY istatistik:2026-07 hata 1     # (integer) 1
HGETALL istatistik:2026-07            # 1) "istek" 2) "1" 3) "hata" 4) "1"
```

Listeler bir kuyruğa, kümeler bir üyelik testine, sıralı kümeler bir puan
tablosuna dönüşür:

```bash
# Liste: üretici-tüketici kuyruğu (sağdan it, soldan çek = FIFO)
RPUSH isler "is-1" "is-2" "is-3"   # (integer) 3
LLEN isler                          # (integer) 3
LRANGE isler 0 -1                   # "is-1" "is-2" "is-3"
LPOP isler                          # "is-1"

# Engelleyen çekme: kuyruk boşsa 5 saniyeye kadar bekler
BLPOP isler 5                       # 1) "isler" 2) "is-2"

# Küme: üyelik
SADD cevrimici:kullanicilar 42 77 91  # (integer) 3
SISMEMBER cevrimici:kullanicilar 77   # (integer) 1
SCARD cevrimici:kullanicilar          # (integer) 3

# Sıralı küme: puan tablosu (ve emir defteri!)
ZADD skorlar 1500 "ayse" 1800 "mehmet" 1200 "zeynep"
ZINCRBY skorlar 400 "zeynep"          # "1600"
ZREVRANGE skorlar 0 2 WITHSCORES      # mehmet 1800, zeynep 1600, ayse 1500
ZRANK skorlar "ayse"                  # (integer) 0  — en düşük puanlı
```

Sıralı kümenin altında, on yedinci bölümün B-ağacı fikrinin küçük bir yankısı
vardır: puanlar hem bir hash tablosunda (üye → puan, O(1) arama) hem de dengeli
bir ağaçta (puan, üye) tutulur; böylece "en iyi alış fiyatı" sorusu — bir borsa
uygulamasının en sıcak sorusu — `ZREVRANGE anahtar 0 0` ile logaritmik zamanda
cevaplanır.

## TTL ve süre dolumu

Anahtar-değer katmanının belge motorundan ayrıldığı en karakteristik nokta,
verinin **kendiliğinden ölebilmesidir**. Bir oturum belirtecini silmek için bir
temizlik işi yazmazsınız; ona bir ömür verirsiniz ve unutursunuz.

```bash
# Ömürle birlikte yaz — üç eşdeğer yol
SETEX oturum:abc 900 "kullanici=42"    # 900 saniye
SET   oturum:abc "kullanici=42" EX 900 # aynısı, SET seçeneğiyle
SET   oturum:abc "kullanici=42" PX 500 # milisaniye cinsinden

TTL oturum:abc      # (integer) 899   — kalan saniye
PTTL oturum:abc     # kalan milisaniye
PERSIST oturum:abc  # (integer) 1     — ömrü kaldır, kalıcı yap
TTL oturum:abc      # (integer) -1    — ömrü yok
TTL yok:boyle:bir   # (integer) -2    — anahtar hiç yok
```

Mekanizma iki katmanlıdır ve her ikisi de gereklidir. Birincisi **tembel süre
dolumudur**: bir anahtar okunduğunda, önce son kullanma zamanı denetlenir; geçmişse
anahtar o anda silinir ve okuma "yok" cevabı döner. Bu, doğruluğu garanti eder —
süresi dolmuş bir değeri asla göremezsiniz. Ama tek başına yetmez: hiç okunmayan
bir anahtar, süresi dolduğu halde sonsuza dek bellekte kalırdı. Bu yüzden ikinci
katman vardır: saniyede bir uyanan bir **süpürücü iş parçacığı**, süresi dolmuş
anahtarları toplu olarak siler. Süpürücü ayrıca, silinen her anahtar için — açıksa
— bir `expired` bildirimi yayımlar ve birazdan göreceğimiz sürüm sayacını artırır;
yani bir anahtarın sessizce ölmesi bile, onu izleyen bir işlem için bir
**değişikliktir**.

Burada dürüst olmak gereken bir sınır var: OxiMem'de ömür yalnızca **dize
anahtarlarına** uygulanır. Bir hash'e, listeye ya da kümeye `EXPIRE` verirseniz
`0` alırsınız — yani "böyle bir şey yapmadım". Bunun pratik sonucu, oturum
önbelleği gibi ömürlü verileri hash olarak değil, tek bir serileştirilmiş dize
olarak tutmanız gerektiğidir.^[TTL, saniye çözünürlüğünde tutulur; `PX` ve
`PSETEX` milisaniye değerlerini yukarı yuvarlar.] Aşağıdaki oturum önbelleği
deseni, bu kısıtı tam olarak böyle karşılar:

```python
import json
import redis

r = redis.Redis(host="localhost", port=6380, decode_responses=True)

OTURUM_OMRU = 15 * 60  # 15 dakika

def oturum_yaz(belirtec: str, kullanici: dict) -> None:
    # Hash'lere TTL uygulanmadığı için oturumu TEK bir dizeye serileştiriyoruz.
    r.setex(f"oturum:{belirtec}", OTURUM_OMRU, json.dumps(kullanici))

def oturum_oku(belirtec: str) -> dict | None:
    ham = r.get(f"oturum:{belirtec}")
    if ham is None:
        return None                      # süresi dolmuş ya da hiç yok
    # Her erişimde ömrü tazele — "kayan pencere" oturumu
    r.expire(f"oturum:{belirtec}", OTURUM_OMRU)
    return json.loads(ham)

oturum_yaz("abc123", {"id": 42, "ad": "Ayşe", "rol": "admin"})
print(oturum_oku("abc123"))   # {'id': 42, 'ad': 'Ayşe', 'rol': 'admin'}
print(oturum_oku("yokboyle")) # None
```

## MULTI, EXEC ve WATCH: aynı fikir, küçük ölçekte

Onuncu ve on birinci bölümlerde, işlemleri ve eşzamanlılık denetimini uzun uzun
konuşmuştuk. Orada anlattığımız **iyimser eşzamanlılık denetiminin** (OCC) özü şuydu:
kilitleme, oku ve çalış; yazma anında, okuduğun şeylerin değişmediğini doğrula;
değiştiyse iptal et ve yeniden dene. Çatışmanın nadir olduğu bir dünyada bu,
kilitlemekten çok daha ucuzdur.

OxiMem'in işlem modeli, tam olarak bu fikrin küçük ölçekte tekrarıdır — ve bu bir
tesadüf değil, yakınsamadır: aynı problem, aynı çözümü doğurur.

`MULTI`, bir kuyruk açar. Ondan sonra gönderdiğiniz her komut çalıştırılmaz;
`QUEUED` cevabıyla biriktirilir. `EXEC` geldiğinde, biriken komutların hepsi
**arka arkaya, araya başka bir işlem girmeden** çalıştırılır:

```bash
MULTI                    # OK
SET sayac 5              # QUEUED  — çalışmadı, kuyruğa girdi
INCRBY sayac 10          # QUEUED
EXEC                     # 1) OK  2) (integer) 15
GET sayac                # "15"
```

Redis bu yalıtımı bedavaya alır: tek iş parçacıklı olduğu için, iki `EXEC` bloğu
zaten iç içe geçemez. OxiMem çok iş parçacıklıdır; bu yüzden yalıtımı açıkça
satın alır: `EXEC`, mağaza düzeyinde bir işlem kilidi alır ve yalnızca kendi
kuyruğunu boşaltırken elinde tutar. İki `EXEC` asla komutlarını birbirine
karıştıramaz.

Ama `MULTI`/`EXEC` tek başına, onuncu bölümdeki anlamıyla bir işlem değildir:
atomikliği verir, **yalıtımın "okuduğumu doğrula" yarısını vermez**. "Bakiyeyi
oku, yeterliyse düş" gibi bir mantık, okumayla yazma arasında birinin araya
girmesine açıktır — klasik kayıp güncelleme anomalisi.

İşte `WATCH` tam burada devreye girer, ve OCC'nin **doğrulama fazının** ta
kendisidir. `WATCH anahtar` dediğinizde, OxiMem o anahtarın o andaki durumunun bir
**parmak izini** alır. `EXEC` geldiğinde, işlem kilidinin altında bu parmak izleri
yeniden hesaplanır; biri bile değişmişse işlem **çalıştırılmaz** ve `EXEC`, RESP'in
null dizisini (`*-1`) döner. İstemci bunu "yeniden dene" diye okur.

Parmak izinin nasıl hesaplandığı, on birinci bölümdeki sürüm sayacı fikrinin
doğrudan uygulamasıdır. Değerin bir kopyası alınmaz — bir emir defteri
sıralı kümesini `WATCH` etmek, o defteri kopyalamak anlamına gelseydi, bu O(n)
maliyetiyle her şeyi öldürürdü. Bunun yerine parmak izi üç küçük parçadan oluşur:
anahtarın **mutasyon sayacı** (her yazma komutu, dokunduğu anahtarın sayacını bir
artırır), mağazanın **çağ sayacı** (`FLUSHALL` her şeyi geçersiz kılar) ve
anahtarın **var olup olmadığı** (böylece iki okuma arasında sessizce süresi dolan
bir anahtar da bir değişiklik sayılır). Üçü de tamsayıdır; anlık görüntü almak
O(1)'dir.

Bu tasarımın bilinçli bir asimetrisi vardır: sayaç, yazma komutu hiçbir şeyi
değiştirmemiş olsa bile artar (aynı değeri tekrar yazmak gibi). Yani **yanlış
alarm mümkündür** — işlem gereksiz yere iptal edilip yeniden denenebilir. Ama
**kaçırılmış değişiklik imkânsızdır**. Bu, doğruluğun performansa tercih edildiği
yerlerden biridir; yanlış alarmın bedeli bir yeniden deneme, kaçırmanın bedeli
bozuk veridir.

Klasik yarışı görelim. İki uçbirim; A cüzdanı izlerken B araya giriyor:

```bash
# --- Uçbirim A ---
SET cuzdan:42 100
WATCH cuzdan:42          # OK — parmak izi alındı
GET cuzdan:42            # "100" — kararımızı bu değere göre vereceğiz

# --- Uçbirim B (araya giriyor) ---
SET cuzdan:42 999        # OK

# --- Uçbirim A (devam) ---
MULTI                    # OK
SET cuzdan:42 50         # QUEUED
EXEC                     # (nil)  — İPTAL: izlenen anahtar değişti
GET cuzdan:42            # "999"  — A'nın yazması hiç gerçekleşmedi
```

`EXEC`'in hattaki cevabı tam olarak şudur — ve bu, "boş dize" ile karıştırılmaması
gereken ayrı bir RESP değeridir:

```text
*-1\r\n      (null dizi = işlem iptal edildi; boş dizi *0\r\n değildir!)
```

Gerçek bir uygulamada bu döngüyü elle yazmazsınız; istemci kütüphanesi yazar.
`redis-py`'nin deseni, OCC'nin ders kitabı biçimidir — oku, karar ver, doğrula,
çatışırsa yeniden dene:

```python
import redis
from redis import WatchError

r = redis.Redis(host="localhost", port=6380, decode_responses=True)
r.set("cuzdan:42", 100)

def para_dus(cuzdan: str, tutar: int, deneme: int = 5) -> bool:
    """Bakiyeyi oku, yeterliyse düş — yarış olursa yeniden dene (OCC)."""
    with r.pipeline() as boru:
        for _ in range(deneme):
            try:
                boru.watch(cuzdan)                  # parmak izini al
                bakiye = int(boru.get(cuzdan) or 0) # okuma (henüz kuyruk yok)
                if bakiye < tutar:
                    boru.unwatch()
                    return False                    # yetersiz bakiye
                boru.multi()                        # buradan sonrası kuyruğa girer
                boru.set(cuzdan, bakiye - tutar)
                boru.execute()                      # EXEC: doğrula + çalıştır
                return True
            except WatchError:
                continue                            # biri araya girdi — tekrar dene
    raise RuntimeError("çekişme çok yüksek, işlem tamamlanamadı")

print(para_dus("cuzdan:42", 30))   # True
print(r.get("cuzdan:42"))          # "70"
print(para_dus("cuzdan:42", 500))  # False — bakiye yetmiyor
```

Bu döngüde OCC'nin üç fazını da görebilirsiniz: `watch` + `get` **okuma fazı**,
`multi` ile biriken komutlar **yazma tamponu**, `execute` ise **doğrulama ve
işleme** fazıdır. Onuncu bölümde belge motoru için çizdiğimiz şemanın, birkaç
mikrosaniyeye sığdırılmış hali.

## EXECABORT: hataları erken yakalamak

Bir işlemin ortasında bir komutun çalışmayacağını, `EXEC` sırasında öğrenmek
kötüdür: komutların yarısı çalışmış, yarısı çalışmamış olurdu. OxiMem bu yüzden
**kuyruğa alma zamanında** doğrulama yapar: kuyruğa giren komut tanınmıyorsa ya da
argüman sayısı yetersizse, o anda hata döner **ve işlem zehirlenir**. Sonraki
`EXEC`, hiçbir şey çalıştırmadan `EXECABORT` ile reddedilir:

```bash
MULTI                    # OK
SET z 1                  # QUEUED
NOPE z                   # (error) ERR unknown command 'NOPE'   ← zehirlendi
GET z                    # QUEUED  (kuyruk hâlâ alıyor, ama artık boşuna)
EXEC                     # (error) EXECABORT Transaction discarded because of
                         #         previous errors.
GET z                    # (nil)  — SET z 1 HİÇ çalışmadı
```

Bu, "ya hep ya hiç"in protokol düzeyindeki savunmasıdır: yazım hatası içeren bir
işlem, veriye hiç dokunmaz.

## Sunucu tarafı betikler ve atomik borç düşme

`WATCH` döngüsü, çatışma seyrekken mükemmeldir; ama çekişme yüksek olduğunda
istemci ile sunucu arasında birkaç gidiş-geliş yapar ve yeniden denemeler artar.
Bu yüzden OxiMem, Redis gibi, **sunucu tarafında betik** çalıştırabilir: gömülü bir
Lua yorumlayıcısı,^[Lua 5.4, `mlua` üzerinden gömülüdür. Betikler `redis.call` /
`redis.pcall` ile komut çalıştırır; `cjson` ve `redis.sha1hex` mevcuttur. Sonsuz
döngüye giren bir betik, `SCRIPT KILL` ile durdurulabilir.] tüm oku-karar-ver-yaz
mantığını tek bir atomik adımda, sunucunun içinde çalıştırır. Gidiş-geliş sayısı:
bir.

```bash
# Yeterli bakiye varsa düş, yoksa dokunma — tek turda, atomik
EVAL "local b = tonumber(redis.call('GET', KEYS[1])) \
      if b >= tonumber(ARGV[1]) then \
        redis.call('INCRBYFLOAT', KEYS[1], '-' .. ARGV[1]) \
        return 1 \
      end \
      return 0" 1 cuzdan:42 30

# (integer) 1  — düşüldü
GET cuzdan:42
# "40"
```

Betikleri her seferinde göndermek yerine `SCRIPT LOAD` ile bir kez yükleyip
`EVALSHA` ile SHA-1 özetinden çağırabilirsiniz — hat üzerinde yüz baytlık bir
betik yerine kırk baytlık bir özet gider.

Bu "kontrol et ve düş" deseni o kadar yaygındır ki, OxiMem onu Lua'ya bile ihtiyaç
bırakmayan yerel bir komut olarak sunar. `DECRBYFLOATGE` — "greater-or-equal ise
düş" — Redis'te bulunmayan, OxiMem'e özgü tek komuttur: yeterli bakiye varsa
atomik olarak düşer ve yeni bakiyeyi döner, yoksa hiç dokunmadan `nil` döner.
Kredi limitini aşan bir borçlandırmanın yarış koşuluyla bile gerçekleşmesi
imkânsızdır:

```bash
SET bakiye:42 100
DECRBYFLOATGE bakiye:42 30    # "70"   — düşüldü
DECRBYFLOATGE bakiye:42 200   # (nil)  — yetersiz; bakiyeye DOKUNULMADI
GET bakiye:42                 # "70"
```

## Gerçek bir desen: hız sınırlama

Şimdiye kadarki parçaları birleştiren, üretimde en çok kullanılan desenlerden
birine bakalım: **sabit pencereli hız sınırlama**. Fikir basittir: her kullanıcı
için, pencerenin adını taşıyan bir sayaç anahtarı tutarsınız; her istekte artırır,
ilk artışta ona bir ömür verirsiniz. Pencere dolduğunda anahtar kendiliğinden
ölür — temizlik işi yoktur. Sayma `INCR` ile atomiktir, ve `INCR` ile `EXPIRE`'ı
bir `MULTI` bloğuna koyarak ikisini tek turda gönderiyoruz:

```javascript
import { createClient } from "redis";

const istemci = createClient({ url: "redis://localhost:6380" });
await istemci.connect();

const LIMIT = 100;        // pencere başına izin verilen istek
const PENCERE = 60;       // saniye

async function izinVarMi(kullaniciId) {
  // Pencere numarasını anahtara gömüyoruz: her dakika yeni anahtar doğar,
  // eskisi TTL ile kendiliğinden ölür.
  const pencere = Math.floor(Date.now() / 1000 / PENCERE);
  const anahtar = `hiz:${kullaniciId}:${pencere}`;

  // INCR + EXPIRE'ı tek bir MULTI/EXEC turunda gönder (atomik ve tek gidiş-geliş)
  const [sayi] = await istemci
    .multi()
    .incr(anahtar)
    .expire(anahtar, PENCERE)
    .exec();

  return { izin: sayi <= LIMIT, kalan: Math.max(0, LIMIT - sayi) };
}

console.log(await izinVarMi(42));  // { izin: true, kalan: 99 }
```

Bu on satırın altında, bu bölümün bütün fikirleri var: atomik sayaç, kendiliğinden
ölen anahtar, tek turda çalışan bir işlem. Aynı işi belge motorunda yapmak — her
istekte bir belge okuyup güncellemek, WAL'a yazmak, fsync beklemek — hem yüz kat
pahalı olurdu, hem de gereksizdi: bu sayaç kaybolsa, kullanıcı birkaç istek fazla
yapardı, o kadar.

Yayın-abonelik de aynı sadelikte çalışır ve gerçek zamanlı bildirimler için
yeterlidir:

```javascript
const abone = istemci.duplicate();
await abone.connect();

// Kalıp aboneliği: "fiyat." ile başlayan tüm kanallar
await abone.pSubscribe("fiyat.*", (mesaj, kanal) => {
  console.log(`${kanal} → ${mesaj}`);   // fiyat.BTC → 68500
});

await istemci.publish("fiyat.BTC", "68500");
```

## Tek bir keyspace — bilinçli bir tercih

OxiDB, çok veritabanlı bir motordur: her veritabanının kendi koleksiyonları, kendi
SQL motoru, kendi işlemleri vardır. OxiMem ise bunun dışındadır: **keyspace
küreseldir.** Hangi veritabanına bağlı olursanız olun, `SET a 1` aynı tek keyspace'e
yazar. `SELECT` komutu kabul edilir ve `OK` döner — ama hiçbir şey yapmaz.

Bu bir eksiklik değil, bir tercihtir. OxiMem'in varlık nedeni, uygulamanın **sıcak
kenarıdır**: oturumlar, sayaçlar, kuyruklar, hız sınırları. Bunlar tipik olarak
veritabanı sınırlarını umursamaz; bir oturum belirteci, hangi mantıksal veritabanına
ait olduğu sorusuna anlamlı bir cevap vermez. Keyspace'i veritabanlarına bölmek,
kazanç sağlamadan bir dolaylılık katmanı eklerdi. Ayrım isteyen, anahtar öneki
kullanır — `musteri:7:oturum:abc` — ki bu, Redis dünyasının da yerleşik pratiğidir.

## Kalıcılık: iki seçenek, ikisi de isteğe bağlı

"Bellek-içi" demek, "veri kaybolabilir" demektir; ve bu, çoğu kullanım için doğru
karardır. Ama "yeniden başlatmayı atlatsın yeter" diyen bir orta yol da vardır ve
OxiMem iki biçimde sunar.

Birincisi, **anlık görüntüdür**. `OXIDB_OXIMEM_SNAPSHOT_SECS` verilirse, mağazanın
tamamı belirtilen aralıklarla bir dosyaya yazılır; sunucu açılışta bu dosyayı okuyup
belleği yeniden kurar. Yazma, geçici dosyaya yaz–fsync–yeniden adlandır üçlüsüyle
yapılır; altıncı bölümde gördüğümüz **atomik değiştirme** desenidir bu: yazmanın
ortasında çöken bir sunucu, önceki anlık görüntüyü asla bozamaz.

İkincisi, **belge motoruna aynalamadır**. `OXIDB_OXIMEM_SQL` açıksa, her yazma aynı
zamanda belge motorunun `_kv`, `_hash`, `_list`, `_set`, `_zset` koleksiyonlarına
yansıtılır; açılışta bellek, bu koleksiyonlardan yeniden kurulur — ömürler dahil,
çünkü ayna mutlak son kullanma zamanını da saklar. Bunun ikinci bir faydası vardır:
sıcak veri, birden bire **sorgulanabilir** hale gelir. Ama bedeli açıktır ve
gizlenmemelidir: her yazma artık belge motoruna da uğrar; OxiMem'in hız avantajının
büyük kısmı gider. Bu yüzden mod, adında dürüstçe ayrılır — biri "hızlı kip", diğeri
"aynalama kipi".

Redis'in AOF'una — her komutu diske ekleyen, saniyede bir fsync'leyen günlüğe —
karşılık gelen bir şey **yoktur**. Anlık görüntü kipinde, son görüntüden sonraki
pencere, çökmede kaybolur. Bunu bilerek kullanın: OxiMem'in kalıcılığı, "yeniden
başlatmayı atlatmak" içindir, "hiçbir yazmayı kaybetmemek" için değil. İkincisini
istiyorsanız, doğru yer belge motorudur.

## Performans: Redis'e ne kadar yakın?

Bir Redis uyumlu katmanın tek anlamlı ölçütü, Redis'in kendisidir. `redis-benchmark`
ile (100 bin istek, 50 istemci, 64 baytlık değerler) alınan tek-komut sonuçları
şöyledir:

| Test | Redis | OxiMem | Oran |
|---|---|---|---|
| PING | 250K op/s | 224K op/s | %90 |
| GET | 251K op/s | 240K op/s | %96 |
| SET | 249K op/s | 231K op/s | %93 |
| INCR | 249K op/s | 187K op/s | %75 |
| LPUSH | 254K op/s | 242K op/s | %95 |
| RPUSH | 238K op/s | 242K op/s | %101 |
| SADD | 253K op/s | 239K op/s | %94 |
| HSET | 253K op/s | 193K op/s | %76 |
| MSET (10) | 164K op/s | 232K op/s | %141 |

Yani tek komut modunda OxiMem, Redis'in yüzde 75 ile 101'i arasındadır; birkaç
komutta onu geçer. Bu, yirmi yıllık, elle optimize edilmiş C veri yapılarına karşı,
standart Rust koleksiyonlarıyla alınmış saygıdeğer bir sonuçtur. Asıl kazanç ise
hızın kendisi değil, bu hızın **aynı süreç içinde**, belge motorunun yanı başında
bulunmasıdır: ayrı bir Redis kurmak, işletmek ve sürümlemek gerekmez.

Dürüst olmak gerekirse boru hattı (pipelining) tarafında Redis hâlâ öndedir; tek
iş parçacıklı olay döngüsü ve özel veri yapıları, yüzlerce komutun tek turda
işlendiği senaryolarda ona avantaj sağlar. OxiMem, ardışık aynı-komut yığınlarını
tek kilit altında birleştiren bir hızlı yol ekleyerek arayı büyük ölçüde kapatmıştır
— ama kapatmıştır, geçmemiştir.

## Eksikler, sınırlar ve ne zaman hangisi

Bu bölümü, olmayanları açıkça söyleyerek bitirmek gerekir; çünkü bir aracın
sınırlarını bilmemek, onu yanlış yerde kullanmanın en kısa yoludur.

**Kimlik doğrulama yoktur.** `AUTH` komutu kabul edilir ve `OK` döner — ama hiçbir
şey doğrulamaz. OxiMem dinleyicisi, açıldığı ağda kime açıksa, ona tam yetki verir.
Bu yüzden onu asla halka açık bir arayüze bağlamayın; yerel arayüzde ya da güvenilir
bir ağ bölmesinde tutun. Belge motorunun SCRAM kimlik doğrulaması ve rol tabanlı
erişim denetimi, OxiMem portunu **kapsamaz**.

**Kümeleme ve çoğaltma yoktur.** OxiMem düğüm yereldir; Raft ile çoğaltılmaz. Bir
kümede her düğümün kendi keyspace'i vardır. Sıcak veri için bu genellikle kabul
edilebilir; paylaşılan durum için değildir.

**RESP3 yoktur.** `HELLO 3` diyen bir istemci `NOPROTO` cevabı alır ve RESP2'ye
düşer — ki bütün büyük istemciler bunu sorunsuzca yapar.

**Anahtar ömrü yalnızca dizelere uygulanır**; hash, liste, küme ve sıralı kümeler
kalıcıdır ve elle silinmelidir.

**Kayıpsız kalıcılık yoktur**: AOF benzeri bir komut günlüğü bulunmaz.

Peki karar? Basit bir soruyla verilir: **bu veriyi kaybetsem ne olur?**

Cevap "hiçbir şey, yeniden hesaplanır ya da zaten kısa ömürlü" ise — oturumlar,
önbellekler, sayaçlar, hız sınırları, kısa ömürlü kuyruklar, canlı fiyatlar, emir
defterinin anlık hali — yer OxiMem'dir. Bu veriye dayanıklılık uygulamak, hiçbir
şey kazandırmadan onu yüz kat yavaşlatır.

Cevap "para kaybederim, denetim kaydım eksilir, müşteri kızar" ise — siparişler,
gerçekleşmiş işlemler, kullanıcı kayıtları, defterler — yer belge motorudur. Orada
yazma-öncesi günlük, işlemler, indeksler ve kurtarma vardır; ve bunların bedelini
seve seve ödersiniz.

En güçlü kullanım ise, ikisini birlikte kullanmaktır ve bu kitabın örnek
uygulamasında tam olarak böyle yapılır: bir borsanın emir defteri OxiMem'in sıralı
kümelerinde yaşar, bakiyeler `WATCH`'lı işlemlerle atomik olarak devredilir — ama
**gerçekleşen her işlem**, kalıcı bir defter kaydı olarak belge motoruna yazılır.
Sıcak yol hızlı, soğuk yol dayanıklıdır. On üçüncü bölümün bellek-disk hiyerarşisi,
burada iki motorlu bir mimariye dönüşmüştür; ve her verinin ait olduğu katmana
yerleştirilmesi, bu kitabın baştan beri savunduğu tek ilkenin — **her tasarım bir
ödünleşimdir, doğru olan, ödünleşimi bilerek seçmektir** — belki de en somut
uygulamasıdır.
