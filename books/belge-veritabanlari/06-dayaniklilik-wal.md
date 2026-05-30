# Dayanıklılık: Write-Ahead Log, fsync ve Çökmeden Kurtarma

Önceki bölümün sonunda sinsi bir soru bırakmıştık: bir veriyi diske
"yazdığınızda", o veri gerçekten kalıcı olmuş mudur? Sezgi rahatça "evet" der.
Ama bu sezgi yanlıştır ve bu yanlışın bedeli, kaybolan ya da bozulan veridir.
Bu bölüm, bir veritabanının en çetin ve en az takdir edilen sorumluluğunu ele
alır: bir yazmanın gerçekten dayanıklı olduğundan emin olmak ve sistem en
beklenmedik anda çökerse, enkazın içinden tutarlı biçimde geri dönmek. Bu,
veritabanını basit bir dosyadan ayıran en derin farklardan biridir.

![Şekil 6 — Önce-yaz günlüğü (WAL) akışı ve çökmeden kurtarma.](sekiller/06-wal.svg){width=80%}

## "Yazdım" demek neden yetmez

Bir programın diske yazma isteği, sandığınız gibi tek adımda diskin yüzeyine
ulaşmaz. Aradan birçok katman geçer ve her katman, hız uğruna veriyi geçici
olarak bekletir. Programınız veriyi yazdığında, o veri önce işletim sisteminin
tuttuğu bir tampona düşer; işletim sistemi, performans için, bu veriyi hemen
diske göndermeyebilir — biriktirip sonra topluca yazmayı tercih edebilir. Veri
diske gönderildiğinde bile, diskin kendi denetleyicisinin de bir önbelleği
vardır; veri orada bekleyip henüz fiziksel ortama işlenmemiş olabilir. Yani
"yazdım" dediğiniz an ile verinin gerçekten kalıcı ortama oturduğu an arasında,
görünmez bir gecikme ve birçok bekleme noktası vardır.

Sıradan zamanlarda bu katmanlar fark edilmez; veri eninde sonunda diske oturur
ve her şey yolundadır. Sorun, tam da bu bekleme noktalarının birinde sistem
çökerse — elektrik kesilirse, makine donarsa, süreç aniden ölürse — ortaya
çıkar. O an, henüz fiziksel ortama ulaşmamış olan her şey buharlaşır. Programınız
"kaydettim" demiş, kullanıcıya "işlem tamam" demiş olabilir; ama o veri
katmanların birinde beklerken yok olmuştur. Kullanıcı, yaptığını sandığı işlemin
hiç gerçekleşmediğini sonradan öğrenir. Bir bankada, bir siparişte, bir tıbbi
kayıtta bunun ne anlama geldiğini düşünün.

## fsync: veriyi gerçekten diske zorlamak

Bu gecikmenin bir panzehiri vardır. İşletim sistemine, "bu veriyi şimdi, bu an,
gerçekten kalıcı ortama yaz ve bunu bana onaylamadan geri dönme" diyebilirsiniz.
Bu açık emre, geleneksel adıyla **fsync** denir. fsync döndüğünde, daha önce
yazdığınız verinin gerçekten dayanıklı hale geldiğinden emin olabilirsiniz; artık
çökme onu silemez.

Ama fsync pahalıdır. Çünkü tam da diskin en sevmediği şeyi yapmaya zorlar:
beklemeyi bırakıp veriyi fiilen fiziksel ortama işlemek. Bu, milisaniyeler
mertebesinde sürebilir — bilgisayar ölçeğinde upuzun bir zaman. Bir veritabanı
her küçük yazma için fsync çağırsaydı, korkunç yavaş olurdu. İşte buradan, bu
bölümün geri kalanını biçimlendiren gerilim doğar: **dayanıklılık güvenlidir ama
yavaştır; onsuz hızlıdır ama tehlikelidir.** İyi bir veritabanı, bu ikisi
arasında akıllıca bir denge kurmak zorundadır. (Bir incelik: bazı sistemlerde
sıradan fsync, veriyi disk denetleyicisinin önbelleğine bırakıp fiziksel ortama
işlendiğini garanti etmeyebilir; gerçek bir dayanıklılık için daha güçlü, daha
da pahalı bir "tam boşaltma" emri gerekir. Üçüncü kısımda OxiDB'nin tam da bu
güçlü boşaltmayı kullandığını ve bunun tek-kayıt güncellemelerinin hızına nasıl
yansıdığını göreceğiz.)

## İkinci tehlike: yarım kalmış yazma

Çökmenin yalnızca veriyi yok etmek gibi bir tehlikesi yoktur; ondan daha sinsi
bir tehlikesi vardır. Bir yazma işleminin **ortasında** çökme olursa ne olur?
Disk veriyi bloklar halinde yazdığı için, büyük bir kaydın bir kısmı yeni
değeriyle, bir kısmı eski değeriyle, hatta arada bir yerde tümüyle bozuk
kalabilir. Buna **yarım kalmış yazma** (torn write) denir. Sonuç, yalnızca eksik
değil, **tutarsız** bir veridir: yarısı bir dünyaya, yarısı başka bir dünyaya ait
bir kayıt. Böyle bir kaydı sonradan okumaya çalışmak, anlamsız ya da yanıltıcı
sonuçlar doğurabilir.

Birinci bölümde para transferi örneğini hatırlayın: bir hesaptan düş, diğerine
ekle. Bu iki adımın arasında çökme olursa, para buharlaşmıştı. Şimdi sorunun daha
da temel bir katmanını görüyoruz: yalnızca iki ayrı adım arasında değil, **tek
bir kaydın yazılması sırasında** bile, çökme bizi tutarsız bir duruma
düşürebilir. Bir veritabanı, hem "ya hep ya hiç" güvencesini vermek hem de yarım
yazmalara karşı kendini korumak zorundadır. Bu iki ihtiyaca birden, şaşırtıcı
derecede zarif tek bir fikir yanıt verir.

## Çözüm: önce niyetini yaz

Bu fikrin adı **yazma-öncesi günlük** — İngilizcesiyle write-ahead log, kısaca
WAL. Özü tek bir cümlede toplanır: **asıl veriyi değiştirmeden önce, ne
yapacağını ayrı bir günlüğe yaz ve o günlüğü dayanıklı kıl.** Yani önce niyetini
kaydedersin, sonra eylemi gerçekleştirirsin.

Bunu günlük hayattan bir benzetmeyle düşünelim. Önemli bir işe girişmeden önce,
ne yapacağınızı bir deftere not aldığınızı varsayın: "şu hesaptan şu kadar düş,
şu hesaba şu kadar ekle." Bu notu aldıktan, yani niyetinizi kalıcı biçimde
kaydettikten sonra, işi yapmaya başlarsınız. İşin tam ortasında bayılıp
düşerseniz ne olur? Ayıldığınızda defterinizi açar, ne yapmaya niyet ettiğinizi
okur ve işi baştan, eksiksiz tamamlarsınız. Defter, niyetinizin kanıtıdır; eylem
yarım kalsa bile, defterdeki kayıt sayesinde onu tamamlayabilirsiniz.

WAL tam olarak böyle çalışır. Veritabanı bir değişiklik yapmadan önce, o
değişikliğin bir kaydını günlüğe **ekler** ve o kaydı fsync ile dayanıklı kılar.
Günlüğe yazmanın güzelliği, önceki bölümde gördüğümüz append-only fikrinin tam da
kendisidir: günlük yalnızca sona eklenir, yani diskin sevdiği o hızlı, ardışık
yazma biçiminde büyür. Bir değişiklik günlüğe dayanıklı biçimde yazıldığı an, o
değişiklik artık **kalıcı kabul edilir** — asıl veri dosyaları henüz
güncellenmemiş olsa bile. Çünkü bir çökme olsa dahi, günlükteki kayıt sayesinde o
değişikliği yeniden uygulayabiliriz.

İşte WAL'ın asıl zekâsı buradadır. Asıl veri yapısını — ister B-ağacı ister
append-only depo olsun — değiştirmek, rastgele yazma içerebilen, yavaş ve riskli
bir iştir. WAL, dayanıklılık güvencesini bu yavaş işten **ayırır**: dayanıklılığı
hızlı, ardışık günlük yazmasıyla hemen sağlar; asıl veri yapısını güncellemeyi
ise sonraya, acele etmeden yapılacak bir işe bırakır. Hız ve güvenlik, böylece
aynı anda elde edilir: günlük hızlı ve güvenlidir, asıl güncelleme ise tembel ve
huzurludur.

## Kurtarma: enkazın içinden geri dönmek

WAL'ın asıl sınavı, çökmeden sonra verdiği vaattir. Sistem yeniden açıldığında,
veritabanı kendini tutarlı bir duruma getirmek zorundadır ve bunu günlüğü
**okuyarak** yapar. Bu sürece **kurtarma** (recovery) denir. Bu kurtarma
düzeninin klasik, etkili biçimi, veritabanı literatüründe ARIES yöntemi olarak
bilinir (Mohan vd., 1992).

Kurtarma şöyle ilerler. Veritabanı, günlüğü baştan tarar ve her kaydı inceler.
Günlükte dayanıklı biçimde yer alan ama asıl veri dosyalarına henüz yansımamış
değişiklikleri bulup **yeniden uygular** — tıpkı bayıldıktan sonra defterini açıp
yarım kalan işi tamamlayan kişi gibi. Böylece, çökmeden hemen önce "tamamlandı"
denmiş her değişiklik, asıl veriye eksiksiz işlenmiş olur. Buna karşılık, günlüğe
tam olarak yazılmamış, yani yarım kalmış kayıtlar atılır; çünkü onların temsil
ettiği değişiklikler hiçbir zaman "tamamlandı" sayılmamıştı.

Bu kurtarma sürecinin güvenilir olması için bir özellik şarttır: yeniden uygulama
**etkisiz tekrarlanabilir (idempotent)** olmalıdır. Yani aynı değişikliği bir kez
ya da iki kez uygulamak, sonucu değiştirmemelidir. Çünkü kurtarma sırasında, bir
değişikliğin asıl veriye zaten yansıyıp yansımadığından her zaman emin olamayız;
bu yüzden onu yeniden uygulamak, eğer zaten uygulanmışsa bile, bir zarar
vermemelidir. İşte bu etkisiz tekrarlanabilirlik, sağlam bir kurtarma tasarımının
temelinde yatar.

## Yarım yazmayı yakalamak: sağlama toplamları

Peki kurtarma, günlüğün kendisinin yarım kalmış bir kaydını nasıl fark eder?
Sonuçta çökme, en son günlük kaydının tam yazılmasının ortasında da olabilir.
İşte burada **sağlama toplamı** (checksum) devreye girer. Veritabanı, her günlük
kaydının yanına, o kaydın içeriğinden hesaplanmış küçük bir doğrulama değeri
ekler. Kurtarma sırasında her kaydı okurken, bu değeri yeniden hesaplar ve kayda
iliştirilmiş olanla karşılaştırır. İkisi uyuşuyorsa kayıt sağlamdır; uyuşmuyorsa,
kayıt çökme sırasında yarım kalmış ya da bozulmuştur. Tipik olarak yalnızca
günlüğün **en sonundaki** kayıt böyle bozulabilir — çünkü çökme anına dek
öncekiler zaten tamamlanmıştı — ve veritabanı o bozuk son kaydı güvenle atıp,
ondan önceki son sağlam noktadan devam eder. Böylece yarım kalmış yazma, sessizce
bozuk veriye dönüşmek yerine, açıkça fark edilip temizlenir.

## Günlük sonsuza dek büyüyemez: denetim noktası

WAL'ın bir sorunu vardır: günlük yalnızca eklenerek büyüdüğü için, zamanla
devasa olur. Eğer hiç temizlenmezse, hem yer kaplar hem de kurtarma sırasında
baştan sona okunması gereken bir dev haline gelir. Çözüm, günlüğü periyodik
olarak güvenle kısaltmaktır.

Bu işleme **denetim noktası** (checkpoint) denir. Mantığı şudur: günlükteki
değişiklikler eninde sonunda asıl veri dosyalarına yansıtılır. Veritabanı, belirli
aralıklarla durup şundan emin olur: "şu ana kadarki tüm günlük kayıtlarının
temsil ettiği değişiklikler, artık asıl veriye güvenle işlenmiş ve dayanıklı
hale gelmiştir." Bu güvence sağlandıktan sonra, o noktaya kadarki günlük
kayıtlarına artık ihtiyaç kalmaz — çünkü kurtarma gerekse bile, onların temsil
ettiği veri zaten asıl depoda durmaktadır. İşte o eski kayıtlar artık atılabilir
ya da günlük yeniden kullanılabilir. Denetim noktası, günlük ile asıl depo
arasındaki **senkronizasyon anıdır**: o ana kadar her şeyin yerli yerinde
olduğunu mühürler ve günlüğün baştan büyümesine izin verir.

## Dayanıklılığın tayfı: katı, gevşek ve grup commit

Bölümün başında, dayanıklılığın güvenli ama yavaş olduğunu söylemiştik. Gerçek
sistemler, bu ödünleşimde tek bir noktaya saplanmaz; bir tayf üzerinde tercih
yaparlar.

Bir uçta **katı dayanıklılık** vardır: her değişiklik, "tamamlandı" denmeden
önce günlüğe yazılır ve fsync ile dayanıklı kılınır. Bu, en güçlü güvencedir —
"tamamlandı" dedikten sonra hiçbir çökme veriyi geri alamaz — ama her değişiklik
bir fsync beklediği için en yavaş olandır. Öteki uçta **gevşek dayanıklılık**
vardır: değişiklikler günlüğe yazılır ama fsync, her değişiklikte değil, arada
bir, toplu olarak yapılır. Bu çok daha hızlıdır; ama bir çökme olursa, henüz
fsync edilmemiş son birkaç değişiklik kaybolabilir. Yani gevşek kip, hızı
küçük bir veri kaybı riski karşılığında satın alır.

İki ucun arasında zarif bir orta yol vardır: **grup commit**. Fikri şudur: aynı
anda birçok değişiklik "tamamlandı" olmayı bekliyorsa, onları tek tek fsync etmek
yerine, hepsini bir araya getirip **tek bir fsync** ile birden dayanıklı kılmak.
Bir fsync, ister bir değişikliği ister yüz değişikliği boşaltsın, kabaca aynı
süreyi alır; bu yüzden yüz değişikliği tek fsync'e toplamak, gerçek dayanıklılık
güvencesinden hiç ödün vermeden verimi kat kat artırır. Bunu, postaneye her
mektup için ayrı ayrı koşmak yerine, eldeki tüm mektupları toplayıp tek seferde
götürmeye benzetebilirsiniz. Üçüncü kısımda OxiDB'nin varsayılan olarak katı
dayanıklılığı seçtiğini — her commit'i fsync ettiğini — ama toplu eklemelerde
bu maliyeti tek bir fsync'e amortize ettiğini ve istendiğinde gevşek kipe
geçilebildiğini göreceğiz.

## WAL nereye oturur

Bu bölümü, önceki bölümle bağını netleştirerek toparlayalım. WAL, beşinci
bölümdeki depolama felsefelerinin bir alternatifi değildir; onların **önünde**
duran, dayanıklılıktan ve atomiklikten sorumlu ayrı bir katmandır. İster veriyi
sayfa tabanlı bir B-ağacında yerinde güncelleyen bir motor olsun, ister
append-only bir depo olsun, ikisi de dayanıklılık ve çökme güvenliği için bir
WAL'a yaslanabilir. Asıl veri yapısı "nasıl saklayacağım" sorusunu yanıtlar; WAL
ise "değiştirirken çökersem ne olacak" sorusunu yanıtlar. İkisi birbirini
tamamlar.

Böylece, beşinci ve altıncı bölümler birlikte, bir belge veritabanının en alt
katmanını — veriyi diske güvenli biçimde yazma ve çökmeden geri dönme yeteneğini
— tamamlamış oldu. Artık verimiz hem kalıcı hem de tutarlı biçimde diskte
duruyor. Ama elimizde, çözülmemiş büyük bir sorun daha var. Birinci bölümde
saymıştık: bir milyon belge arasından aradığımız tek kaydı, diski baştan sona
taramadan nasıl buluruz? Veriyi güvenle saklamayı öğrendik; şimdi onu hızla
**bulmayı** öğrenmemiz gerekiyor. Bir sonraki bölümde, veritabanlarını taramaya
mahkûm olmaktan kurtaran o yardımcı yapılara — indekslere — eğiliyoruz.
