# Önsöz {-}

Bir veritabanı, ilk bakışta sıradan bir araçtır: veriyi koyarsınız, sonra geri
alırsınız. Oysa bu basit vaadin altında, bilgisayar biliminin en zarif ve en
zorlu problemlerinden bazıları yatar. Elektrik kesildiğinde verinin kaybolmaması
nasıl garanti edilir? Binlerce kullanıcı aynı anda aynı kaydı değiştirmeye
kalktığında düzen nasıl korunur? Milyonlarca belge arasından aradığınız tek
kaydı, diski baştan sona taramadan saniyenin binde birinde nasıl bulursunuz?
Veri tek bir makineye sığmaz hale geldiğinde, onu nasıl bölersiniz ve
böldüğünüzde tutarlılığı nasıl sürdürürsünüz? Bu kitap, işte bu soruların
peşinden gider.

## Bu kitap ne anlatıyor {-}

Kitap iki yolculuğu birleştirir. Birincisi **kavramsal** yolculuktur: belge
veritabanlarının ne olduğunu, neden var olduklarını ve içeride nasıl
çalıştıklarını temelden kurar. Veriyle başlar; veriyi kalıcı kılmanın neden zor
olduğunu, veri modellerinin tarih boyunca nasıl evrildiğini, belge modelinin bu
evrimde nereye oturduğunu anlatır. Ardından bir belge veritabanının iç
mimarisini katman katman açar: verinin diske nasıl yazıldığı, çökmeden nasıl
kurtarıldığı, indekslerin aramayı nasıl hızlandırdığı, sorguların nasıl
işlendiği, işlemlerin tutarlılığı nasıl koruduğu ve sistemin tek makinenin
ötesine nasıl ölçeklendiği.

İkinci yolculuk **somut** olandır: bu kavramların gerçek bir sistemde nasıl
hayata geçtiğini, OxiDB adlı bir belge veritabanı motoru üzerinden adım adım
gösterir. Soyut bir ilkeyi öğrendikten sonra, onun gerçek bir mühendislik
kararına nasıl dönüştüğünü, hangi ödünleşimlerin yapıldığını ve neden öyle
yapıldığını görürsünüz. Böylece kitap yalnızca "ne" değil, "neden böyle"
sorusunu da yanıtlar.

## Bu kitap kime göre {-}

Bu kitap, veritabanlarını kullanan ama içlerinde ne olup bittiğini merak eden
yazılım geliştiricileri; sistemlerin nasıl kurulduğunu derinlemesine anlamak
isteyen mühendisler; ve bir veri sisteminin altındaki ilkeleri kavramak isteyen
öğrenciler için yazıldı. Belirli bir programlama diline ya da belirli bir
ürünün kullanımına hâkim olmanız gerekmiyor. Bilgisayarların nasıl çalıştığına
dair temel bir aşinalık — bir dosyanın diskte durduğunu, belleğin geçici
olduğunu, bir programın komutları sırayla yürüttüğünü bilmek — yeterlidir.

## Bu kitabın yöntemi {-}

Bilinçli bir tercihle, kitapta **örnek kod yoktur**. Amaç, sizi belirli bir
sözdizimine bağlamadan, kavramların kendisini anlatmaktır. Bir fikrin özünü
düz metinle, benzetmelerle ve adım adım akıl yürütmeyle anlatmak, çoğu zaman
bir kod parçasından daha kalıcı bir kavrayış bırakır. Kod ezberlenir ve
unutulur; bir mekanizmanın *neden* öyle çalıştığını bir kez gerçekten
anladığınızda ise o bilgi sizinle kalır.

Kitap kademeli ilerler. Her kavram, kendinden öncekilerin üzerine kurulur:
dayanıklılığı anlamadan işlemleri, indekslemeyi anlamadan sorgu işlemeyi tam
kavrayamazsınız. Bu yüzden bölümleri sırayla okumanız önerilir. Yine de her
bölüm, kendi başına da anlamlı olacak şekilde, gerektiğinde önceki kavramları
kısaca hatırlatarak yazılmıştır.

## Yazardan bir not: OxiDB ve bu kitap {-}

Bu kitabın üçüncü kısmında adım adım incelenen sistem, OxiDB, benim — Barış
AKIN'ın — sıfırdan tasarlayıp yazmaya başladığım bir belge veritabanı motorudur.
OxiDB'yi yazmaya başladığım günden bu yana, bir veritabanının içinde verdiğim her
kararın — verinin diske nasıl yerleştiğinden bir çökmeden nasıl geri dönüleceğine,
bir indeksin nasıl yapılandırılacağından bir işlemin tutarlılığının nasıl
korunacağına kadar — kitabın ikinci kısmında anlatılan o soyut ilkelerin somut
bir karşılığı olduğunu gördüm. Bu kitap, işte o deneyimden doğdu.

OxiDB'nin geliştirilmesi, yalnız bir uğraş da değildi. Sistemin kodu, Anthropic'in
yapay zekâ modelleriyle — Claude'un Opus serisiyle — birlikte, bir eşlik ve
yardımlaşma içinde yazıldı. Mimari kararlar tartışıldı, ödünleşimler birlikte
değerlendirildi, kod birlikte üretildi ve sınandı. Bu kitabın iki yazarlı oluşu
(Barış AKIN ve Anthropic) da bu ortak çalışmanın doğal bir yansımasıdır.

Önemli bir noktanın altını çizmek isterim: bu kitap, OxiDB'nin **gerçek kod
tabanına** (codebase) dayanılarak yazılmıştır. Üçüncü kısımda OxiDB hakkında
okuyacağınız her mekanizma — depolama biçimleri, yazma-öncesi günlüğün kayıt
düzeni, indeks yapıları, iyimser eşzamanlılık denetimi, sıkıştırma, küme
replikasyonu — bir tasarım niyetinden ya da tanıtım metninden değil, sistemin o an
çalışan kaynağından alınmış ve ona karşı doğrulanmıştır. Bir ödünleşim
anlatıldığında o ödünleşim gerçekten verilmiştir; bir sayı verildiğinde o sayı
gerçekten ölçülmüştür. Amacım, kavram ile çalışan kod arasındaki mesafeyi tümüyle
kapatmaktı — okuduğunuz ilke ile onu hayata geçiren satırlar arasında hiçbir
kopukluk kalmasın diye.

## Kitabın düzeni {-}

Kitap üç kısma ayrılır. **Birinci Kısım** temelleri kurar: veri ve veritabanı
kavramı, veri modellerinin tarihi ve belge modelinin doğası. **İkinci Kısım**,
herhangi bir belge veritabanının içeride nasıl çalıştığını genel ilkeler
düzeyinde anlatır — depolama, dayanıklılık, indeksleme, sorgu, işlemler ve
ölçeklendirme. **Üçüncü Kısım**, tüm bu ilkelerin OxiDB'de nasıl somutlaştığını
adım adım gösterir.

Şimdi başlayalım. İlk durağımız, sandığınızdan daha derin bir soru: aslında bir
veritabanı tam olarak nedir ve onu basit bir dosyadan ayıran nedir?
