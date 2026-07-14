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

## Kitabın adı üzerine {-}

Fizikçiler bir asırdır *her şeyin teorisi* peşinde koşar: doğanın birbirinden
kopuk görünen kuvvetlerini tek bir çerçevede toplayan bir formül. Bu kitabın adı,
o arayışa göz kırpar — ama bir iddia olarak değil, bir soru olarak. Veri
dünyasında da uzun süre benzer bir bölünmüşlük yaşandı: belgeler bir sistemde,
tablolar başkasında, ölçüm akışları bir üçüncüsünde, dosyalar bir nesne
deposunda, sıcak geçici durum ise bir önbellek sunucusunda durdu. Her biri kendi
protokolünü, kendi işletim yükünü, kendi yedekleme planını getirdi.

*Her şeyin veritabanı* diye bir şey var mıdır? Dürüst yanıt: yoktur — ve bu
kitap size bunun neden böyle olduğunu, her modelin hangi ödünleşimin bedelini
ödediğini anlatacak. Ama bir sistemin, bu farklı veri şekillerinin her birine
kendi doğasına uygun bir motorla karşılık verip hepsini tek bir çatı altında,
tek bir bağlantı ve tek bir işletim disiplini içinde sunması mümkündür. OxiDB'nin
yaptığı budur: belge motoru, ilişkisel SQL motoru ve zaman serisi motoru; yanına
S3 uyumlu nesne depolama ve bellek-içi anahtar-değer katmanı. Tek formül değil,
bir arada yaşamayı bilen birkaç motor. Kitabın adı, o arayışın hem hedefini hem
de sınırını anmak içindir.

## Bu kitabın yöntemi {-}

Kitabın ilk iki kısmında **örnek kod yoktur**. Bu bilinçli bir tercihtir: amaç,
sizi belirli bir sözdizimine bağlamadan kavramların kendisini anlatmaktır. Bir
fikrin özünü düz metinle, benzetmelerle ve adım adım akıl yürütmeyle anlatmak,
çoğu zaman bir kod parçasından daha kalıcı bir kavrayış bırakır. Kod ezberlenir
ve unutulur; bir mekanizmanın *neden* öyle çalıştığını bir kez gerçekten
anladığınızda ise o bilgi sizinle kalır.

Üçüncü kısımda ise tutum değişir. Orada artık soyut bir ilkeyi değil, çalışan bir
sistemi anlatıyoruz; ve bir motorun yüzeyini — hangi sorguyu kabul ettiğini,
hangi yanıtı döndürdüğünü, bir işlemin nasıl açılıp kapandığını — sözcüklerle
tarif etmek, onu göstermekten hem daha uzun hem daha bulanıktır. Bu yüzden
üçüncü kısımda **bol örnek** bulacaksınız: tel üzerindeki JSON istekleri, SQL
ifadeleri, Python, C#, JavaScript ve kabuk komutları. Bu örnekler metnin yerine
geçmez; kavram önce düzyazıyla kurulur, örnek onu somutlaştırır. Hepsi de
sistemin o an çalışan sürümüne karşı denenmiştir.

Kitap kademeli ilerler. Her kavram, kendinden öncekilerin üzerine kurulur:
dayanıklılığı anlamadan işlemleri, indekslemeyi anlamadan sorgu işlemeyi tam
kavrayamazsınız. Bu yüzden bölümleri sırayla okumanız önerilir. Yine de her
bölüm, kendi başına da anlamlı olacak şekilde, gerektiğinde önceki kavramları
kısaca hatırlatarak yazılmıştır.

## Yazardan bir not: OxiDB ve bu kitap {-}

Bu kitabın üçüncü kısmında adım adım incelenen sistem, OxiDB, benim — Barış
AKIN'ın — sıfırdan tasarlayıp yazmaya başladığım bir veritabanı motorudur.
OxiDB'yi yazmaya başladığım günden bu yana, bir veritabanının içinde verdiğim her
kararın — verinin diske nasıl yerleştiğinden bir çökmeden nasıl geri dönüleceğine,
bir indeksin nasıl yapılandırılacağından bir işlemin tutarlılığının nasıl
korunacağına kadar — kitabın ikinci kısmında anlatılan o soyut ilkelerin somut
bir karşılığı olduğunu gördüm. Bu kitap, işte o deneyimden doğdu.

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

Üçüncü kısmın son bölümleri, kitabın adındaki iddiayı sınar. Belge motorunu
enine boyuna inceledikten sonra, aynı sunucunun içinde yaşayan diğer motorlara
geçeriz: satırları, tabloları ve join'leri olan **ilişkisel SQL motoru**; saniyede
yüz binlerce ölçümü sıkıştırarak yutan **zaman serisi motoru**; büyük ikili
nesneler için **S3 uyumlu nesne depolama**; ve sıcak, geçici durum için Redis'in
telini konuşan **bellek-içi anahtar-değer katmanı**. Kapanış bölümü hepsini tek
bir çalışan uygulamada buluşturur ve asıl soruyu yanıtlar: hangi veri hangi
motora gider, ve neden?

Şimdi başlayalım. İlk durağımız, sandığınızdan daha derin bir soru: aslında bir
veritabanı tam olarak nedir ve onu basit bir dosyadan ayıran nedir?
