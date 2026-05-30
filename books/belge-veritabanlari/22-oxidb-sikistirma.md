# OxiDB'de Sıkıştırma: Ölü Alan ve Otomatik Tetikleme

İşlemleri ele alırken, OxiDB'nin disk-öncelikli kipinin append-only doğasına
birkaç kez değindik. Beşinci ve on altıncı bölümlerde söylediğimiz gibi,
append-only depolama veriyi asla üzerine yazmaz; her güncelleme yeni bir kayıt
ekler ve eskisi ölü alana dönüşür. Bu ölü alan zamanla birikir ve onu geri
kazanmak gerekir. Bu bölüm, OxiDB'nin bu temizlik işini — sıkıştırmayı, onu
güvenle nasıl yaptığını ve bu kitap yazılırken eklenen otomatik tetikleyicisini —
ele alıyor. Sıkıştırma, append-only bir motorun ayrılmaz, sessiz bakım işidir.

![Şekil 22 — Sıkıştırma: ölü alanın taze dosyaya kopyalanarak geri kazanılması.](sekiller/22-oxidb-sikistirma.svg){width=80%}

## Ölü alan nereden gelir

Beşinci bölümdeki muhasebe defteri benzetmesini hatırlayalım: append-only bir
depo, hiçbir eski satırı silmez; bir kaydı güncellemek için onun yeni hâlini
defterin sonuna yazar ve en son yazılanı geçerli sayar. Bunun kaçınılmaz sonucu,
eski hâllerin defterde öylece durmaya devam etmesidir. Bir belgeyi yüz kez
güncellerseniz, disk-öncelikli kipin veri dosyasında o belgenin yüz kopyası
birikir; yalnızca sonuncusu geçerlidir, gerisi **ölü alandır**. Silmeler de
benzer biçimde, kaydı fiziksel olarak çıkarmak yerine ölü olarak işaretler.

Bu olgunun kritik bir sonucu vardır: veri dosyasının boyutu, **yaşayan verinin**
boyutuyla değil, **yapılan yazma sayısıyla** büyür. Yoğun güncelleme alan bir
koleksiyonun veri dosyası, içindeki gerçek, geçerli veri küçük kalsa bile, zamanla
çoğu ölü kayıttan oluşan bir dev haline gelebilir. Bu hem yer israfıdır hem de
on altıncı bölümde gördüğümüz okuma ve tarama maliyetlerini artırır; çünkü daha
büyük bir dosyada gezinmek gerekir. İşte bu şişkinliği gidermek, sıkıştırmanın
görevidir.

## Sıkıştırma ne yapar

Sıkıştırma, beşinci bölümde tanımladığımız temizlik işidir: veri dosyasını baştan
yazıp, yalnızca **yaşayan** kayıtları taze bir dosyaya kopyalamak; ölü kayıtları
geride bırakmak. Muhasebe defteri dolup taştığında oturup yalnızca hâlâ geçerli
satırları temiz bir deftere geçirmeye benzer. İşlem bittiğinde, taze dosya
yalnızca geçerli veriyi içerir ve eski, şişmiş dosyanın yerini alır; biriken tüm
ölü alan geri kazanılmıştır. OxiDB üzerinde yapılan ölçümlerde, yoğun güncelleme
sonrası şişmiş bir veri dosyasının, sıkıştırmayla birkaç kat küçüldüğü — ve bu
sırada tüm yaşayan verinin ve indeksli sorguların hem sıkıştırma sırasında hem de
yeniden açılışta eksiksiz kaldığı — doğrulandı.

Belleğe öncelikli kipte ise sıkıştırma farklı bir anlam taşır. O kipte
append-only bir veri dosyası yoktur; belgeler bellekteki eşlemede yerinde
güncellenir, dolayısıyla biriken bir ölü alan da yoktur. Orada "sıkıştırma",
yalnızca güncel bellek içeriğinin taze bir anlık görüntüsünü diske yazmaktan
ibarettir. Yani sıkıştırma, asıl olarak disk-öncelikli kipin bir ihtiyacıdır.

## Zor kısım: yaşayan sistemde güvenle yapmak

Sıkıştırmanın asıl zorluğu, onu veritabanı çalışmaya devam ederken, okuma ve
yazmalar sürerken güvenle yapmaktır. Burada, on birinci bölümdeki eşzamanlılık
kaygılarının somut bir örneği belirir ve OxiDB'nin çözümü öğreticidir.

Sorunu görelim. Disk-öncelikli kipte bir belgeyi okumak iki adımdır: kimlik-konum
dizininden belgenin dosyadaki konumunu bulmak ve sonra o konumdan baytları
okumak. Şimdi düşünün: bir okuyucu konumu öğrenmiş, tam o baytları okuyacakken,
sıkıştırma araya girip dosyayı baştan yazsa ve belgeler yeni dosyada başka
konumlara taşınsa ne olur? Okuyucunun elindeki konum, artık eski dosyaya aittir;
yeni dosyada o konumda bambaşka bir şey olabilir. Bu, sessiz bir veri
bozulmasıdır.

OxiDB bunu, veri dosyası tutamağı üzerinde bir okuma-yazma engeliyle çözer. Normal
işlemler — okumalar ve yazmalar — bir **paylaşımlı okuma kilidi** tutar ve bu
kilit, hem konumu bulma hem de baytları okuma adımlarının ikisini birden kapsar;
yani bir konum, her zaman onu okuduğu dosyaya karşı kullanılır, asla başka bir
dosyaya karşı değil. Sıkıştırma ise **dışlayıcı bir yazma kilidi** alır: bu kilidi
tutarken hiçbir okuma ya da yazma araya giremez; sıkıştırma, taze dosyayı kurar,
kimlik-konum dizinini yeni konumlara göre yeniden eşler ve dosyayı bütünüyle
değiştirir; ancak bundan sonra kilidi bırakır. Böylece hiçbir konum, hiçbir zaman
değiştirilmiş bir dosyaya karşı kullanılmaz. Bu, on birinci bölümdeki
eşzamanlılık ilkelerinin — paylaşımlı ve dışlayıcı erişimin dikkatli
düzenlenmesinin — gerçek bir doğruluk sorununu nasıl çözdüğünün somut bir
örneğidir.

## İndeksler neden sağ kalır

Sıkıştırmanın zarif bir yanı, indeksleri bozmadan çalışmasıdır ve bunun nedeni
öğreticidir. On sekizinci bölümde gördüğümüz gibi, OxiDB'nin alan indeksleri
belgeleri **kimliğe** göre tutar, fiziksel konuma göre değil. Sıkıştırma ise
yalnızca belgelerin dosyadaki **konumlarını** değiştirir — kimliklerini değil. Bu
yüzden veri dosyasını baştan yazmak, indeksleri geçersiz kılmaz; yalnızca
kimlikten konuma giden o küçük eşlemenin yeniden kurulması yeterlidir. Kimlik ile
fiziksel konum arasındaki bu ayrışma — indekslerin kimliğe, dizinin konuma
bakması — sıkıştırmayı, indekslere hiç dokunmadan yapılabilir kılar. İyi bir
soyutlama ayrımının, karmaşık bir işi nasıl basitleştirdiğinin güzel bir
örneğidir bu.

## Açık çağrıdan otomatik tetiklemeye

Sıkıştırmanın OxiDB'deki gelişimi, bu kitap yazılırken yaşanan iki aşamalı bir
öyküdür ve onu anlatmak, mühendislik kararlarının nasıl olgunlaştığını gösterir.

İlk aşamada, sıkıştırma **açıkça çağrılan** bir işlemdi: ne zaman gerek
duyduğunuza siz karar verir ve sıkıştırmayı elle başlatırdınız. Bu, az önce
anlattığımız tüm güvenlik özelliklerine — yaşayan sistemde güvenli yeniden yazma,
indekslerin sağ kalması — sahipti ve ölçümlerle doğrulanmıştı; ama ne zaman
çalıştırılacağına karar vermek kullanıcıya kalıyordu.

İkinci aşamada, bir **otomatik tetikleyici** eklendi. Artık OxiDB, periyodik bakım
sırasında ucuz bir ölü-alan ölçütüne bakar ve gerektiğinde sıkıştırmayı kendisi
başlatır. Ölçüt şöyledir: veri dosyası hem belirli bir **asgari boyutu**
aşmışsa hem de içeriğinin belirli bir **oranından fazlası ölüyse**, sıkıştırma
tetiklenir. Ölü oran, basitçe, dosyanın ne kadarının yaşayan veri **olmadığıyla**
ölçülür — yaşayan baytların dosya boyutuna oranı ne kadar düşükse, ölü oran o
kadar yüksektir.

İki koşulun birlikte aranması bilinçlidir. Asgari boyut koşulu, küçük dosyalar
için boşuna sıkıştırma yapılmasını engeller; çünkü küçük bir dosyada kazanılacak
yer azdır, sıkıştırmanın maliyetine değmez. Ölü oran koşulu ise, ancak gerçekten
kayda değer bir ölü alan biriktiğinde sıkıştırma yapılmasını sağlar; az miktarda
ölü alan için dosyayı baştan yazmak israf olurdu. İki eşik birden sağlandığında,
yani dosya hem yeterince büyük hem de yeterince ölüyse, sıkıştırma devreye girer.

Bu otomatik tetikleyicinin zarif bir özelliği, **kendini sınırlamasıdır**. Bir
sıkıştırma yapıldığında, ölü alan sıfırlanır; dolayısıyla ölçüt bir süre daha
sağlanmaz ve sıkıştırma boşuna tekrar tekrar tetiklenmez. Sistem, yalnızca ölü
alan yeniden birikip eşiği aştığında tekrar sıkıştırır. Bu eşikler ayarlanabilir
ve on ikinci bölümde gördüğümüz gibi per-koleksiyon belirlenebilir; istenirse
otomatik tetikleme tümüyle kapatılıp sıkıştırma elle yönetilebilir.

## Sıkıştırmanın bedeli ve dengesi

Sıkıştırma bedava değildir: tüm yaşayan veriyi baştan yazmayı gerektirir, yani
bir disk yükü taşır; ve dışlayıcı kilidi tuttuğu kısa süre boyunca diğer işlemleri
bekletir. Bu yüzden onu sürekli değil, ölü alan gerçekten büyüdüğünde yapmak
gerekir — az önceki eşiklerin amacı tam olarak budur. Sıkıştırma, beşinci bölümde
söylediğimiz gibi, arka planda yürüyen, amorti edilmiş bir bakım işidir: dosyayı
sürekli temiz tutmaya çalışmak yerine, kirlilik belli bir düzeye ulaştığında
toplu bir temizlik yapmak, hem maliyeti hem de kesintiyi en aza indirir.

## Bu bölümün bıraktığı yer

Bu bölümde, append-only bir motorun ayrılmaz bakım işini — sıkıştırmayı — OxiDB
bağlamında ele aldık. Ölü alanın nereden geldiğini ve veri dosyasını yazma
sayısıyla nasıl şişirdiğini; sıkıştırmanın yalnızca yaşayan kayıtları taze bir
dosyaya geçirip ölü alanı geri kazandığını; bunu yaşayan bir sistemde güvenle
yapmak için kullanılan okuma-yazma engelini; indekslerin kimliğe dayandığı için
neden sağ kaldığını; açık çağrıdan otomatik tetiklemeye uzanan gelişim öyküsünü
ve otomatik tetikleyicinin ölü-oran ölçütünü, kendini sınırlamasını ve dengesini
gördük.

Buraya kadar Kısım III'te, OxiDB'nin çekirdek belge motorunu — depolama,
dayanıklılık, indeks, sorgu, toplama, işlem ve sıkıştırma — eksiksiz dolaştık.
Ama on beşinci bölümde söylediğimiz gibi, OxiDB klasik bir belge veritabanının
ötesine geçen birkaç ek yetenek de sunar. Bir sonraki bölümde, bu ek yüzeylerden
başlıcalarını — tam metin aramayı, büyük ikili nesneler için nesne deposunu,
dururken şifrelemeyi ve zamanın bir noktasına geri dönmeyi sağlayan kurtarmayı —
ele alacağız.
