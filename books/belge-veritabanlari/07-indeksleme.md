# İndeksleme: B-Ağacı İndeksleri, Bileşik İndeksler ve Ters İndeks

Önceki iki bölümde veriyi diske güvenle yazmayı ve çökmeden geri dönmeyi
öğrendik. Artık verimiz kalıcı ve tutarlı biçimde duruyor. Ama birinci bölümde
saydığımız sorunlardan en görünür olanı hâlâ çözülmedi: bir milyon belge
arasından aradığımız tek kaydı, diski baştan sona taramadan nasıl buluruz? Bu
bölüm, veritabanlarını taramaya mahkûm olmaktan kurtaran o yardımcı yapılara —
indekslere — eğiliyor. İndeksler, bir veritabanının hızının büyük bölümünün
geldiği yerdir; onlarsız her soru, tüm veriyi okumayı gerektirirdi.

## Tarama sorunu

Bir koleksiyonda bir milyon kullanıcı belgesi olduğunu ve belirli bir e-posta
adresine sahip olanı aradığımızı düşünelim. Hiçbir yardımcı yapı yoksa, tek
seçeneğimiz belgeleri tek tek okuyup e-posta alanına bakmaktır. Aradığımız belge
şanslıysa başlarda, şanssızsak en sonda; ortalama olarak yarım milyon belgeyi
okumamız gerekir. Buna **tam tarama** (full scan) denir ve maliyeti veriyle
birlikte büyür: veri iki katına çıkınca arama da iki kat yavaşlar. Üstelik aynı
aramayı her tekrarladığınızda baştan tararsınız; sistem hiçbir şey öğrenmez.

Bu, kabul edilemez bir durumdur. Bir milyon, hatta bir milyar kayıt arasından
aradığımızı, neredeyse anında bulabilmeliyiz. Bunun için, verinin yanı sıra,
aramayı hızlandıran ayrı bir yapı tutmamız gerekir.

## İndeks nedir: kitabın arkasındaki dizin

İndeksin ne olduğunu anlamak için, elinizdeki bu kitabın arkasındaki dizini
düşünün. "fsync" kavramının nerede geçtiğini bulmak istediğinizde, kitabı baştan
sona okumazsınız; arkadaki dizine bakar, "fsync" sözcüğünün yanında yazan sayfa
numarasını görür ve doğrudan o sayfaya gidersiniz. Dizin, kitabın içeriğinin bir
kopyası değildir; ondan **türetilmiş**, yalnızca "hangi kavram hangi sayfada"
bilgisini sıralı biçimde tutan, küçük ve aranabilir bir yapıdır.

Bir veritabanı indeksi tam olarak budur: belirli bir alanın değerlerinden, o
değere sahip belgelerin **konumuna** giden bir eşleme. E-posta alanı için bir
indeksimiz varsa, aradığımız e-posta adresini bu indekste buluruz ve indeks bize
o adrese sahip belgenin nerede olduğunu doğrudan söyler. Bir milyon belgeyi
taramak yerine, küçük ve düzenli indeks yapısında birkaç adımda sonuca ulaşırız.
İndeks, asıl veriden türetilmiştir — yani onda yeni bir bilgi yoktur, veri zaten
belgelerin içindedir — ama o bilgiyi, aramaya uygun bir düzende yeniden
düzenleyerek, bulmayı hızlandırır.

## İndeksin bedeli: bedava hız yoktur

İndeksler sihirli görünür, ama bedelsiz değildir ve bu bedeli görmek, onları
doğru kullanmanın ön koşuludur. İki maliyet vardır.

Birincisi **yazma maliyetidir**. İndeks, asıl veriden türetildiği için, veri her
değiştiğinde indeksin de güncellenmesi gerekir. Yeni bir belge eklediğinizde,
yalnızca belgeyi yazmakla kalmaz, o belgenin indekslenen her alanı için ilgili
indekse de bir giriş eklersiniz. Bir belgeyi sildiğinizde ya da indekslenen bir
alanını değiştirdiğinizde, indeksleri de buna göre düzeltmeniz gerekir.
Dolayısıyla her indeks, okumayı hızlandırırken yazmayı bir miktar yavaşlatır.
Bir koleksiyonda ne kadar çok indeks varsa, her yazma o kadar çok ek iş doğurur.

İkincisi **yer maliyetidir**: her indeks, diskte ek yer kaplar.

Bu maliyetler, basit ama önemli bir ilkeye götürür: **her alanı indekslemezsiniz;
yalnızca üzerinde sık arama yaptığınız alanları indekslersiniz.** Hangi alanları
indeksleyeceğiniz, üçüncü bölümdeki o değişmez ilkeye geri döner — erişim
örüntülerinize bağlıdır. Hangi sorguları sık soruyorsanız, o sorguların
süzdüğü alanları indekslersiniz. "Her ihtimale karşı her şeyi indeksleyelim"
yaklaşımı, yazmaları boğan ve yer israf eden yaygın bir hatadır.

## Sıralı indeksler: hem eşitlik hem aralık

İndeksin *ne* olduğunu anladık; peki *nasıl* yapılır? En yaygın ve en güçlü
indeks türü, değerleri **sıralı** tutan yapılardır — beşinci bölümde tanıdığımız
B-ağacının indeks olarak kullanılan biçimi. Burada B-ağacı, asıl veriyi değil,
**indekslenen alanın değerlerini** sıralı biçimde tutar; her değerin yanında, o
değere sahip belgelerin konumu durur.

Değerleri sıralı tutmanın üç ayrı kazancı vardır ve bu, sıralı indeksleri bu
kadar değerli kılan şeydir. Birincisi **eşitlik aramasıdır**: belirli bir değere
sahip kayıtları, sıralı yapıda hızlıca buluruz. İkincisi **aralık aramasıdır**:
"şu değerle bu değer arasındaki tüm kayıtlar" sorusunu, sıralı yapıda o aralığın
başına gidip yan yana ilerleyerek yanıtlarız — değerler zaten sıralı olduğu için
bu doğaldır. Üçüncüsü ve çoğu zaman gözden kaçanı, **sıralı getirmedir**: bir
sorgu sonuçları belirli bir alana göre sıralı isterse ve o alanın sıralı bir
indeksi varsa, sıralama işini ayrıca yapmaya gerek kalmaz — indeks zaten o sırayı
tutmaktadır. "Şu alana göre sıralı ilk on kayıt" gibi bir istek, tüm veriyi
sıralamak yerine, indeksin başından on adım yürüyerek yanıtlanabilir. Üçüncü
kısımda OxiDB'nin sıralamayı tam da böyle, indeks üzerinden, taramadan
yaptığını göreceğiz.

Sıralı indeksin bir alternatifi, değerleri sıralı tutmayan ama eşitlik aramasını
çok hızlı yapan **karma (hash) indekstir**. Karma indeks, bir değeri doğrudan
konumuna eşler; eşitlik aramasında çok hızlıdır ama değerleri sıralı tutmadığı
için aralık sorgularını ya da sıralı getirmeyi yapamaz. Yalnızca tam eşleşme
aradığınız ve aralık ya da sıralama gerekmediği durumlarda yeterlidir. Sıralı
indeksler ise daha genel amaçlıdır; bu yüzden veritabanlarının çoğunlukla
yaslandığı yapı onlardır.

## Seçicilik: her indeks aynı ölçüde işe yaramaz

Bir indeksin ne kadar işe yaradığı, indekslenen alanın **seçiciliğine** bağlıdır.
Seçicilik, bir değerin kaç belgeyle eşleştiğiyle ilgilidir. Bir alanın her değeri
yalnızca birkaç belgeyle eşleşiyorsa — örneğin e-posta adresi, ki neredeyse her
biri benzersizdir — indeks muhteşem çalışır: aradığınız değeri bulur ve sizi
doğrudan o birkaç belgeye götürür.

Ama bir alanın yalnızca birkaç farklı değeri varsa, indeksin yararı azalır. Bir
"aktif mi" alanını düşünün: değeri ya doğru ya yanlıştır. Bu alanı indekslerseniz,
"aktif olanları getir" sorgusu, belki belgelerin yarısını seçer; yarım milyon
belgeyi getirmek için indeksten geçmek, neredeyse tüm veriyi taramaktan pek
de hızlı değildir. Düşük seçicilikli alanları indekslemek, çoğu zaman bedelini
hak etmez. İyi bir indeks, çok sayıda belge arasından **azını** ayıklayan
alanlar üzerine kurulur.

## Bileşik indeksler: birden çok alan birlikte

Sorgular çoğu zaman tek bir alanı değil, birkaç alanı birden süzer: "şu
bölgedeki, şu yaş aralığındaki, şu durumdaki kullanıcılar." Bunun için **bileşik
indeks** (composite index) kullanılır: tek bir alan yerine, birkaç alanın
birleşimi üzerine kurulu bir indeks.

Bileşik indeksin inceliği, alanların **sırasının** önemli olmasıdır. Bir
telefon rehberini düşünün: kayıtlar önce soyada, aynı soyad içinde ada göre
sıralanmıştır. Bu rehber, "soyadı Yılmaz olanlar" ya da "soyadı Yılmaz, adı Ali
olanlar" sorularını kolayca yanıtlar; çünkü sıralama önce soyada göredir. Ama
"adı Ali olanlar" sorusunu — soyadından bağımsız olarak — kolayca yanıtlayamaz,
çünkü Ali'ler tüm rehbere dağılmıştır. Bileşik indeks de tıpkı böyledir:
(bölge, yaş) üzerine kurulu bir indeks, yalnızca bölgeye göre ya da bölge artı
yaşa göre aramayı hızlandırır; ama yalnızca yaşa göre aramaya pek yaramaz. Buna
**önek kuralı** denir: bileşik indeks, alan sırasının baştan başlayan bir
önekini kullanan sorgulara yarar. Bu yüzden bileşik indekste alanları hangi
sırayla dizeceğiniz, hangi sorguları hızlandıracağınızı belirleyen önemli bir
tasarım kararıdır.

## Kapsayan indeksler: belgeye hiç dokunmamak

İndekslerin zarif bir gücü daha vardır. Normalde bir indeks size belgenin
**konumunu** verir; sonra o konuma gidip belgeyi okursunuz. Ama bazen, sorgunun
ihtiyaç duyduğu tüm bilgi zaten indeksin içinde durur. O zaman belgeye hiç gitmeye
gerek kalmaz; yanıt, yalnızca indeksten üretilir.

En arı örneği saymadır. "Şu bölgede kaç kullanıcı var?" sorusunu düşünün. Eğer
bölge alanının bir indeksi varsa, o indekste her bölgenin altında hangi belgelerin
olduğu zaten yazılıdır; saymak için yalnızca o girişleri saymak yeterlidir,
belgelerin hiçbirini açmaya gerek yoktur. Sorgunun ihtiyaç duyduğu her şeyi
indeksin tek başına sağlamasına **kapsama** (covering) denir ve bir sorgunun
verilebilecek en hızlı yanıtlarından birini doğurur: hiç belge okumadan, yalnızca
indeksten. Üçüncü kısımda OxiDB'nin saymayı tam da böyle, belgelere hiç dokunmadan
yaptığını göreceğiz.

İndekslerin bir yan faydasını da burada anmak gerekir: **benzersizlik
güvencesi**. Bir alanı "benzersiz indeks" olarak işaretlerseniz, indeks o alanda
aynı değerin iki kez yazılmasını engeller. Böylece indeks yalnızca aramayı
hızlandırmakla kalmaz, bir veri bütünlüğü kuralını da dayatmış olur.

## Ters indeks: metni aranabilir kılmak

Şimdiye dek anlattığımız indeksler, bir alanın **tam değeriyle** çalışır: e-posta
adresi şuna eşit, yaş şu aralıkta. Ama bambaşka bir arama türü vardır: metnin
**içinde** sözcük aramak. "İçinde 'veritabanı' sözcüğü geçen tüm belgeleri getir"
demek istediğinizde, alanın tam değerine bakan indeksler işe yaramaz; çünkü siz
tam eşleşme değil, metnin içinde geçen bir sözcüğü arıyorsunuz. Bu ihtiyaç için
tasarlanmış, tümüyle farklı bir yapı vardır: **ters indeks** (inverted index).

Ters indeksin fikri, sıradan indeksin tersini yapmaktır. Sıradan indeks
"belge → içindeki değer" ilişkisini tutarken, ters indeks "sözcük → o sözcüğü
içeren belgeler" ilişkisini tutar. Bir kitabın arkasındaki dizine yine
dönelim; orada her kavramın yanında, o kavramın geçtiği sayfaların listesi
vardır. Ters indeks tam olarak budur, ama metindeki her anlamlı sözcük için.
Bir belge eklendiğinde, metni anlamlı sözcüklere ayrılır (buna parçalama denir)
ve her sözcüğün altına o belge eklenir. Bir sözcüğü aradığınızda, ters indeks
size o sözcüğü içeren tüm belgelerin listesini doğrudan verir — metinleri
taramadan.

Metin aramanın sıradan aramadan bir farkı daha vardır: **alaka düzeyi**. Bir
sözcüğü içeren yüzlerce belge olabilir, ama hepsi aynı ölçüde ilgili değildir.
O sözcüğün sık geçtiği, kısa bir belge, o sözcüğün bir kez geçtiği uzun bir
belgeden büyük olasılıkla daha ilgilidir. Bu yüzden metin arama sistemleri,
sonuçları yalnızca bulmakla kalmaz, **sıralar** da: hangi belgenin sorguya daha
alakalı olduğunu, sözcüğün belgede ne sıklıkta geçtiği ve genel olarak ne kadar
yaygın bir sözcük olduğu gibi ölçütlere bakarak puanlar. Üçüncü kısımda OxiDB'nin
tam metin aramayı, böyle bir ters indeks ve alaka puanlaması üzerine kurduğunu
göreceğiz.

## İndeksler de dayanıklı olmalı

Son bir bağlantı kuralım. İndeksler asıl veriden türetilmiş olsa da, her
sorgudan önce baştan kurulamayacak kadar pahalıdır; bu yüzden onların da kalıcı
olması gerekir. İndeksler de, asıl veri gibi, diske yazılır ve bir önceki
bölümde gördüğümüz dayanıklılık kaygılarının kapsamına girer. Bir çökme sonrası,
indekslerin de asıl veriyle tutarlı bir duruma getirilmesi gerekir; kimi
sistemler indeksleri günlükten kurtarır, kimileri ise gerekirse asıl veriyi
tarayarak yeniden kurar. İndeks ile veri arasındaki bu tutarlılığın korunması,
sağlam bir veritabanının sessiz ama kritik bir görevidir.

## Bu bölümün bıraktığı yer

Bu bölümde, veritabanlarını taramaya mahkûm olmaktan kurtaran indeksleri
tanıdık. Bir indeksin, asıl veriden türetilmiş, aramayı hızlandıran ayrı bir
yapı olduğunu; bunun karşılığında yazmayı yavaşlatıp yer kapladığını; bu yüzden
seçici biçimde, erişim örüntülerine göre kurulduğunu gördük. Sıralı indekslerin
eşitlik, aralık ve sıralı getirmeyi birden desteklediğini; karma indekslerin
yalnızca eşitlikte hızlı olduğunu; seçiciliğin bir indeksin yararını belirlediğini;
bileşik indekslerin alan sırasıyla birden çok koşulu hızlandırdığını; kapsayan
indekslerin belgeye hiç dokunmadan yanıt üretebildiğini; ve ters indekslerin
metni sözcük düzeyinde aranabilir kıldığını öğrendik.

Ama bir indeks, tek başına, yalnızca bir araçtır. Bir kullanıcının sorusunu —
"şu bölgedeki, şu yaş aralığındaki, adında şu geçen kullanıcıları, şuna göre
sıralı getir" — alıp, bu soruyu hangi indekslerin işe yarayacağına karar veren,
veriyi en az iş yaparak süzecek bir plana dönüştüren ayrı bir akıl gerekir. O
akıl, sorgu işleyicidir. Bir sonraki bölümde, bir sorunun bir yanıta nasıl
dönüştüğünü — ayrıştırmadan planlamaya, indeks seçiminden yürütmeye — adım adım
izleyeceğiz.
