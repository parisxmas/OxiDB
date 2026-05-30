# Uyumluluk Katmanları ve İstemciler

Önceki bölümün sonunda, bir veritabanının ona erişen uygulamalar kadar değerli
olduğunu söylemiştik. Buraya kadar OxiDB'yi hep veriyi sağlayan taraftan — motor
ve sunucu olarak — tanıdık. Bu bölüm, madalyonun öteki yüzüne, OxiDB'ye **nasıl
erişildiğine** bakıyor: farklı programlama dillerinden kullanmayı sağlayan istemci
kütüphanelerine ve OxiDB'yi başka ekosistemlerin protokolleriyle konuşturan
uyumluluk katmanlarına. Bu bölüm, on beşinci bölümde tanıttığımız "tek çekirdek,
çok yüz" felsefesinin en somut görünümüdür.

## Çekirdeğe iki kapı

Birinci bölümde, bir veritabanına iki uçtan erişilebileceğini söylemiştik: gömülü
olarak ya da bir sunucu üzerinden. OxiDB'ye erişim de bu iki kapıdan birinden
geçer. **Gömülü** kapıda, uygulamanız OxiDB ile aynı süreçte çalışır ve veriye
doğrudan, bir ağ aradan geçmeden, işlev çağrılarıyla erişir; bu çok hızlıdır ve
hiçbir sunucu kurulumu gerektirmez. **Sunucu** kapısında ise uygulamanız, yirmi
dördüncü bölümdeki OxiWire protokolüyle ağ üzerinden bağlanır; bu, aynı veriye
birçok uygulamanın aynı anda erişmesini sağlar.

İstemci kütüphanelerinin işi, bu iki kapı arasındaki farkı uygulamadan
**gizlemektir**. Bir istemci kütüphanesi kullandığınızda, ister gömülü ister
sunucu kipinde çalışın, karşınıza aynı kavramlar — koleksiyonlar, sorgular,
indeksler, toplama, işlemler — aynı biçimde çıkar; çünkü hepsi, on beşinci
bölümde söylediğimiz gibi, aynı çekirdek motorun farklı kapılarıdır.

## Diğer dillere köprü: FFI

OxiDB'nin çekirdeği Rust dilinde yazılmıştır; peki başka dillerden nasıl
kullanılır? Yanıt, **C uyumlu bir köprü** katmanındadır. Çekirdek, C dilinin
çağırma kurallarına uyan bir arayüz sunar; ve neredeyse her programlama dili, C
ile konuşabildiği için, bu köprü sayesinde OxiDB'yi gömülü olarak çağırabilir. Bu,
yaygın bir tekniktir: düşük seviyeli, performans-kritik bir çekirdeği bir kez
yazmak ve onu, bu C köprüsü üzerinden, birçok yüksek seviyeli dile açmak. Böylece
OxiDB'nin gömülü hızı, yalnızca Rust'a değil, başka dillere de sunulur.

## İstemci kütüphaneleri

Bu köprünün ve sunucu protokolünün üzerine kurulu istemci kütüphaneleri, OxiDB'yi
çeşitli programlama dillerinden — örneğin Python, Go, .NET ve JavaScript'ten —
kullanılabilir kılar. Her istemci, ya gömülü köprüye doğrudan bağlanır, ya da
sunucuya ağ üzerinden konuşur; bazı istemciler, tek bir arayüzün arkasında her iki
kipi birden sunar, böylece uygulamanız gömülüden sunucuya geçerken kodunu
değiştirmez.

İstemcilerin ortak özelliği, hepsinin aynı komut dağarcığını yansıtmasıdır; çünkü
hepsi aynı çekirdeğe konuşur. Bu birörnekliğin somut bir örneği, bu kitap
yazılırken yaşandı. OxiDB'ye yeni bir yetenek — koleksiyonları belirli depolama
seçenekleriyle oluşturma — eklendiğinde, bu yetenek önce çekirdekte ve sunucu
protokolünde tanımlandı; sonra istemcilerin her birine, bu komutu çağıran küçük
bir sarmalayıcı eklendi. Python'da bir, Go'da bir, .NET'te bir, JavaScript'te bir.
Hepsi aynı altta yatan komutu çağırıyordu; yalnızca her dilin kendi deyimine —
Python'un adlandırılmış argümanlarına, Go'nun işlev-seçenek desenine — uygun bir
biçim alıyordu. Bu, istemcilerin neden hepsinin aynı kavramları sunduğunu güzel
gösterir: onlar bağımsız veritabanları değil, tek bir motorun farklı dillerdeki
kapılarıdır.

## Uyumluluk katmanları: başka protokolleri konuşmak

İstemci kütüphaneleri, uygulamaların OxiDB'nin kendi protokolüyle konuşmasını
kolaylaştırır. Ama bazı uygulamalar, zaten **başka bir protokolü** konuşacak
biçimde yazılmıştır ve OxiWire öğrenmek istemez. İşte burada OxiDB'nin uyumluluk
katmanları devreye girer: OxiDB'yi, var olan araçların ve istemcilerin tanıdığı
yüzlerle sunarak, onların az değişiklikle ya da hiç değişmeden çalışmasını sağlar.

Birinci uyumluluk yüzü, bir **bellek-içi anahtar-değer katmanıdır**. Bu katman,
yaygın bir önbellek protokolüyle uyumlu çalışır; böylece o protokolü konuşan
mevcut önbellek istemcileri ve araçları, OxiDB'ye doğrudan bağlanabilir. İkinci
bölümde tanıdığımız anahtar-değer modelini hatırlayın; OxiDB, kendi belge
motorunun üzerine, bu sade ve yaygın anahtar-değer yüzünü de giydirir ve sıralı
kümeler, yayınla-abone ol gibi tanıdık yetenekleri sunar.

İkinci uyumluluk yüzü, bir **mesajlaşma protokolü** desteğidir. Bu, nesnelerin
interneti gibi, çok sayıda aygıtın yayınla-abone ol biçiminde haberleştiği
senaryolar içindir; ve anahtar-değer katmanının yayınla-abone ol kanallarıyla
çapraz çalışabilir, yani bir protokolden yayınlanan bir mesaj diğerinden
dinlenebilir.

Üçüncü ve belki en geniş uyumluluk yüzü, **web yüzeyidir**: doğrudan bir HTTP
arayüzü, gerçek zamanlı bir abonelik kanalı, bir kimlik sistemi ve belge düzeyinde
kurallar. Yirmi üçüncü ve on beşinci bölümlerde değindiğimiz bu Firebase benzeri
yüz, web ve mobil uygulamaların, araya bir sunucu katmanı koymadan, doğrudan
OxiDB ile — gerçek zamanlı güncellemeler, kimlik doğrulama ve belge başına erişim
kurallarıyla — çalışmasını mümkün kılar. JavaScript istemcisi, tam da bu web
yüzeyi üzerinden konuşur.

## Tek çekirdek, çok yüz

Tüm bu erişim biçimlerinin — gömülü çağrı, OxiWire sunucusu, dil istemcileri,
uyumluluk katmanları — ortak bir noktası vardır ve o, on beşinci bölümdeki
felsefenin özüdür: hepsi aynı çekirdek motora oturur. Python istemcisiyle yazılan
bir belge, web yüzeyinden okunabilir; anahtar-değer katmanıyla konan bir değer,
aynı motorun verisidir. Hangi kapıdan girerseniz girin, ardındaki depolama,
indeks, işlem ve dayanıklılık mekanizmaları aynıdır. OxiDB'nin bu genişliği —
tek bir motoru bu kadar çok yüzle sunması — onu klasik bir belge veritabanından
ayıran belirgin özelliklerinden biridir.

Bu genişliğin bir bedeli olduğunu da dürüstçe söylemek gerekir. Her yüz, bakımı
gereken bir yüzeydir; ve uyumluluk katmanları, çoğu zaman yabancı protokolün
**tamamını** değil, "yeterli" bir altkümesini konuşur — yani o protokolün her
inceliğini değil, en yaygın kullanımını destekler. Bu, makul bir ödünleşimdir:
amaç, başka bir protokolün birebir kopyası olmak değil, o ekosistemin araçlarının
çoğunun OxiDB ile çalışmasını sağlamaktır. Yine de, bir uyumluluk katmanını
kullanırken, onun bir köprü olduğunu — hedef sistemin yerine geçen tam bir taklit
değil — akılda tutmak gerekir.

## Bu bölümün bıraktığı yer

Bu bölümde, OxiDB'ye erişimin iki kapısını — gömülü ve sunucu — ve istemci
kütüphanelerinin bu farkı nasıl gizlediğini gördük. Çekirdeği başka dillere açan C
köprüsünü; istemcilerin neden hepsinin aynı kavramları sunduğunu ve bunun bu kitap
yazılırken eklenen bir yetenekle nasıl somutlaştığını izledik. Anahtar-değer,
mesajlaşma ve web olmak üzere üç uyumluluk yüzünü ve OxiDB'nin başka ekosistemlerin
araçlarını nasıl ağırladığını gördük. Hepsinin aynı çekirdeğe oturduğunu —
"tek çekirdek, çok yüz" — ve bu genişliğin dürüst bedelini de gördük.

Böylece, OxiDB'nin tüm katmanlarını — depolamadan dayanıklılığa, indeksten
sorguya, işlemden ölçeklendirmeye, sunucudan istemcilere — dolaşmış olduk. Geriye,
kitabı kapatmadan önce bir adım geri çekilip bütüne bakmak kaldı. Son bölümde,
OxiDB'nin bu kitap boyunca birçok kez değindiğimiz bellek optimizasyonunu bir
araya getirecek, onu MongoDB ile karşılaştıran ölçümleri dürüstçe
değerlendirecek ve baştan beri izlediğimiz ilkelerin gerçek bir sistemde nasıl
bir bütün oluşturduğu üzerine düşüneceğiz.
