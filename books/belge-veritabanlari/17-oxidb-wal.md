# OxiDB'de WAL, Dayanıklılık ve Kurtarma; Katı ve Gevşek Senkronizasyon

Önceki bölümde, OxiDB'nin belgeleri nasıl sakladığını gördük. Ama altıncı
bölümde öğrendiğimiz gibi, bir veriyi diske yazmak, onun çökmeye karşı güvende
olduğu anlamına gelmez. Bu bölüm, OxiDB'nin bir yazmanın gerçekten dayanıklı
olduğundan nasıl emin olduğunu, çökmeden nasıl kurtulduğunu ve altıncı bölümde
tanıdığımız o dayanıklılık tayfında — her commit'i diske zorlamak ile arada bir
zorlamak arasında — nereye oturduğunu ele alıyor. Bu, OxiDB'nin verdiği en temel
sözlerden birinin — "tamamlandı dediysem, kaybolmaz" — ardındaki düzenektir.

## OxiDB'nin yazma-öncesi günlüğü

Altıncı bölümdeki ilkeyi hatırlayalım: asıl veriyi değiştirmeden önce, ne
yapacağını ayrı bir günlüğe yaz ve o günlüğü dayanıklı kıl; dayanıklılık
günlükten gelir, asıl güncelleme tembelce yapılır. OxiDB, bu ilkeyi doğrudan
uygular. Her koleksiyonun, yalnızca sona eklenerek büyüyen bir yazma-öncesi
günlüğü vardır.

Bu günlüğe yazılan her kayıt, altıncı bölümde anlattığımız iki korumayı taşır.
Birincisi, kaydın içeriğinden hesaplanan bir **sağlama toplamıdır** (CRC); bu,
kurtarma sırasında yarım kalmış ya da bozulmuş bir kaydı yakalamayı sağlar.
İkincisi, kaydın hangi işleme ait olduğunu belirten bir **işlem kimliğidir**; bu,
ileride işlemleri ele alırken önemli olacak. Bir yazma geldiğinde, OxiDB önce bu
kaydı günlüğe ekler ve onu dayanıklı kılar; ancak ondan sonra, değişikliği bir
önceki bölümdeki depolama katmanına — belleğe öncelikli kipte bellekteki
eşlemeye, disk-öncelikli kipte `.bdat` dosyasına — ve indekslere uygular. Yani
günlük her zaman asıl veriden **önce** dayanıklı olur; çökme tam o sırada olsa
bile, niyet günlükte kayıtlıdır.

Burada altıncı bölümün bir incelğini somut görürüz. Bir kaydı "dayanıklı kılmak",
yalnızca diske göndermek değil, oraya gerçekten oturduğundan emin olmaktır;
altıncı bölümde, bazı sistemlerde sıradan boşaltmanın veriyi yalnızca disk
denetleyicisinin önbelleğine bıraktığını söylemiştik. OxiDB bu konuda tavizsizdir:
dayanıklılık gerektiğinde, veriyi fiziksel ortama gerçekten işleyen, tam bir
boşaltma kullanır. Bu güçlü garanti, az sonra göreceğimiz gibi, bir hız bedeli
taşır — ama verdiği söz de o kadar güçlüdür.

## Katı dayanıklılık: varsayılan tercih

Altıncı bölümde dayanıklılığın bir tayf üzerinde tercih edildiğini söylemiştik.
OxiDB, bu tayfın katı ucunu **varsayılan** olarak seçer: her commit, "tamamlandı"
denmeden önce günlüğe yazılır ve diske gerçekten boşaltılır. Bu, en güçlü
güvencedir — bir işlem tamamlandı dendikten sonra hiçbir çökme onu geri alamaz —
ve OxiDB'nin güvenliğe verdiği önceliği yansıtır.

Ama bu güvencenin dürüstçe konuşulması gereken bir maliyeti vardır ve bu maliyet,
OxiDB üzerinde yapılan ölçümlerde açıkça görülür. Tek bir belgeyi güncelleyen tek
bir işlem düşünün. Bu işlem, tek başına bir günlük yazması ve tek bir tam
boşaltma gerektirir; ve tam boşaltma, fiziksel ortama gerçek bir yazma olduğu
için, bu makinede milisaniyeler mertebesinde — bilgisayar ölçeğinde upuzun bir
süre — alır. Ölçümlerde, katı kipte tek bir belge güncellemesi yaklaşık dört
milisaniye sürüyordu. Bu, OxiDB'nin, varsayılan ayarlarıyla MongoDB'nin
arkasında kaldığı az sayıdaki işlemden biridir; çünkü MongoDB varsayılan
ayarında her yazmayı diske zorlamaz, bellekten onaylayıp günlüğü sonradan, toplu
olarak boşaltır. Yani bu karşılaştırma, elma ile armuttur: OxiDB her tekil yazmayı
gerçekten dayanıklı kılarken bir bedel öder; MongoDB o bedeli erteler, daha az
dayanıklılık karşılığında daha hızlı görünür.

## Grup commit: bedeli toplu yazmada eritmek

Peki OxiDB, her yazma için bir tam boşaltma ödüyorsa, çok sayıda belge eklerken
nasıl hızlı kalır? Yanıt, altıncı bölümde tanıttığımız grup commit fikrindedir.
Bir tam boşaltma, ister bir kaydı ister binlerce kaydı diske işlesin, kabaca aynı
süreyi alır. Bu yüzden OxiDB, toplu bir ekleme yaparken, her belgeyi ayrı ayrı
boşaltmaz; tüm partiyi günlüğe yazar ve **tek bir boşaltmayla** birden dayanıklı
kılar. Böylece tam boşaltmanın maliyeti, partideki belge sayısına bölünür ve
belge başına neredeyse hiçe iner.

Bunun somut sonucu çarpıcıdır. Ölçümlerde, beş binlik partiler halinde yapılan
toplu eklemede OxiDB, her partiyi tek boşaltmayla dayanıklı kıldığı için,
MongoDB ile başa baş ekleme hızına ulaşıyordu — hem de MongoDB'nin yapmadığı bir
şeyi, her partiyi gerçekten diske boşaltmayı, yaparak. Yani katı dayanıklılık,
toplu yazmalarda bir dezavantaj olmaktan çıkıyordu; çünkü grup commit, güvenceden
hiç ödün vermeden maliyeti eritiyordu. Tek tek yazmada görünen dört milisaniyelik
fark, toplu yazmada kayboluyordu; çünkü orada amorti edilecek bir parti vardı,
tek tek güncellemede ise yoktu.

## Gevşek senkronizasyon: hızı seçmek

OxiDB, katı dayanıklılığı varsayılan yapar, ama bunu zorunlu kılmaz. Altıncı
bölümdeki tayfın gevşek ucu da, isteğe bağlı bir ayarla — bir ortam değişkeniyle
— açılabilir. Gevşek kipte, bir yazma günlüğe yazılır ama hemen diske
boşaltılmaz; boşaltma, her commit'te değil, arka planda çalışan bir iş parçacığı
tarafından belirli aralıklarla, toplu olarak yapılır. Yazma, bellekten onaylanır
ve hemen geri döner.

Bu kipin hızı, ölçümlerde net biçimde görülür: gevşek kipte, tek bir belge
güncellemesi, katı kipteki yaklaşık dört milisaniyeden, yaklaşık seksen yedi
mikrosaniyeye iniyordu — yani kırk katından fazla hızlanıyor ve MongoDB'nin
varsayılan hızını bile geçiyordu. Çünkü artık her güncelleme bir tam boşaltma
beklemiyordu. Bedeli, altıncı bölümde söylediğimiz risktir: bir çökme olursa,
henüz boşaltılmamış son birkaç yazma kaybolabilir. Yani gevşek kip, hızı küçük
bir veri kaybı riski karşılığında satın alır ve bu, MongoDB'nin varsayılan
dayanıklılık modeline çok benzer bir tercihtir. OxiDB'nin yaptığı şey, bu tercihi
kullanıcının eline vermektir: güvenlik mi, hız mı — duruma göre seçersiniz.

## Kurtarma: çökmeden geri dönmek

OxiDB'nin dayanıklılık vaadi, asıl sınavını çökmeden sonra verir. Sistem yeniden
açıldığında, kendini tutarlı bir duruma getirmek için altıncı bölümdeki kurtarma
sürecini uygular; bu süreç, bir önceki bölümdeki iki depolama kipine göre küçük
farklarla işler.

Önce bir **taban** kurulur. Belleğe öncelikli kipte bu, `.btree` anlık
görüntüsünün belleğe yüklenmesidir. Disk-öncelikli kipte ise, `.bdat`
dosyasındaki yaşayan kayıtların taranıp kimlik-konum dizininin yeniden
kurulmasıdır. Bu taban hazır olduktan sonra, OxiDB **yazma-öncesi günlüğü taban
üzerine oynatır**: günlükteki, henüz tabana yansımamış değişiklikleri yeniden
uygular. Böylece, çökmeden hemen önce dayanıklı kılınmış her değişiklik geri
gelir.

Bu yeniden oynatmanın güvenli olması, altıncı bölümdeki "etkisiz
tekrarlanabilirlik" ilkesine dayanır ve OxiDB bunu somut biçimde sağlar:
değişiklikler belge kimliğiyle ilişkilendirildiği için, aynı eklemeyi iki kez
oynatmak zararsızdır — ikinci kez, aynı kimliğe yazılır ve sonucu değiştirmez. Bu
yüzden kurtarma, bir değişikliğin tabana zaten yansıyıp yansımadığından emin
olmasa bile, onu güvenle yeniden uygulayabilir.

Kurtarmanın yarım kalmış son kaydı nasıl ele aldığı, altıncı bölümdeki sağlama
toplamı mekanizmasının doğrudan uygulamasıdır. Çökme, en son günlük kaydının
yazılmasının ortasında olmuş olabilir; o kayıt yarım kalmış, bozulmuş olabilir.
OxiDB, her kaydı oynatırken sağlama toplamını yeniden hesaplar; uyuşmuyorsa, o
kaydın yarım kaldığını anlar, onu güvenle atar ve ondan önceki son sağlam
noktadan devam eder. Böylece yarım yazma, sessizce bozuk veriye dönüşmek yerine,
açıkça fark edilip temizlenir.

## Denetim noktası ve günlüğün dizginlenmesi

Altıncı bölümde, günlüğün sonsuza dek büyüyemeyeceğini ve periyodik olarak
denetim noktalarıyla dizginlenmesi gerektiğini söylemiştik. OxiDB de, günlükteki
değişiklikler asıl depoya güvenle yansıdıktan sonra, o noktaya kadarki günlük
kayıtlarını geri kazanır; böylece günlük baştan büyümeye devam edebilir.

Burada OxiDB'nin bir tasarım inceliği vardır ve onu görmek öğreticidir.
Performans için, asıl depoya yazma — yani denetim noktası — tembelce, arada bir
yapılır; bu arada, tamamlanmış bir değişikliğin tek dayanağı bir süre yalnızca
günlük olabilir. Bu, hız kazandıran bilinçli bir tercihtir; ama kurtarma
mantığının buna uygun tasarlanmasını gerektirir, çünkü kurtarma, asıl depoda
henüz görünmeyen ama günlükte dayanıklı olan değişiklikleri de geri getirmek
zorundadır. OxiDB'nin geliştirilmesinde, bu inceliğin gözden kaçtığı bir
durumun nasıl bir kurtarma hatasına yol açtığı ve nasıl düzeltildiği, dayanıklılık
mantığının ne kadar dikkat gerektirdiğinin iyi bir örneğidir.

## WAL her iki kibin de önünde durur

Altıncı bölümde, yazma-öncesi günlüğün depolama felsefesinin bir alternatifi
değil, onun önünde duran ayrı bir katman olduğunu vurgulamıştık. OxiDB'de bu,
açıkça görülür: aynı günlük ve aynı dayanıklılık makinesi, hem belleğe öncelikli
hem de disk-öncelikli kipin önünde durur. Hangi kipte olursanız olun, bir yazma
önce aynı günlüğe gider; yalnızca o yazmanın "asıl depoya" uygulanma biçimi —
bellekteki eşlemeye mi yoksa `.bdat` dosyasına mı — kipe göre değişir. Böylece
dayanıklılık, depolama kipinden bağımsız, ortak bir güvence olarak kalır.

## Bu bölümün bıraktığı yer

Bu bölümde, OxiDB'nin dayanıklılık mekanizmasını yakın plana aldık. Her
koleksiyonun, sağlama toplamlı ve işlem kimlikli kayıtlardan oluşan bir
yazma-öncesi günlük tuttuğunu; her yazmanın önce bu günlüğe gidip dayanıklı
kılındığını, sonra depoya uygulandığını gördük. Katı dayanıklılığın varsayılan
olduğunu ve bunun tek tek güncellemelerde bir hız bedeli taşıdığını, ama grup
commit sayesinde toplu yazmalarda bu bedelin eridiğini; gevşek kipin ise hızı bir
kayıp riski karşılığında nasıl artırdığını ölçümlerle gördük. Kurtarmanın taban
kurma, günlük oynatma, etkisiz tekrarlanabilirlik ve yarım-kayıt temizleme
adımlarını ve denetim noktasının günlüğü nasıl dizginlediğini inceledik.

Artık verimiz hem kalıcı hem de çökmeye dayanıklı biçimde duruyor. Ama yedinci
bölümde gördüğümüz gibi, veriyi güvenle saklamak yetmez; onu taramadan hızla
bulabilmek de gerekir. Bir sonraki bölümde, OxiDB'nin aramayı hızlandıran
yapılarına — alan indekslerine, bileşik indekslere ve disk-öncelikli kipte
belleğe yansıtılan indekslere — ve bunların yedinci bölümdeki ilkelerle nasıl
örtüştüğüne eğiliyoruz.
