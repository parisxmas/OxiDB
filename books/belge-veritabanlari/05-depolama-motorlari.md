# Depolama Motorları: Sayfa Tabanlı, Append-Only, LSM ve B-Ağaçları

Kısım I boyunca hep mantıksal düzeyde kaldık: veriyi nasıl düşündüğümüzle,
belgenin ne olduğuyla ilgilendik. Şimdi perdeyi aralıyor ve fiziksel düzeye
iniyoruz. Bir belge veritabanı, tüm o belgeleri diske tam olarak nasıl yazar ve
geri okur? Bu sorunun yanıtını veren bileşene **depolama motoru** denir ve o,
bir veritabanının kalbidir. Bu bölümde, depolama motorlarının çözmek zorunda
olduğu temel gerilimi ve bu gerilime tarih boyunca verilen başlıca yanıtları —
sayfa tabanlı B-ağaçlarını ve append-only/log-yapılı yaklaşımları —
inceleyeceğiz. Buradan sonraki birkaç bölüm daha teknik olacak, ama yöntemimiz
aynı: her tasarımı, çözmeye çalıştığı somut sorundan başlayarak anlamak.

![Şekil 5 — Yerinde güncelleme ile ekle-only depolamanın temel ödünleşimi.](sekiller/05-depolama-motorlari.svg){width=80%}

## Depolama motoru ne yapar

Bir depolama motorunu, veritabanının geri kalanından soyutlayarak düşünmek
yararlıdır. Üstündeki katmanlar — sorgu işleyici, indeksler, işlem yöneticisi —
ona iki temel istekle gelir: "şu kimliğe sahip belgeyi sakla" ve "şu kimliğe
sahip belgeyi geri ver." Depolama motorunun işi, bu basit isteklerin altındaki
zor sorunu çözmektir: baytları diskte nereye, hangi düzende yerleştireceğini ve
onları nasıl geri bulacağını kararlaştırmak. Bu kararlar, veritabanının ne kadar
hızlı yazıp okuyacağını, ne kadar yer kaplayacağını ve çökmeye karşı ne kadar
dayanıklı olacağını doğrudan belirler. Bu yüzden depolama motoru tasarımı,
veritabanı mühendisliğinin en belirleyici tercihidir.

Önemli bir noktayı baştan ayıralım: depolama motoru, belgenin **içeriğiyle**
ilgilenmez. Onun için bir belge, bir kimliğe bağlı bir bayt yığınıdır. Belgenin
alanlarını yorumlamak, üzerinde sorgu çalıştırmak üst katmanların işidir; depolama
motoru yalnızca o bayt yığınını güvenilir biçimde saklayıp geri vermekle
yükümlüdür. Bu ayrım, depolama motorlarının neden farklı veri modellerinin
altında — ilişkisel de olsa belge de olsa — aynı ilkelerle çalışabildiğini
açıklar.

## Her şeyi belirleyen kısıt: disk belleğe benzemez

Birinci bölümde, diskin belleğe hiç benzemediğine değinmiştik. Depolama
motorlarını anlamak için, bu farkın somut sonuçlarını derinleştirmemiz gerekir;
çünkü bu bölümdeki her tasarım kararı, doğrudan diskin doğasından doğar.

Diskin üç temel huyu vardır. Birincisi, **yavaştır**: belleğe erişim
nanosaniyeler alırken, diske erişim kat be kat daha uzun sürer. Bu yüzden
depolama motorunun en büyük amacı, gereksiz disk erişimlerinden kaçınmaktır.
İkincisi, disk **ardışık erişimi**, dağınık erişimden çok daha iyi yapar.
Diskten art arda gelen bir bölgeyi okumak ya da dosyanın sonuna eklemeye devam
etmek hızlıdır; oysa diskin orasından burasından, rastgele konumlara erişmek çok
daha yavaştır. Bu fark, geleneksel dönen disklerde uçurum kadar büyüktü; katı hal
sürücülerinde daralsa da hâlâ önemlidir. Üçüncüsü, disk veriyi **bloklar halinde**
okuyup yazar: tek bir baytı bile değiştirmek isteseniz, sistem o baytı içeren koca
bir bloğu okuyup, değiştirip, geri yazar. Yani küçük, dağınık değişiklikler
diskte orantısız bir maliyet taşır.

Bu üç huy, depolama motoru tasarımına tek bir buyruk dayatır: **diske ardışık,
büyük ve seyrek yaz; rastgele, küçük ve sık erişimden kaçın.** Bu bölümdeki tüm
yaklaşımlar, bu buyruğa farklı biçimlerde uymaya çalışan stratejilerdir.

## Temel gerilim: okumaya mı, yazmaya mı eniyilemek

Depolama motoru tasarımının kalbinde tek bir gerilim yatar ve onu en baştan
görmek, geri kalan her şeyi aydınlatır: bir veri yerleşimi, hem okumayı hem
yazmayı aynı anda en iyi yapamaz. Bu ikisi arasında bir ödünleşim vardır.

Neden? Çünkü okumayı hızlandırmak için veriyi **düzenli** tutmak istersiniz —
örneğin sıralı, dengeli bir yapıda — ki aradığınızı çabucak bulasınız. Ama
veriyi düzenli tutmak, her yeni yazmada o düzeni korumak için diskin ortasına,
"doğru yere" müdahale etmeyi gerektirir; bu da rastgele yazma demektir, yani
diskin en sevmediği şey. Tersine, yazmayı hızlandırmak için veriyi olduğu gibi,
geldiği sırada, dosyanın sonuna ardışık olarak eklersiniz; bu yazma için
idealdir, ama o zaman veri düzensiz birikir ve aradığınızı bulmak zorlaşır.
Düzen, okumanın dostu ama yazmanın düşmanıdır; düzensizlik ise tersi. İşte iki
büyük depolama felsefesi — sayfa tabanlı B-ağaçları ve log-yapılı yaklaşımlar —
bu gerilimin iki ucundan doğar.

## Sayfa tabanlı B-ağaçları: yerinde güncelleme

Birinci felsefe, veriyi **düzenli tutmayı** önceler ve onlarca yıl boyunca
veritabanı dünyasına egemen oldu. Bu yaklaşımda disk, sabit boyutlu **sayfalara**
bölünür — her sayfa, belirli sayıda kaydı tutan, diskten tek seferde okunup
yazılan bir blok. Veri, bu sayfalar üzerinde **B-ağacı** denen bir yapıyla
düzenlenir.

B-ağacını, devasa, çok katmanlı bir dosyalama dolabı gibi düşünebilirsiniz. En
üstte, "A–M arası şu çekmecede, N–Z arası bu çekmecede" diyen bir yönlendirme
katmanı vardır. Onun altında daha ince ayrımlar, en altta ise verinin kendisini
tutan yapraklar bulunur. Bir kaydı aramak için tepeden başlar, her adımda doğru
dala saparak yapraklara inersiniz; birkaç adımda, milyonlarca kayıt arasında bile,
aradığınıza ulaşırsınız. Üstelik veri sıralı tutulduğu için, "şu değerle bu değer
arasındaki tüm kayıtlar" gibi **aralık sorgularını** da verimli yanıtlarsınız —
yapraklar arasında yan yana ilerlemek yeterlidir. B-ağacı, hem tekil hem aralık
okumalarında olağanüstü iyidir; bu yüzden okuma-yoğun, sorgu-zengin sistemlerin
gözdesi olmuştur.

Bedeli ise yazmada ortaya çıkar. Bir kaydı değiştirdiğinizde, B-ağacı onu diskte
ait olduğu sayfada, **yerinde** günceller. Bu, az önce diskin en sevmediği şey
dediğimiz rastgele yazma demektir: değiştirilecek sayfa diskin neresindeyse,
oraya gidilip o sayfa okunur, değiştirilir, geri yazılır. Dahası, ağaç dengesini
korumak için sayfaların ara sıra bölünmesi ya da birleşmesi gerekir; bu da ek
yazmalar doğurur. Tek bir küçük değişikliğin diske birden çok blok yazılmasına yol
açmasına **yazma büyütmesi** (write amplification) denir ve B-ağaçlarının doğal
bir maliyetidir. Bir de eşzamanlılık zorluğu vardır: birçok işlem aynı sayfalara
aynı anda dokunabileceği için, sayfaların dikkatlice kilitlenmesi gerekir;
bunun inceliklerine on birinci bölümde döneceğiz.

Özetle B-ağacı yaklaşımı, okumayı en üst düzeye çıkarmak için veriyi düzenli ve
yerinde tutar; bunun karşılığında rastgele yazma ve yazma büyütmesi maliyetini
öder.

## Append-only ve log-yapılı yaklaşımlar: asla üzerine yazma

İkinci felsefe, gerilimin öteki ucundan başlar ve **yazmayı** önceler. Temel
fikri tek bir cümleyle özetlenebilir: **var olan veriyi asla yerinde değiştirme;
her yazmayı dosyanın sonuna ekle.** Bunu, sürekli yeni satırlar eklediğiniz, ama
hiçbir eski satırı silmediğiniz bir muhasebe defterine benzetebilirsiniz. Bir
kaydı güncellemek istediğinizde, eski kaydı bulup değiştirmezsiniz; onun yeni
hâlini defterin sonuna yazarsınız. En son yazılan, geçerli olandır.

Bu yaklaşımın yazmadaki üstünlüğü açıktır: her yazma, dosyanın sonuna **ardışık**
bir ekleme olduğu için, diskin en sevdiği erişim biçimidir. Rastgele yazma yoktur;
yazma büyütmesi en aza iner. Bu yüzden log-yapılı (append-only) motorlar, yazma-
yoğun iş yüklerinde B-ağaçlarını rahatça geçer.

Ama bedava öğle yemeği yoktur; bu yaklaşım iki yeni sorun doğurur. Birincisi
**okuma sorunudur**. Bir kaydın en güncel hâli defterin neresinde diye, dosyayı
baştan sona taramak kabul edilemez. Bu yüzden append-only motorlar, ayrı bir
**dizin** tutar: her kaydın kimliğinden, o kaydın en güncel hâlinin dosyada
durduğu konuma bir eşleme. Yazarken bu dizini güncellersiniz; okurken önce dizine
bakıp konumu öğrenir, sonra doğrudan o konuma gidersiniz. Yani append-only motor,
veriyi düzensiz yazmanın getirdiği okuma zorluğunu, ayrı bir dizinle telafi eder.
(Bu dizinin kendisi de bellekte ya da diskte tutulabilir; üçüncü kısımda OxiDB'nin
tam olarak böyle bir kimlik→konum dizini kullandığını göreceğiz.)

İkincisi **ölü alan sorunudur**. Madem eski kayıtların üzerine yazmıyoruz, onlar
dosyada öylece durmaya devam eder. Bir kaydı yüz kez güncellerseniz, dosyada o
kaydın yüz eski kopyası birikir; yalnızca sonuncusu geçerlidir, gerisi **ölü
alandır**. Zamanla dosya, çoğu artık geçersiz kayıtlarla şişer. Bu şişkinliği
gidermek için, arada bir, dosyayı baştan yazıp yalnızca yaşayan (geçerli)
kayıtları yeni bir dosyaya kopyalamak gerekir; ölü kayıtlar geride bırakılır. Bu
temizlik işlemine **sıkıştırma** (compaction) denir ve append-only motorların
ayrılmaz bir parçasıdır. Sıkıştırmayı, defter dolup taştığında oturup yalnızca
hâlâ geçerli satırları temiz bir deftere geçirmeye benzetebilirsiniz. (Üçüncü
kısımda OxiDB'nin sıkıştırmayı ne zaman ve nasıl yaptığına ayrı bir bölüm
ayıracağız.)

## LSM ağaçları: log-yapılının olgun hâli

Append-only fikrini bir adım ileri taşıyan ve birçok modern belge ve geniş-sütun
veritabanının kalbinde yatan tasarıma **LSM ağacı** denir — açılımı "log-yapılı
birleştirmeli ağaç". Saf append-only yaklaşımının okuma zorluğunu, akıllıca bir
düzenlemeyle hafifletir.

LSM'nin fikri şudur: gelen yazmaları önce **bellekte**, sıralı bir yapıda
biriktir. Bellek hızlı olduğu için bu yazmalar anında kabul edilir. Bellekteki
bu tampon dolduğunda, içerik tek seferde, **sıralı** biçimde diske bir
"parça" olarak yazılır — yine ardışık yazma, yine diskin sevdiği biçim. Zamanla
diskte böyle birçok sıralı parça birikir. Arka planda çalışan bir süreç, bu
parçaları periyodik olarak **birleştirir** (merge): birkaç sıralı parçayı tek bir
büyük sıralı parçaya kaynaştırır, bu sırada ölü kayıtları da ayıklar. Bu
birleştirme, append-only modelin sıkıştırmasının daha düzenli, kademeli bir
biçimidir.

LSM, yazmada olağanüstü hızlıdır, çünkü her yazma önce belleğe gider ve diske hep
ardışık yazılır. Okumada ise bir bedel vardır: bir kaydı ararken, en güncel
hâli hangi parçada diye birden çok parçaya bakmak gerekebilir. Bu maliyeti
azaltmak için LSM motorları, "bu kayıt bu parçada **kesinlikle yok**" sorusunu
diske hiç bakmadan, çok ucuza yanıtlayan olasılıksal yardımcı yapılar kullanır;
böylece çoğu boşa aramadan kurtulur. Yine de LSM, doğası gereği, yazmayı
okumanın ve arka plan birleştirme yükünün önüne koyan bir tasarımdır.

## Üç büyütme: kaçınılmaz üçgen

Bu noktada, depolama motorlarını karşılaştırmak için kullanışlı bir çerçeve
ortaya çıkar. Her tasarım, üç tür "büyütme" arasında bir denge kurar ve
hiçbiri üçünü birden en aza indiremez; biri azalırken genellikle bir diğeri
artar.

**Yazma büyütmesi**, mantıksal olarak yazdığınız bir baytın diske kaç bayt
olarak yazıldığıdır. B-ağaçlarında yerinde güncelleme ve sayfa bölünmeleri bunu
artırır; LSM'de ise asıl yük, arka plandaki birleştirmenin aynı veriyi defalarca
yeniden yazmasından gelir. **Okuma büyütmesi**, mantıksal bir okuma için diskten
kaç parçaya bakıldığıdır; B-ağacı burada genellikle iyidir (birkaç sayfa), LSM
ise birden çok parçaya bakmak zorunda kalabildiği için daha kötüdür. **Yer
büyütmesi**, verinin diskte mantıksal boyutunun kaç katı yer kapladığıdır;
append-only ve LSM'de biriken ölü kayıtlar bunu artırır, sıkıştırma azaltır.

Bu üçgen, neden tek bir "en iyi" depolama motoru olmadığını net biçimde gösterir.
B-ağaçları okuma büyütmesini en aza indirir, yazma ve belirli durumlarda yer
büyütmesini öder; LSM ve append-only yaklaşımlar yazmayı en aza indirir, okuma ve
yer büyütmesini öder. Hangisinin doğru olduğu, üçüncü bölümdeki o değişmez ilkeye
geri döner: iş yükünüze, yani okuma mı yoksa yazma mı baskın olduğuna bağlıdır.

## Belleğin rolü ve veriyi belleğe yansıtmak

Şimdiye dek hep diskten söz ettik, ama hiçbir gerçek depolama motoru yalnızca
diske güvenmez; bellekle disk arasında sürekli bir dans vardır. Sık erişilen
veriyi bellekte tutmak, onu her seferinde diskten okumaktan kat kat hızlıdır. Bu
yüzden depolama motorları, diskin en çok kullanılan parçalarını bellekte tutan
bir **önbellek** işletir; sıcak veri bellekte kalır, soğuk veri diskte. Bu
önbelleğin ne kadar büyük olacağı, hangi verinin tutulup hangisinin atılacağı,
başlı başına bir tasarım konusudur ve on üçüncü bölümü tümüyle ona ayıracağız.

Belleği işin içine katmanın zarif bir yolu daha vardır ve onu burada tohumlamak
yararlı olur: bir dosyayı, işletim sistemine söyleyerek, doğrudan belleğe
**yansıtmak**. Bu teknikte dosya, sanki belleğin bir parçasıymış gibi davranır;
ona erişmek, diskten açıkça okuma çağrısı yapmak yerine, yalnızca bellekteki bir
adrese bakmak kadar basit hale gelir. Verinin gerçekten diskten belleğe ne zaman
getirileceğine işletim sistemi karar verir ve bellek darda kaldığında o veriyi
sessizce geri atabilir. Bu yaklaşım, depolama motorunun kendi önbelleğini elle
yönetme yükünü büyük ölçüde işletim sistemine devretmesini sağlar. Üçüncü kısımda
OxiDB'nin "disk-öncelikli" kipinde tam olarak bu tekniği kullandığını ve bunun
bellek ayak izini nasıl küçülttüğünü ayrıntısıyla göreceğiz.

## Bu bölümün bıraktığı yer ve bir sonrakine köprü

Bu bölümde, bir belge veritabanının baytları diske nasıl yerleştirdiğini —
depolama motorunun işini — ve bu işin kalbindeki okuma-yazma gerilimini gördük.
İki büyük felsefeyi tanıdık: veriyi düzenli ve yerinde tutarak okumayı önceleyen
sayfa tabanlı B-ağaçları; ve veriyi asla üzerine yazmadan ardışık ekleyerek
yazmayı önceleyen append-only ve LSM yaklaşımları. Bu yaklaşımların hiçbirinin
mutlak üstün olmadığını, her birinin üç büyütme arasında farklı bir denge
kurduğunu gördük.

Ama hangi felsefeyi seçerseniz seçin, henüz konuşmadığımız, sinsi bir sorun
geride duruyor. Bir veriyi diske "yazdığınızda", o veri gerçekten kalıcı olmuş
mudur? Sezgi "evet" der, ama gerçek çok daha incelikli. Yazma işlemleri yolda,
çeşitli tamponlarda bekliyor olabilir; ve sistem tam o sırada çökerse, "yazdım"
sandığınız veri buharlaşabilir — ya da daha kötüsü, yarısı yazılmış, bozuk bir
hâlde kalabilir. Bir sonraki bölümde, depolama motorlarının en çetin
sorumluluğuna, yani bir yazmanın gerçekten dayanıklı olduğundan emin olmaya ve
çökmenin ortasından tutarlı biçimde geri dönmeye — yazma-öncesi günlüğe ve fsync'in
inceliklerine — eğileceğiz.
