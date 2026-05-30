# Bellek, Önbellek ve Disk Ödünleşimi

Önceki iki bölümde, bir veritabanını birçok makineye yaymanın koordinasyon
sorunlarıyla uğraştık. Şimdi bakışımızı yeniden tek bir makineye, tek bir düğümün
içine çeviriyoruz; çünkü her düğüm, kendi içinde de çözmesi gereken bir kaynak
yönetimi sorunu taşır. Beşinci bölümde tohumlamıştık: bellek hızlı ama küçük ve
uçucu, disk yavaş ama büyük ve kalıcıdır. Bir düğümün performansının büyük
bölümü, tek bir karara dayanır: hangi verinin bellekte tutulacağı, hangisinin
diske bırakılacağı. Bu bölüm, o sessiz ama belirleyici dengeyi — bellek, önbellek
ve disk arasındaki ödünleşimi — ele alıyor.

## Temel asimetri ve oyunun özü

Önce, kitap boyunca birkaç kez değindiğimiz asimetriyi netleştirelim, çünkü bu
bölümdeki her şey ondan doğar. Bellek çok hızlıdır ama pahalıdır, sınırlıdır ve
elektrik gidince içeriğini kaybeder. Disk çok yavaştır ama ucuzdur, büyüktür ve
kalıcıdır. Bu ikisini, bir masa ile bir depoya benzetebilirsiniz: masanız (bellek)
küçüktür ama üzerindeki her şeye anında uzanırsınız; depo (disk) kocamandır ama
oradan bir şey getirmek zaman alır.

Oyunun özü şudur: çalışmak için ihtiyaç duyduğunuz şeyleri masanın üstünde
tutmak, gerisini depoda bırakmak. Bir veritabanının tek bir düğümdeki hızı, asıl
olarak bu kararı ne kadar iyi verdiğine bağlıdır. Doğru veri bellekteyse, sistem
uçar; yanlış veri bellekteyse ve aradığınız sürekli diskten getirilmek
zorundaysa, sistem sürünür. Bütün mesele, sınırlı belleği en çok işe yarayacak
veriyle doldurmaktır.

## Çalışma kümesi ve uçurum etkisi

Bu kararı anlamanın anahtarı, **çalışma kümesi** (working set) kavramıdır.
Herhangi bir anda, verinin yalnızca bir kısmı "sıcaktır" — yani etkin biçimde
kullanılmaktadır. Bir e-ticaret sitesinde, o anki kampanyadaki ürünler, son
siparişler, aktif kullanıcılar sıcaktır; yıllar önceki kayıtlar ise soğuk,
nadiren dokunulan veridir. Çalışma kümesi, işte bu sıcak kısımdır.

Buradan çarpıcı bir gerçek doğar: performans, kademeli bir eğim değil, bir
**uçurumdur**. Çalışma kümeniz belleğe sığdığı sürece, sistem hızlıdır — ihtiyaç
duyduğunuz her şey zaten masanın üstündedir. Ama çalışma kümesi belleği biraz
olsun aşmaya başladığında, sistem aniden çöker; çünkü artık sürekli olarak depoya
koşmak, bir şeyi getirip masaya koymak için başka bir şeyi geri kaldırmak,
sonra onu da geri istemek zorunda kalırsınız. Bu sürekli oraya buraya taşıma
durumuna **debelenme** (thrashing) denir ve performansı dramatik biçimde düşürür.
Bu yüzden bir veritabanını boyutlandırırken sorulacak en kritik soru, "tüm verim
belleğe sığar mı" değil, "**çalışma kümem** belleğe sığar mı" sorusudur.

## Önbellek: belleği akıllıca kullanmak

Bir düğüm, sıcak veriyi bellekte tutmak için bir **önbellek** (cache) işletir.
Mantığı basittir. Bir veri istendiğinde, sistem önce önbelleğe bakar. Veri orada
varsa — buna "isabet" (hit) denir — hızlıca, diske hiç gitmeden döndürülür. Yoksa
— buna "ıska" (miss) denir — diskten getirilir, kullanıcıya verilir ve gelecekte
yine istenebileceği umuduyla önbelleğe yerleştirilir. Önbellek, böylece uygulama
ile yavaş disk arasında bir tampon görevi görür; sık istenen veriyi belleğe
çekerek disk erişimini en aza indirir.

Önbellek tek bir katmanda olmak zorunda değildir. Sistem, ham disk bloklarını
önbellekleyebilir — böylece aynı bloğu tekrar tekrar diskten okumaktan kurtulur.
Ya da çözülmüş, kullanıma hazır belgeleri önbellekleyebilir — böylece aynı belgeyi
her seferinde yeniden ayrıştırmaktan kurtulur. Ayrıca indeks yapıları da bellekte
tutulur, çünkü onlar neredeyse her sorguda kullanılır. Farklı katmanları
önbelleklemek, farklı türden tekrar eden maliyetleri ortadan kaldırır. Üçüncü
kısımda OxiDB'nin hem çözülmüş belgeler hem de ham baytlar için ayrı önbellekler
tuttuğunu göreceğiz.

## Tahliye: yer açmak için ne atılır

Önbellek sonludur; eninde sonunda dolar. Dolduğunda, yeni bir şeyi içeri almak
için eskilerden birini **atmak** (tahliye etmek) gerekir. Hangi verinin
atılacağına karar veren kurala **tahliye politikası** denir ve bu, önbelleğin ne
kadar işe yarayacağını doğrudan belirler.

En yaygın politika, "en uzun süredir dokunulmayanı at" ilkesidir — kısaca LRU.
Mantığı bir bahse dayanır: yakın zamanda kullandığınız bir şeyi, yine yakında
kullanma olasılığınız yüksektir; çok uzun süredir dokunmadığınız bir şeyi ise
muhtemelen bir süre daha kullanmayacaksınız. Bu yüzden LRU, en uzun süredir
boşta duranı kurban seçer. Bir kütüphanecinin, sık istenen kitapları el
arabasında yakınında tutup, aylardır kimsenin sormadıklarını rafa kaldırmasına
benzer.

LRU çoğu zaman iyi çalışır, ama bir zayıflığı vardır. Büyük bir tarama düşünün —
örneğin tüm koleksiyonu bir kez baştan sona okuyan bir toplama sorgusu. Bu tarama,
gerçekte sıcak olmayan bir sürü veriyi önbelleğe doldurur ve bu sırada asıl sıcak
veriyi dışarı atar. Tarama bittiğinde önbellek, bir daha kullanılmayacak soğuk
veriyle dolu, asıl ihtiyaç duyulan sıcak veriden ise yoksun kalmıştır. Buna
**önbellek kirlenmesi** denir. Bu yüzden olgun sistemler, büyük taramaların
önbelleği kirletmesini önleyen, "tarama-dirençli" politikalar kullanır. Bunun
çeşitli incelikleri vardır; ama temel ders şudur: iyi bir tahliye politikası,
yalnızca "ne zaman kullanıldı" değil, "gerçekten sıcak mı" sorusunu da gözetmeye
çalışır.

## Önbelleğin kendi maliyeti ve sınırlama zorunluluğu

Önbellek bedava bir kazanç değildir; kendi maliyetleri vardır. Önbelleğe ayrılan
bellek, başka hiçbir işe ayrılamayan bellektir. Önbelleği yönetmenin — neyin ne
zaman kullanıldığını takip etmenin — kendi küçük yükü vardır. Ve en tehlikelisi:
eğer bir önbellek sınırsız büyümeye bırakılırsa, tüm belleği yiyip bitirebilir ve
sistemi belleksiz bırakıp çökertebilir.

Bu yüzden ciddi sistemler, önbelleklerini bir **bellek bütçesiyle sınırlar**:
önbellek, belirli bir boyutu aşamaz; o boyuta ulaştığında, yeni bir şey almak
için mutlaka eskisini atar. Böylece bellek kullanımı öngörülebilir kalır ve
denetimden çıkmaz. Üçüncü kısımda OxiDB'nin, bellek kullanımının kontrolden
çıkmasını önlemek için önbelleklerini tam da böyle sabit bir bütçeyle
sınırladığını ve bunun bellek ayak izini nasıl dizginlediğini göreceğiz.

## İki felsefe: belleğe öncelikli ve diske öncelikli

Beşinci bölümde, depolama motorlarının iki felsefesini görmüştük; bellek-disk
ilişkisinde de benzer bir ikilik vardır ve doğrudan o bölümle bağlanır.

Birinci felsefe **belleğe önceliklidir**: tüm veriyi bellekte tutmak, diski
yalnızca kalıcılık için — yani anlık görüntüler ve günlük için — kullanmak. Bu
yaklaşımda her okuma, zaten bellekte olan veriye dokunduğu için olağanüstü
hızlıdır. Bedeli, belleğin **veriyle birlikte büyümesidir**: veri ikiye katlanınca,
gereken bellek de ikiye katlanır. Bu, küçük ve orta ölçekli veri için harikadır,
ama veri büyüdükçe bellek hem pahalı hem de bir tavana dayanan bir kısıt haline
gelir. Üçüncü kısımda OxiDB'nin varsayılan kipinin tam da bu belleğe öncelikli
yaklaşım olduğunu göreceğiz.

İkinci felsefe **diske önceliklidir**: bellekte yalnızca küçük bir şey — örneğin
bir kimlik-konum dizini ve sıcak bir alt küme — tutmak, verinin asıl gövdesini
diskte bırakmak ve gerektiğinde oradan getirmek. Bu yaklaşımda bellek kullanımı,
veriyle birlikte büyümez; öngörülebilir ve düşük kalır. Bedeli, soğuk veriye
erişimin diske gitmeyi, yani bir gecikmeyi göze almasıdır. Bu, belleğin değerli
ya da verinin çok büyük olduğu durumlar için biçilmiş kaftandır. Üçüncü kısımda
OxiDB'nin "disk-öncelikli" kipinin tam olarak bunu yaptığını — taze açılışta tüm
veri yerine yalnızca küçük bir dizinin bellekte durduğunu — ve bunun bellek ayak
izini nasıl dramatik biçimde küçülttüğünü ayrıntısıyla göreceğiz.

## İşletim sistemine güvenmek: belleğe yansıtma

Bellek yönetimini elle yapmanın zarif bir alternatifi vardır ve beşinci bölümde
ona değinmiştik: bir dosyayı **belleğe yansıtmak** (mmap). Bu teknikte, dosyaya
erişmek bellekteki bir adrese erişmek kadar basit hale gelir ve verinin diskten
belleğe ne zaman getirileceğine, bellek darda kaldığında neyin geri atılacağına
işletim sistemi karar verir.

Bunun güzelliği, önbellek yönetiminin büyük bölümünü işletim sistemine
**devretmesidir**. İşletim sistemi zaten dosya sayfalarını bellekte tutan,
sıcakları saklayıp soğukları atan olgun bir mekanizmaya sahiptir; mmap, bu
mekanizmadan doğrudan yararlanır. Sıcak dosya sayfaları bellekte kalır, soğuklar
otomatik olarak atılır ve önemlisi, bunlar disk üzerinde bir kopyaya sahip
oldukları için bellek baskı altındayken **geri alınabilir** — yani gerektiğinde
serbest bırakılabilir.

Ama bu kolaylığın bir inceliği vardır ve onu görmek önemlidir. Belleğe yansıtılan
sayfalar, dokunulduklarında bellekte yer kaplar ve sistemin "bu süreç ne kadar
bellek kullanıyor" ölçümüne dahil olur. Bu yüzden, diske öncelikli, mmap'e dayanan
bir sistemin bellek kullanımı yanıltıcı görünebilir: taze açılışta çok düşükken,
tüm veriye dokunan büyük bir taramadan sonra yükselebilir — çünkü işletim sistemi
o sayfaları belleğe çekmiştir. Ama bu yükselen bellek, geri alınabilir bir
bellektir; baskı altında işletim sistemi onu sessizce serbest bırakır. "Veritabanım
ne kadar bellek kullanıyor" sorusunun, sandığınızdan çok daha incelikli olmasının
bir nedeni budur: kalıcı (anonim) bellek ile geri alınabilir (dosya destekli)
belleği ayırmak gerekir. Üçüncü kısımda OxiDB'nin disk-öncelikli kipinde tam da
bu olguyu — taze açılışta çok düşük, tam iş yükünden sonra yükselen ama geri
alınabilir bellek — ölçümlerle göreceğiz.

## Sıkıştırma: yer, işlemci ve sıfır-kopya üçgeni

Belleği ve diski yönetmenin bir aracı daha vardır: **sıkıştırma**. Veriyi
sıkıştırarak saklamak, hem diskte hem bellekte daha az yer kaplamasını sağlar;
böylece aynı belleğe daha çok veri sığar, çalışma kümesinin sığma olasılığı artar.
Ama sıkıştırma bedava değildir: sıkıştırılmış veriyi okumak için, her erişimde onu
**açmak** gerekir ve bu, işlemci zamanı harcar.

Burada üç yönlü bir ödünleşim belirir: yer, işlemci ve bir üçüncü incelik. Veri
sıkıştırılmamışsa ve şifrelenmemişse, onu okumak için hiçbir dönüşüm gerekmez;
bellekteki ham baytlara doğrudan, hiç kopyalamadan dokunulabilir — buna
**sıfır-kopya** erişim denir ve son derece hızlıdır. Veri sıkıştırılmışsa, bu
sıfır-kopya olanağı kaybolur; her okuma bir açma ve bir kopyalama gerektirir.
Dolayısıyla sıkıştırma, yer kazandırırken okuma hızından ödün verebilir. Hangisinin
kazandığı, veriye bağlıdır: gerçekten sıkışan, çok okunmayan veri için sıkıştırma
kazandırır; az sıkışan ama çok ve hızlı okunan veri için sıkıştırmamak daha
iyidir. Üçüncü kısımda OxiDB'nin disk-öncelikli kipinde, sıkıştırılmış ve
sıkıştırılmamış depolama arasındaki bu tam ödünleşimi ölçtüğünü ve belirli
yüklerde sıkıştırmayı kapatmanın taramaları nasıl hızlandırdığını göreceğiz.

## Bu bölümün bıraktığı yer

Bu bölümde, tek bir düğümün performansını belirleyen sessiz dengeyi — bellek,
önbellek ve disk arasındaki ödünleşimi — inceledik. Bellek ile diskin temel
asimetrisini; çalışma kümesi kavramını ve onun belleğe sığıp sığmamasının yarattığı
uçurum etkisini; önbelleğin nasıl çalıştığını ve tahliye politikalarının (özellikle
LRU'nun ve önbellek kirlenmesinin) inceliklerini; önbelleğin kendi maliyetini ve
sınırlama zorunluluğunu; belleğe öncelikli ve diske öncelikli iki felsefeyi;
belleğe yansıtmanın işletim sistemine devrettiği kolaylığı ve bellek ölçümünün
inceliğini; ve sıkıştırmanın yer-işlemci-sıfırkopya üçgenini gördük.

Böylece Kısım II'nin neredeyse tamamını — bir belge veritabanının içeride nasıl
çalıştığını — tamamlamış olduk: veriyi sakladık, dayanıklı kıldık, indeksledik,
sorguladık, özetledik, tutarlı tuttuk, ölçeklendirdik ve belleğini yönettik.
Geriye, OxiDB'ye geçmeden önce ele alınması gereken son bir genel konu kaldı ve
o, belki de en çok ihmal edilenidir: tüm bu veriyi **korumak**. Kim okuyabilir,
kim yazabilir; veri yetkisiz ellere geçerse ne olur; kimin ne yaptığı nasıl
kaydedilir? Bir sonraki bölümde, bir veritabanının güvenlik boyutuna — kimlik
doğrulamaya, yetkilendirmeye, şifrelemeye ve denetime — eğilerek Kısım II'yi
kapatıyoruz.
