# Belge Modeli Derinlemesine: JSON, Şema Esnekliği, Gömme ve Referans

Önceki üç bölümde belge modelini dışarıdan, diğer modellerle karşılaştırarak
tanıdık. Artık onun içine giriyoruz. Bu bölümde bir belgenin tam olarak neyden
yapıldığını, JSON gibi gösterimlerin neden bu kadar yaygınlaştığını, "şemasızlık"
denen şeyin gerçekte ne anlama geldiğini ve belge modeliyle çalışmanın
kalbindeki o tekrar eden kararı — gömmek mi, atıfta bulunmak mı —
inceleyeceğiz. Bu bölüm, Kısım I'in son durağı; onu bitirdiğimizde belgenin *ne*
olduğunu tam olarak bilecek ve Kısım II'de bir veritabanının bu belgeleri içeride
*nasıl* sakladığına geçmeye hazır olacağız.

## Bir belge neyden yapılır

Bir belge, özünde, **adlandırılmış alanların** bir koleksiyonudur. Her alanın
bir adı (örneğin `ad`, `yas`, `adres`) ve bir değeri vardır. Değer birkaç temel
türden biri olabilir. En basitleri **skaler** değerlerdir: bir metin, bir sayı,
bir doğru/yanlış (mantıksal) değeri ya da "değer yok" anlamına gelen boş (null)
değer. Ama belge modelini güçlü kılan, değerin yalnızca skaler olmak zorunda
olmamasıdır.

Bir değer, bir **liste** (dizi) olabilir: sıralı bir öğeler topluluğu, örneğin
bir yazının etiketleri ya da bir siparişin kalemleri. Daha da önemlisi, bir değer
**başka bir belge** olabilir: alanları olan, iç içe geçmiş bir nesne. Bir
kullanıcının `adres` alanı, içinde `sokak`, `sehir`, `posta_kodu` alanları olan
bir alt-belge olabilir. Ve bu iç içe geçme istediğiniz kadar derinleşebilir:
listelerin içinde belgeler, belgelerin içinde listeler. İşte belge modelinin
ağaç biçimli, hiyerarşik doğası buradan gelir; ikinci bölümde hiyerarşik modelin
iç içe veriyi tutmaktaki gücünden söz etmiştik — belge modeli o gücü, ama tek bir
katı hiyerarşiye mahkûm olmadan, devralır.

Bu yapı sayesinde, uygulamanızın zihnindeki bir "şey" — bir sipariş, bir profil,
bir ürün — tek bir belgede, kendi doğal biçimiyle durabilir. Belgenin şekli,
nesnenin şekline neredeyse birebir uyar. Üçüncü bölümde sözünü ettiğimiz nesne
uyumsuzluğunun erimesi, tam da bu yapısal benzerlikten kaynaklanır.

## Kendini tanımlayan veri

Belge modeliyle ilişkisel model arasındaki en derin yapısal fark, **şemanın
nerede durduğudur**. İlişkisel bir tabloda alan adları ve türleri yalnızca bir
kez, tablo tanımında yazar; satırların kendisi yalnızca değerleri taşır, hangi
değerin hangi sütuna ait olduğu tablonun yapısından bilinir. Belge modelinde
ise her belge, alan adlarını **kendi içinde** taşır. Yani bir belgeye baktığınızda,
ayrı bir tanıma bakmaya gerek kalmadan, hangi değerin neyi ifade ettiğini
görürsünüz. Buna **kendini tanımlayan** veri denir.

Bu özelliğin iki yüzü vardır. Armağan tarafı: veri taşınabilir ve esnektir. Bir
belgeyi tek başına anlamlandırabilirsiniz; her belge kendi alanlarını taşıdığı
için farklı belgeler farklı alanlara sahip olabilir ve yeni bir alan eklemek için
merkezi bir tanımı değiştirmek gerekmez. Bedel tarafı: alan adlarının her belgede
tekrarlanması yer kaplar — bir milyon belgenin her birinde `kullanici_adi`
metnini taşımak, o adı bir kez tanımlamaktan daha savurgandır — ve adların
tutarlılığını güvence altına alan merkezi bir otorite yoktur. Bir belgede
`telefon`, başkasında `tel`, bir üçüncüsünde `telefon_no` yazarsa, sistem bunu
bir hata olarak görmez; bu tutarsızlıkla baş etmek size kalır. Bu, şema esnekliği
tartışmasının kalbinde yatan gerilimdir ve birazdan ona döneceğiz.

## JSON neden kazandı

Belgeleri yazıya dökmenin birçok yolu olabilirdi, ama pratikte bir gösterim
neredeyse evrensel hale geldi: JSON. Bunun nedenleri öğreticidir. JSON,
insan tarafından okunabilir düz metindir; bir belgeye bakıp ne olduğunu hemen
anlarsınız. Yapısı, tam da az önce anlattığımız belge yapısına uyar: adlandırılmış
alanlar, skaler değerler, listeler ve iç içe nesneler. Ve belki en önemlisi,
modern programlama dillerindeki nesnelere neredeyse doğrudan karşılık gelir;
bir programdaki nesneyi JSON'a çevirmek ve geri almak kolaydır. Bu üç özellik —
okunabilirlik, belge yapısına uygunluk ve nesnelere yakınlık — JSON'u belge
dünyasının ortak dili yaptı.

Ama JSON'un, bir veritabanının iç gösterimi olarak doğrudan kullanmak için iki
önemli eksiği vardır. Birincisi **tür yoksulluğudur**. JSON yalnızca birkaç temel
tür tanır; örneğin tarih, zaman ya da ikili (binary) veri için ayrı bir türü
yoktur. Bir tarihi JSON'da saklamak için onu bir metne ya da sayıya çevirmek
gerekir ve bu çevrimin anlamı, yorumlayan tarafın bilgisine kalır. İkincisi
**verimsizliktir**: JSON düz metindir, bu yüzden hem yer açısından savurgandır
hem de bir alanı bulmak için metni baştan ayrıştırmak gerekir; sayıları bile
metin olarak yazıp yeniden okumak zorundadır.

Bu yüzden ciddi belge veritabanları, JSON'u dışarıya — kullanıcıyla iletişimde —
kullanırken, **içeride** daha zengin ve daha verimli bir **ikili gösterime**
çevirir. Bu ikili biçimler, JSON'un tanımadığı türleri (tarih, ikili veri,
kesin sayılar) ekler ve veriyi, ayrıştırmadan doğrudan üzerinde işlem
yapılabilecek şekilde kodlar; örneğin bir alanın değerine, tüm belgeyi metin
olarak okumadan, doğrudan atlayabilirsiniz. Üçüncü kısımda OxiDB'nin tam olarak
böyle bir iç ikili gösterim kullandığını ve bunun sorgu hızına nasıl katkı
sağladığını göreceğiz. Şimdilik akılda tutulacak nokta şu: JSON belge modelinin
yüzüdür, ama bir veritabanının içinde yaşayan biçim genellikle JSON'un daha
zengin, daha sıkı bir akrabasıdır.

## Belgeler ve koleksiyonlar

Belgeler tek başlarına durmaz; bir arada gruplanırlar. Bu gruplara genellikle
**koleksiyon** denir — ilişkisel dünyadaki tablonun belge dünyasındaki
karşılığı. Ama önemli bir farkla: bir tablo, içindeki tüm satırların aynı
sütunlara sahip olmasını dayatır; bir koleksiyon ise içindeki belgelerin
birbirine benzemesini **zorunlu kılmaz**. Aynı koleksiyonda, farklı alanlara
sahip belgeler bir arada durabilir. Pratikte bir koleksiyondaki belgeler genelde
benzer biçimdedir — çünkü hepsi aynı tür "şeyi" temsil eder — ama bu benzerlik,
sistemin dayattığı bir kural değil, uygulamanın gözettiği bir gelenektir.

Her belgenin, kendisini koleksiyon içinde benzersiz biçimde tanımlayan bir
**kimliği** vardır. Bu kimlik, belgeye doğrudan, taramadan erişmenin anahtarıdır;
ikinci bölümde anahtar-değer modelinden söz ederken değindiğimiz "anahtarla
erişim" fikri burada da yaşar. Belge veritabanları, bir belgeyi kimliğiyle
bulmayı her zaman çok hızlı kılacak şekilde tasarlanır. Diğer alanlara göre arama
ise — yedinci bölümde göreceğimiz — indekslerin işidir.

## "Şemasızlık" yanılgısı: şema-okumada ve şema-yazmada

Belge veritabanları sık sık "şemasız" diye anılır, ama bu niteleme yanıltıcıdır
ve düzeltilmesi gerekir. Veri her zaman bir şemaya sahiptir — yani bir biçime,
bir beklentiye. Soru, o şemanın **nerede yaşadığı** ve **ne zaman dayatıldığıdır**.

İlişkisel modelde şema **yazma anında** dayatılır: bir satırı yazmadan önce
tablonun tanımına uymak zorundadır, aksi halde yazma reddedilir. Buna "şema-yazmada"
denir. Şema merkezîdir, açıktır ve veritabanı tarafından zorlanır. Avantajı,
verinin her zaman beklenen biçimde olmasıdır; veriyi okuyan hiç kimse "acaba bu
alan var mı, türü doğru mu" diye endişelenmek zorunda değildir. Dezavantajı,
biçimi değiştirmenin pahalı olmasıdır: yeni bir alan eklemek, var olan tüm satırları
ilgilendiren bir değişiklik gerektirir.

Belge modelinde ise şema tipik olarak **okuma anında** anlam kazanır: veritabanı,
yazdığınız belgenin biçimine karışmaz; biçimi yorumlama işi, veriyi okuyan
uygulamaya düşer. Buna "şema-okumada" denir. Şema kaybolmamıştır; yalnızca
veritabanından uygulamaya, açıktan örtüğe taşınmıştır. Avantajı esnekliktir:
biçimi değiştirmek için merkezi bir tanım güncellemek gerekmez; farklı belgeler
farklı biçimlerde olabilir; veri zamanla evrilebilir. Dezavantajı, şemayı
zorlama sorumluluğunun uygulamaya geçmesidir.

Bu devrin pratik sonuçları vardır. Esneklik, bir armağandır: gereksinimler
değiştiğinde göç (migration) denen o zahmetli, tüm veriyi dönüştüren işlemleri
yapmadan, yeni alanları yazmaya başlayabilirsiniz. Ama aynı esneklik bir tuzaktır:
veritabanı sizi tutarlı tutmadığı için, zamanla **biçim kayması** oluşabilir —
aynı koleksiyonda kimi belgelerde bir alan vardır, kimilerinde yoktur; kiminde
sayıdır, kiminde metin; kiminde `tel`, kiminde `telefon`. Veriyi okuyan kod, bu
çeşitliliğe karşı **savunmacı** olmak, eksik ya da beklenmedik alanlara hazırlıklı
olmak zorunda kalır. "Şemasızlık", "şema düşünmek gerekmiyor" demek değildir;
tersine, şema disiplinini veritabanı yerine sizin taşımanız gerektiği anlamına
gelir. İyi kullanılan belge veritabanlarında bu disiplin yok olmaz; uygulama
katmanında, bilinçli biçimde sürdürülür.

## Belge modelinin kalbindeki karar: gömmek mi, atıfta bulunmak mı

Belge modeliyle veri tasarlarken karşılaşacağınız en sık ve en belirleyici karar
şudur: birbiriyle ilişkili iki veri parçasını, birini diğerinin **içine gömerek**
mi tek bir belgede tutmalı, yoksa ayrı belgelerde tutup birinden diğerine
**atıfta bulunarak** (referansla) mı bağlamalı? Bu kararın iyi verilmesi, belge
modelini ustaca kullanmanın özüdür. Üçüncü bölümdeki çoğaltma ödünleşimi, burada
somut bir tasarım kararına dönüşür.

**Gömmek**, ilişkili veriyi ana belgenin içine yerleştirmektir. Bir blog
yazısının yorumlarını yazının kendi belgesi içinde bir liste olarak tutmak buna
örnektir. Gömmenin armağanları, üçüncü bölümde saydığımız yerellik kazançlarıdır:
veriyi tek bir okumayla, parçaları toplamadan alırsınız; ve bir belgeyi
değiştirmek atomik olduğu için, gömülü veriyi ana veriyle birlikte tutarlı biçimde
güncelleyebilirsiniz. Bedeli ise iki katmanlıdır. Birincisi, eğer gömülü bilgi
başka belgelerde de tekrarlanıyorsa, çoğaltmanın güncelleme maliyetini ve
tutarsızlık riskini üstlenirsiniz. İkincisi ve daha sinsi olanı, **sınırsız
büyüme** tehlikesidir: yorumları gömdüğünüz bir yazı yıllar içinde on binlerce
yorum biriktirirse, o belge devasa büyür; her okumada — yalnızca başlığı görmek
isteseniz bile — tüm o yorum yığınını taşımak zorunda kalırsınız. Sınırı belli
olmayan, sürekli büyüyen listeleri gömmek, belge modelinin en yaygın
tuzaklarından biridir.

**Atıfta bulunmak**, ilişkili veriyi ayrı bir belgede tutup, ona yalnızca
kimliğiyle işaret etmektir — ilişkisel modelin paylaşılan-değerle bağ kurma
fikrinin belge dünyasındaki karşılığı. Bunun armağanları normalleştirmenin
armağanlarıdır: paylaşılan bilgi tek bir yerde durur, çoğaltma olmaz, değiştirmek
ucuzdur ve belgeler şişmez. Bedeli ise yerelliğin kaybıdır: ilişkili veriyi
almak için birden fazla okuma ya da uygulama tarafında bir birleştirme gerekir.

Bu iki seçenek arasında karar verirken birkaç yol gösterici ilke yardımcı olur.
İlişkili veri, ana varlığın **ayrılmaz bir parçasıysa** ve onunla **birlikte
okunup yazılıyorsa** — bir adresin bir kullanıcıya, bir sipariş kaleminin bir
siparişe ait olması gibi — ve büyümesi **sınırlıysa**, gömmek doğaldır. İlişkili
veri **birçok varlık tarafından paylaşılıyorsa**, **bağımsız olarak
sorgulanıyorsa**, **sık değişiyorsa** ya da **sınırsız büyüyebiliyorsa**, atıfta
bulunmak daha sağlamdır. Ve sık karşılaşılan çoktan-çoğa ilişkilerde — ikinci
bölümden beri peşimizi bırakmayan o zorlu örüntü — neredeyse her zaman atıf
tercih edilir, çünkü gömmek kaçınılmaz biçimde çoğaltmaya yol açar.

Bu kararın tek ve kesin bir doğru yanıtı yoktur; doğru yanıt, üçüncü bölümde
gördüğümüz gibi, erişim örüntülerinize bağlıdır. Aynı "kullanıcı ve adres"
ilişkisi, bir uygulamada gömülecek, başka birinde referansla bağlanacaktır.
Belge modelini iyi kullanmak, bu kararı her ilişki için bilinçli biçimde,
erişim örüntülerine bakarak vermektir.

## Atomikliğin sınırı belgedir

Son olarak, ileride önemli olacak bir noktayı şimdiden tohumlayalım. Belge
veritabanlarında, üzerinde "ya hep ya hiç" güvencesi en kolay verilen birim, tek
bir belgedir. Bir belgeyi değiştirdiğinizde, o değişiklik ya tümüyle gerçekleşir
ya da hiç gerçekleşmez; arada bir durum olmaz. İşte gömmenin daha önce
değinmediğimiz bir avantajı buradan doğar: ilişkili veriyi tek bir belgede
topladığınızda, onları birlikte, tek bir atomik işlemde güncelleyebilirsiniz.
Veriyi ayrı belgelere dağıttığınızda ise, birden çok belgeyi tutarlı biçimde
değiştirmek daha güçlü işlem güvenceleri gerektirir. Bu, üçüncü bölümdeki
"tutarlı kalması gereken birim ne kadar büyük" sorusunun belge modelindeki
yankısıdır ve onuncu bölümde işlemleri ele alırken bütün ağırlığıyla geri
gelecek.

Böylece Kısım I'i tamamlamış olduk. Artık belgenin ne olduğunu — alanlardan,
iç içe yapılardan, kendini tanımlayan veriden oluştuğunu; JSON'la gösterilip
içeride daha zengin bir ikili biçimde yaşadığını; koleksiyonlarda gruplandığını;
şemasının yazma yerine okuma anında anlam kazandığını; ve onunla çalışmanın
kalbinde gömme-referans kararının durduğunu — biliyoruz. Şimdiye dek hep
*mantıksal* düzeyde, yani veriyi nasıl düşündüğümüz düzeyinde kaldık. Kısım II ile
birlikte perdeyi aralıyor ve *fiziksel* düzeye iniyoruz: bir belge veritabanı,
tüm bu belgeleri diske tam olarak nasıl yazar, çökmeden nasıl korur, aradığımızı
taramadan nasıl bulur ve sorularımızı nasıl yanıtlar? Bir sonraki bölümde işin
en altından, depolama motorundan başlıyoruz.
