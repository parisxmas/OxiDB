# Ek B — Kaynaklar {-}

Bu kitap, belge veritabanlarını ve OxiDB'yi temelden, kod kullanmadan anlatmayı
amaçladı. Daha derine inmek isteyenler için, burada konuları temalara göre
gruplanmış bir okuma ve inceleme rehberi sunuyoruz. Belirli kaynakları, kesin
yayın künyeleri yerine ne içerdikleriyle tanıttık; çünkü amaç, bir bibliyografya
değil, hangi yöne bakacağınıza dair bir pusula vermektir.

## Genel: veri-yoğun sistemlerin temelleri

Bu kitabın ikinci kısmında ele aldığımız ilkelerin — depolama, dayanıklılık,
indeksleme, replikasyon, tutarlılık — derinlemesine ve bütünsel bir incelemesi
için, Martin Kleppmann'ın *Designing Data-Intensive Applications* adlı eseri,
alanın en yaygın başvurulan modern kaynağıdır. Veritabanı motorlarının iç
mekanizmalarına — depolama düzenleri, B-ağaçları, LSM ağaçları, işlem ve
dağıtım — daha doğrudan inen bir kaynak için Alex Petrov'un *Database Internals*
kitabı önerilir. Bu iki eser, bu kitabın ikinci kısmının doğal devamı niteliğindedir.

## Veri modelleri ve ilişkisel temel

Üçüncü bölümde değindiğimiz ilişkisel modelin temelini atan, E. F. Codd'un 1970
tarihli "A Relational Model of Data for Large Shared Data Banks" makalesi, alanın
kurucu metnidir ve mantık-fizik ayrışması fikrinin kaynağıdır. Belge modelinin
pratiğini ve gömme-referans kararını daha somut görmek için, yaygın belge
veritabanlarının (örneğin MongoDB'nin) veri modelleme kılavuzları, kavramları
gerçek senaryolarla ilişkilendirir.

## JSON ve belge biçimleri

Dördüncü bölümde tarihçesini anlattığımız JSON'un sade tanımı ve dilbilgisi için
`json.org` adresi, biçimin kaynağıdır. Biçimin resmî, kesin tanımları, yazılım
endüstrisi ve internet standart kuruluşlarının yayımladığı şartnamelerde bulunur.
Veritabanlarının içeride kullandığı, JSON'un daha zengin ikili akrabaları
(belge veritabanlarının ikili kodlamaları gibi) hakkında, ilgili sistemlerin
biçim belgeleri ayrıntı sağlar.

## Depolama motorları ve indeksleme

Beşinci ve yedinci bölümlerin konuları — B-ağaçları, log-yapılı ve LSM
depolama, indeks yapıları — klasik veritabanı ders kitaplarında ve yukarıda anılan
*Database Internals* eserinde kapsamlı biçimde işlenir. Tam metin aramanın
temelindeki alaka puanlama yöntemleri (terim sıklığı temelli yaklaşımlar ve onların
olgun biçimleri), bilgi erişimi (information retrieval) alanının standart
kaynaklarında ele alınır.

## İşlemler, eşzamanlılık ve tutarlılık

Onuncu ve on birinci bölümlerin konuları — ACID, yalıtım düzeyleri, kilitleme,
çok sürümlü ve iyimser eşzamanlılık denetimi — veritabanı kuramının klasik
metinlerinde derinlemesine incelenir. Dağıtık tutarlılık tarafında, on birinci
bölümde değindiğimiz CAP içgörüsü Eric Brewer'a; ilgili pratik tutarlılık modelleri
ise dağıtık sistemler yazınına dayanır.

## Ölçeklendirme ve konsensüs

On ikinci ve yirmi beşinci bölümlerde anlattığımız konsensüs, OxiDB'nin de
dayandığı Raft protokolünün temelinde yatar; Diego Ongaro ve John Ousterhout'un
"In Search of an Understandable Consensus Algorithm" başlıklı çalışması, Raft'ı
anlaşılır biçimde tanıtan kurucu metindir ve konsensüsün neden çoğunlukla
çalıştığını kavramak için en iyi başlangıçtır.

## OxiDB'nin kendi kaynakları

Üçüncü kısımda anlattığımız OxiDB'nin somut tasarımına daha yakından bakmak
isteyenler için en doğrudan kaynak, projenin açık kaynak deposudur (`parisxmas/OxiDB`).
Deponun içinde, bu kitapta değindiğimiz birçok mühendislik kararının ardındaki
gerekçeyi bulabileceğiniz birkaç materyal vardır. Mimari karar kayıtları
(`docs/decisions/` altındaki belgeler), disk-öncelikli depolama ve sıkıştırma gibi
konularda yapılan tercihleri ve ödünleşimleri belgeler. Karşılaştırmalı
değerlendirme materyali (`tests/benchmark-1m/` altındaki rapor), yirmi yedinci
bölümde özetlediğimiz ölçümlerin ayrıntısını içerir. Sürüm değişiklik günlüğü
(`CHANGELOG.md`), bu kitap yazılırken eklenen yeteneklerin — çok-yönlü analiz,
pencere fonksiyonları, sıkıştırmasız kip, per-koleksiyon ayarlar — gerekçeleriyle
birlikte kaydını tutar. Ve doğrulama testleri (örneğin küme ve parçalar-arası
toplama testleri), yirmi beşinci bölümde anlattığımız davranışların nasıl sınandığını
gösterir.

## Son bir söz

Bu kaynaklar, bir bitiş değil, bir başlangıçtır. Bu kitabın amacı, size belirli
bir sistemi ezberletmek değil, herhangi bir belge veritabanına baktığınızda onun
altındaki ilkeleri ve ödünleşimleri görebilecek bir bakış kazandırmaktı. O bakışla
donanmış olarak, yukarıdaki kaynaklara — ya da karşınıza çıkacak herhangi bir veri
sistemine — artık çok daha hazırlıklı yaklaşabilirsiniz. İyi okumalar.
