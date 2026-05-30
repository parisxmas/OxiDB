# Ek B — Kaynaklar {-}

Bu kitap, belge veritabanlarını ve OxiDB'yi temelden, kod kullanmadan anlatmayı
amaçladı. Daha derine inmek isteyenler için, burada konuları temalara göre
gruplanmış bir okuma ve inceleme rehberi sunuyoruz. Önce kaynakları, ne
içerdikleriyle ve hangi yöne baktıklarıyla temalara göre tanıtıyoruz; ardından,
bu bölümün sonundaki **Kaynakça**'da, metin boyunca atıfta bulunduğumuz eserlerin
tam künyelerini ve erişim bağlantılarını (URL/DOI) topluyoruz. Böylece hem bir
pusula hem de doğrudan başvurabileceğiniz kesin bir referans listesi elinizde
olur. Gövde metnindeki **(yazar, yıl)** biçimindeki kısa atıflar, bu listedeki
maddelere işaret eder.

## Genel: veri-yoğun sistemlerin temelleri

Bu kitabın ikinci kısmında ele aldığımız ilkelerin — depolama, dayanıklılık,
indeksleme, replikasyon, tutarlılık — derinlemesine ve bütünsel bir incelemesi
için, Martin Kleppmann'ın *Designing Data-Intensive Applications* adlı eseri (Kleppmann, 2017),
alanın en yaygın başvurulan modern kaynağıdır. Veritabanı motorlarının iç
mekanizmalarına — depolama düzenleri, B-ağaçları, LSM ağaçları, işlem ve
dağıtım — daha doğrudan inen bir kaynak için Alex Petrov'un *Database Internals*
kitabı önerilir (Petrov, 2019). Bu iki eser, bu kitabın ikinci kısmının doğal devamı niteliğindedir.

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

## Kaynakça

Aşağıdaki künyeler, metin boyunca **(yazar, yıl)** biçiminde atıfta bulunulan
eserlerin tam listesidir. Bağlantılar, mümkün olduğunda kalıcı DOI ya da resmî
sayfa adresleridir; erişim tarihinde geçerliydiler.

1. Codd, E. F. "A Relational Model of Data for Large Shared Data Banks."
   *Communications of the ACM*, 13(6), 1970, ss. 377–387.
   <https://doi.org/10.1145/362384.362685>

2. Bayer, R. ve McCreight, E. M. "Organization and Maintenance of Large Ordered
   Indexes." *Acta Informatica*, 1(3), 1972, ss. 173–189.
   <https://doi.org/10.1007/BF00288683>

3. Gray, J. "The Transaction Concept: Virtues and Limitations." *Proc. 7th Int.
   Conf. on Very Large Data Bases (VLDB '81)*, 1981, ss. 144–154.
   <https://dl.acm.org/doi/10.5555/1286831.1286846>

4. Kung, H. T. ve Robinson, J. T. "On Optimistic Methods for Concurrency
   Control." *ACM Transactions on Database Systems*, 6(2), 1981, ss. 213–226.
   <https://doi.org/10.1145/319566.319567>

5. Mohan, C.; Haderle, D.; Lindsay, B.; Pirahesh, H.; Schwarz, P. "ARIES: A
   Transaction Recovery Method Supporting Fine-Granularity Locking and Partial
   Rollbacks Using Write-Ahead Logging." *ACM Transactions on Database Systems*,
   17(1), 1992, ss. 94–162. <https://doi.org/10.1145/128765.128770>

6. Berenson, H.; Bernstein, P.; Gray, J.; Melton, J.; O'Neil, E.; O'Neil, P. "A
   Critique of ANSI SQL Isolation Levels." *Proc. ACM SIGMOD 1995*, ss. 1–10.
   <https://doi.org/10.1145/223784.223785>

7. O'Neil, P.; Cheng, E.; Gawlick, D.; O'Neil, E. "The Log-Structured Merge-Tree
   (LSM-Tree)." *Acta Informatica*, 33(4), 1996, ss. 351–385.
   <https://doi.org/10.1007/s002360050048>

8. Gilbert, S. ve Lynch, N. "Brewer's Conjecture and the Feasibility of
   Consistent, Available, Partition-Tolerant Web Services." *ACM SIGACT News*,
   33(2), 2002, ss. 51–59. <https://doi.org/10.1145/564585.564601>

9. Manning, C. D.; Raghavan, P.; Schütze, H. *Introduction to Information
   Retrieval.* Cambridge University Press, 2008.
   <https://nlp.stanford.edu/IR-book/information-retrieval-book.html>

10. Robertson, S. ve Zaragoza, H. "The Probabilistic Relevance Framework: BM25
    and Beyond." *Foundations and Trends in Information Retrieval*, 3(4), 2009,
    ss. 333–389. <https://doi.org/10.1561/1500000019>

11. Brewer, E. "CAP Twelve Years Later: How the 'Rules' Have Changed."
    *Computer (IEEE)*, 45(2), 2012, ss. 23–29.
    <https://doi.org/10.1109/MC.2012.37>

12. Abadi, D. J. "Consistency Tradeoffs in Modern Distributed Database System
    Design: CAP is Only Part of the Story." *Computer (IEEE)*, 45(2), 2012,
    ss. 37–42. <https://doi.org/10.1109/MC.2012.33>

13. Ongaro, D. ve Ousterhout, J. "In Search of an Understandable Consensus
    Algorithm." *Proc. 2014 USENIX Annual Technical Conference (USENIX ATC '14)*,
    ss. 305–319. <https://raft.github.io/raft.pdf>

14. Kleppmann, M. *Designing Data-Intensive Applications.* O'Reilly Media, 2017.
    <https://dataintensive.net/>

15. Petrov, A. *Database Internals: A Deep Dive into How Distributed Data Systems
    Work.* O'Reilly Media, 2019. <https://www.databass.dev/>

16. Crockford, D. "Introducing JSON." json.org. <https://www.json.org/>

17. Ecma International. *ECMA-404: The JSON Data Interchange Syntax.* 2. baskı,
    Aralık 2017.
    <https://ecma-international.org/publications-and-standards/standards/ecma-404/>

18. Bray, T. (Ed.). *RFC 8259: The JavaScript Object Notation (JSON) Data
    Interchange Format.* IETF, STD 90, Aralık 2017.
    <https://www.rfc-editor.org/rfc/rfc8259.html>
