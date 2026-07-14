# Ek B — Kaynaklar {-}

Bu kitap, belge veritabanlarını ve OxiDB'yi temelden, kod kullanmadan anlatmayı
amaçladı. Daha derine inmek isteyenler için, burada konuları temalara göre
gruplanmış bir okuma ve inceleme rehberi sunuyoruz. Önce kaynakları, ne
içerdikleriyle ve hangi yöne baktıklarıyla temalara göre tanıtıyoruz; ardından,
bu bölümün sonundaki **Kaynakça**'da, metin boyunca atıfta bulunduğumuz eserlerin
tam künyelerini ve erişim bağlantılarını (URL/DOI) topluyoruz. Böylece hem bir
pusula hem de doğrudan başvurabileceğiniz kesin bir referans listesi elinizde
olur. Kitap boyunca, bir eserden ilk söz edilen sayfada onun künyesi **sayfa
altındaki dipnotta** verilir; aşağıdaki Kaynakça ise bu eserleri tek bir yerde,
erişim bağlantılarıyla birlikte toplar.

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

Aşağıdaki künyeler, kitap boyunca **sayfa altı dipnotlarda** anılan eserlerin
kronolojik, tam listesidir. Bağlantılar mümkün olduğunda kalıcı DOI ya da resmî
sayfa adresleridir; erişim tarihinde geçerliydiler. Bazı konferans yayınlarının,
kitapların ve derleme ciltlerin (USENIX, BSDCan vb.) kalıcı bir DOI'si yoktur;
bunlarda resmî sayfa adresi verilmiştir.

1. Denning, P. J. "The Working Set Model for Program Behavior."
    *Communications of the ACM*, 11(5), 1968, ss. 323–333.
    <https://doi.org/10.1145/363095.363141>

2. Bloom, B. H. "Space/Time Trade-offs in Hash Coding with Allowable
    Errors." *Communications of the ACM*, 13(7), 1970, ss. 422–426.
    <https://doi.org/10.1145/362686.362692>

3. Codd, E. F. "A Relational Model of Data for Large Shared Data Banks."
    *Communications of the ACM*, 13(6), 1970, ss. 377–387.
    <https://doi.org/10.1145/362384.362685>

4. Bayer, R. ve McCreight, E. M. "Organization and Maintenance of Large
    Ordered Indexes." *Acta Informatica*, 1(3), 1972, ss. 173–189.
    <https://doi.org/10.1007/BF00288683>

5. Codd, E. F. "Further Normalization of the Data Base Relational Model." R.
    Rustin (ed.), *Data Base Systems* (Courant Computer Science Symposia 6),
    Prentice-Hall, 1972, ss. 33–64.

6. Bachman, C. W. "The Programmer as Navigator." *Communications of the
    ACM*, 16(11), 1973, ss. 653–658. <https://doi.org/10.1145/355611.362534>

7. Lamport, L. "Time, Clocks, and the Ordering of Events in a Distributed
    System." *Communications of the ACM*, 21(7), 1978, ss. 558–565.
    <https://doi.org/10.1145/359545.359563>

8. Gifford, D. K. "Weighted Voting for Replicated Data." *Proc. ACM SOSP*,
    1979, ss. 150–162. <https://doi.org/10.1145/800215.806583>

9. Selinger, P. G.; Astrahan, M. M.; Chamberlin, D. D.; Lorie, R. A.; Price,
    T. G. "Access Path Selection in a Relational Database Management
    System." *Proc. ACM SIGMOD*, 1979, ss. 23–34.
    <https://doi.org/10.1145/582095.582099>

10. Gray, J. "The Transaction Concept: Virtues and Limitations." *Proc. 7th
    Int. Conf. on Very Large Data Bases (VLDB '81)*, 1981, ss. 144–154.
    <https://dl.acm.org/doi/10.5555/1286831.1286846>

11. Kung, H. T. ve Robinson, J. T. "On Optimistic Methods for Concurrency
    Control." *ACM Transactions on Database Systems*, 6(2), 1981, ss.
    213–226. <https://doi.org/10.1145/319566.319567>

12. Bernstein, P. A. ve Goodman, N. "Multiversion Concurrency Control—Theory
    and Algorithms." *ACM Transactions on Database Systems*, 8(4), 1983, ss.
    465–483. <https://doi.org/10.1145/319996.319998>

13. Fischer, M. J.; Lynch, N. A.; Paterson, M. S. "Impossibility of
    Distributed Consensus with One Faulty Process." *Journal of the ACM*,
    32(2), 1985, ss. 374–382. <https://doi.org/10.1145/3149.214121>

14. Herlihy, M. P. ve Wing, J. M. "Linearizability: A Correctness Condition
    for Concurrent Objects." *ACM Transactions on Programming Languages and
    Systems*, 12(3), 1990, ss. 463–492.
    <https://doi.org/10.1145/78969.78972>

15. Pugh, W. "Skip Lists: A Probabilistic Alternative to Balanced Trees."
    *Communications of the ACM*, 33(6), 1990, ss. 668–676.
    <https://doi.org/10.1145/78973.78977>

16. Gray, J. ve Reuter, A. *Transaction Processing: Concepts and
    Techniques.* Morgan Kaufmann, 1992. ISBN 978-1-55860-190-1.

17. Mohan, C.; Haderle, D.; Lindsay, B.; Pirahesh, H.; Schwarz, P. "ARIES: A
    Transaction Recovery Method Supporting Fine-Granularity Locking and
    Partial Rollbacks Using Write-Ahead Logging." *ACM Transactions on
    Database Systems*, 17(1), 1992, ss. 94–162.
    <https://doi.org/10.1145/128765.128770>

18. Graefe, G. "Volcano—An Extensible and Parallel Query Evaluation System."
    *IEEE Transactions on Knowledge and Data Engineering*, 6(1), 1994, ss.
    120–135. <https://doi.org/10.1109/69.273032>

19. Berenson, H.; Bernstein, P.; Gray, J.; Melton, J.; O'Neil, E.; O'Neil,
    P. "A Critique of ANSI SQL Isolation Levels." *Proc. ACM SIGMOD*, 1995,
    ss. 1–10. <https://doi.org/10.1145/223784.223785>

20. O'Neil, P.; Cheng, E.; Gawlick, D.; O'Neil, E. "The Log-Structured
    Merge-Tree (LSM-Tree)." *Acta Informatica*, 33(4), 1996, ss. 351–385.
    <https://doi.org/10.1007/s002360050048>

21. Karger, D.; Lehman, E.; Leighton, T.; Panigrahy, R.; Levine, M.; Lewin,
    D. "Consistent Hashing and Random Trees: Distributed Caching Protocols
    for Relieving Hot Spots on the World Wide Web." *Proc. ACM STOC*, 1997,
    ss. 654–663. <https://doi.org/10.1145/258533.258660>

22. Lamport, L. "The Part-Time Parliament." *ACM Transactions on Computer
    Systems*, 16(2), 1998, ss. 133–169.
    <https://doi.org/10.1145/279227.279229>

23. Adya, A.; Liskov, B.; O'Neil, P. "Generalized Isolation Level
    Definitions." *Proc. IEEE ICDE*, 2000, ss. 67–78.
    <https://doi.org/10.1109/ICDE.2000.839388>

24. National Institute of Standards and Technology. *FIPS PUB 197: Advanced
    Encryption Standard (AES).* NIST, 2001.
    <https://doi.org/10.6028/NIST.FIPS.197>

25. Gilbert, S. ve Lynch, N. "Brewer's Conjecture and the Feasibility of
    Consistent, Available, Partition-Tolerant Web Services." *ACM SIGACT
    News*, 33(2), 2002, ss. 51–59. <https://doi.org/10.1145/564585.564601>

26. Megiddo, N. ve Modha, D. S. "ARC: A Self-Tuning, Low Overhead
    Replacement Cache." *Proc. USENIX FAST*, 2003.
    <https://www.usenix.org/conference/fast-03/arc-self-tuning-low-overhead-replacement-cache>

27. DeCandia, G.; Hastorun, D.; Jampani, M.; Kakulapati, G.; Lakshman, A.;
    Pilchin, A.; Sivasubramanian, S.; Vosshall, P.; Vogels, W. "Dynamo:
    Amazon's Highly Available Key-value Store." *Proc. ACM SOSP*, 2007, ss.
    205–220. <https://doi.org/10.1145/1294261.1294281>

28. Cahill, M. J.; Röhm, U.; Fekete, A. D. "Serializable Isolation for
    Snapshot Databases." *Proc. ACM SIGMOD*, 2008, ss. 729–738.
    <https://doi.org/10.1145/1376616.1376690>

29. Manning, C. D.; Raghavan, P.; Schütze, H. *Introduction to Information
    Retrieval.* Cambridge University Press, 2008.
    <https://nlp.stanford.edu/IR-book/information-retrieval-book.html>

30. Percival, C. "Stronger Key Derivation via Sequential Memory-Hard
    Functions." *BSDCan*, 2009. <https://www.tarsnap.com/scrypt/scrypt.pdf>

31. Robertson, S. ve Zaragoza, H. "The Probabilistic Relevance Framework:
    BM25 and Beyond." *Foundations and Trends in Information Retrieval*,
    3(4), 2009, ss. 333–389. <https://doi.org/10.1561/1500000019>

32. Newman, C.; Menon-Sen, A.; Melnikov, A.; Williams, N. *RFC 5802: Salted
    Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API
    Mechanisms.* IETF, 2010. <https://doi.org/10.17487/RFC5802>

33. Abadi, D. J. "Consistency Tradeoffs in Modern Distributed Database
    System Design: CAP is Only Part of the Story." *Computer (IEEE)*, 45(2),
    2012, ss. 37–42. <https://doi.org/10.1109/MC.2012.33>

34. Brewer, E. "CAP Twelve Years Later: How the 'Rules' Have Changed."
    *Computer (IEEE)*, 45(2), 2012, ss. 23–29.
    <https://doi.org/10.1109/MC.2012.37>

35. Ongaro, D. ve Ousterhout, J. "In Search of an Understandable Consensus
    Algorithm." *Proc. 2014 USENIX Annual Technical Conference (USENIX ATC
    '14)*, ss. 305–319. <https://raft.github.io/raft.pdf>

36. T. Hansen. *RFC 7677: SCRAM-SHA-256 and SCRAM-SHA-256-PLUS Simple
    Authentication and Security Layer (SASL) Mechanisms.* IETF, 2015.
    <https://doi.org/10.17487/RFC7677>

37. Biryukov, A.; Dinu, D.; Khovratovich, D. "Argon2: New Generation of
    Memory-Hard Functions for Password Hashing and Other Applications."
    *Proc. IEEE EuroS&P*, 2016, ss. 292–302.
    <https://doi.org/10.1109/EuroSP.2016.31>

38. Lu, L.; Pillai, T. S.; Arpaci-Dusseau, A. C.; Arpaci-Dusseau, R. H.
    "WiscKey: Separating Keys from Values in SSD-Conscious Storage." *Proc.
    USENIX FAST*, 2016.
    <https://www.usenix.org/conference/fast16/technical-sessions/presentation/lu>

39. Bray, T. (ed.). *RFC 8259: The JavaScript Object Notation (JSON) Data
    Interchange Format.* IETF, STD 90, 2017.
    <https://www.rfc-editor.org/rfc/rfc8259.html>

40. Ecma International. *ECMA-404: The JSON Data Interchange Syntax.* 2.
    baskı, 2017.
    <https://ecma-international.org/publications-and-standards/standards/ecma-404/>

41. Kleppmann, M. *Designing Data-Intensive Applications.* O'Reilly Media,
    2017. <https://dataintensive.net/>

42. Petrov, A. *Database Internals: A Deep Dive into How Distributed Data
    Systems Work.* O'Reilly Media, 2019. <https://www.databass.dev/>

43. Bormann, C. ve Hoffman, P. *RFC 8949: Concise Binary Object
    Representation (CBOR).* IETF, 2020. <https://doi.org/10.17487/RFC8949>

44. Collet, Y. ve Kucherawy, M. (ed.). *RFC 8878: Zstandard Compression and
    the 'application/zstd' Media Type.* IETF, 2021.
    <https://doi.org/10.17487/RFC8878>

Aşağıdaki kaynaklar, sürümsüz/canlı çevrimiçi belgelerdir:

45. Crockford, D. "Introducing JSON." json.org. <https://www.json.org/>

46. *BSON (Binary JSON) Specification.* MongoDB Inc.
    <https://bsonspec.org/spec.html>

47. *JSON Schema — Specification.* JSON Schema Org.
    <https://json-schema.org/specification>
