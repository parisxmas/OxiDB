# Ek A — Sözlük {-}

Bu sözlük, kitap boyunca kullanılan başlıca terimleri kısaca tanımlar.
Tanımlar, terimin ilk geçtiği bölümdeki ayrıntılı anlatımın yerini tutmaz;
hızlı bir hatırlatma olarak düşünülmüştür.

**ACID**
:   Bir işlemin verdiği dört güvencenin kısaltması: atomiklik, tutarlılık,
    yalıtım ve dayanıklılık.

**Açma (unwind)**
:   Bir toplama aşaması; içinde liste barındıran bir belgeyi, listenin her
    öğesi için bir tane olmak üzere birçok belgeye genişletir.

**AEAD (kimliği doğrulanmış şifreleme)**
:   Gizliliği ve bütünlüğü birlikte sağlayan; çözerken bir doğrulama
    etiketiyle veriyi kurcalamaya karşı koruyan şifreleme kipi (örneğin AES-
    GCM).

**Anahtar-değer modeli**
:   Veriyi bir anahtardan opak bir değere giden eşleme olarak tutan, en
    yalın veri modeli.

**Anlık görüntü yalıtımı (snapshot isolation)**
:   Her işlemin, başladığı andaki tutarlı bir veri anlık görüntüsünü gördüğü
    MVCC tabanlı yalıtım düzeyi; serileştirilebilir değildir (yazma
    eğriltmesine açıktır).

**Append-only (yalnızca-ekleme)**
:   Var olan veriyi yerinde değiştirmeyen, her yazmayı dosyanın sonuna
    ekleyen depolama yaklaşımı; ardışık yazma hızlıdır, ölü alan biriktirir.

**ARIES**
:   Yazma-öncesi günlüğe dayanan klasik kurtarma yöntemi; analiz, yineleme
    (redo) ve geri-alma (undo) fazlarıyla çökme sonrası tutarlılığı sağlar.

**Atlama listesi (skip list)**
:   Sıralı bağlı listeye olasılıksal "ekspres şeritler" ekleyerek logaritmik
    aramayı kilitsiz biçimde sağlayan, dengeli ağaçlara olasılıksal bir
    alternatif.

**Atomiklik**
:   Bir işlemin tüm adımlarının ya hep birlikte gerçekleşmesi ya da hiç
    gerçekleşmemesi güvencesi.

**Ayrıştırma (parsing)**
:   Ham bir sorguyu, sistemin üzerinde işlem yapabileceği yapısal bir koşul
    ağacına çevirme adımı.

**B-ağacı**
:   Veriyi sıralı ve dengeli tutan, hem tekil hem aralık aramalarında
    verimli bir ağaç yapısı; sayfa tabanlı depolama motorlarının ve sıralı
    indekslerin temelidir.

**B+ağacı**
:   Verinin yalnızca yapraklarda tutulduğu, yaprakların bağlı liste
    oluşturduğu B-ağacı türevi; aralık taramalarını hızlandırır.

**Bayt düzeyinde süzme**
:   OxiDB'nin, bir belgeyi koşula uyup uymadığını anlamak için onu nesneye
    çevirmeden, kodlanmış baytları üzerinde denetleyen ve eşleşmeyenleri hiç
    çözmeden eleyen tekniği.

**Belge (doküman)**
:   Alanlardan oluşan, değerleri skaler, liste ya da iç içe belge olabilen,
    kendini tanımlayan veri birimi.

**Bellek-zoru fonksiyon (memory-hard)**
:   Kasıtlı olarak çok bellek isteyen, böylece özel donanımla kaba kuvvet
    saldırısını pahalılaştıran parola özetleme yaklaşımı (scrypt, Argon2).

**Bileşik indeks**
:   Birden çok alanın birleşimi üzerine kurulu indeks; alan sırasının baştan
    başlayan bir önekini kullanan sorgulara yarar (önek kuralı).

**Biriktirici (accumulator)**
:   Gruplama sırasında her grup için bir özet değer hesaplayan işlem; sayma,
    toplama, ortalama, en büyük, en küçük gibi.

**Birleştirme algoritmaları (join)**
:   İki veri kümesini eşleştirme yöntemleri; iç içe döngü, hash
    birleştirmesi ve sırala-birleştir, farklı boyut ve sıralılık
    koşullarında üstün gelir.

**Bloom filtresi**
:   Bir öğenin kümede "kesinlikle yok" ya da "olabilir" olduğunu az yer
    kullanarak söyleyen olasılıksal yapı; LSM okuma yolunda gereksiz disk
    erişimini önler.

**Boyut-katmanlı sıkıştırma (size-tiered)**
:   LSM'de benzer boyuttaki parçaları birleştiren; yazma büyütmesini düşük,
    ama yer ve okuma büyütmesini yüksek tutan birleştirme stratejisi.

**CAP**
:   Ağ bölündüğünde, bir dağıtık sistemin tutarlılık ile erişilebilirlik
    arasında seçim yapmak zorunda olduğunu ifade eden içgörü.

**Çalışma kümesi (working set)**
:   Verinin, herhangi bir anda etkin biçimde kullanılan sıcak kısmı; belleğe
    sığıp sığmaması performansı belirler.

**Çift yazma (double-write)**
:   Yarım yazma riskine karşı, sayfayı önce ayrı bir tampona, sonra asıl
    yerine yazan koruma; append-only tasarımlar buna gerek duymaz.

**Çoğunluk (quorum)**
:   Bir kararın geçerli sayılması için gereken düğüm çoğunluğu; herhangi iki
    çoğunluğun kesişmesi, konsensüsün güvenliğini sağlar.

**Dağıt-topla (scatter-gather)**
:   Sharding'de, parça anahtarı içermeyen bir isteğin tüm parçalara
    gönderilip kısmi yanıtların birleştirilmesi örüntüsü.

**Dayanıklılık (durability)**
:   Bir işlem tamamlandı dendikten sonra, hiçbir çökmenin onu geri alamaması
    güvencesi.

**Denetim (audit)**
:   "Kim, ne zaman, ne yaptı" sorusunu yanıtlayan, hesap verebilirlik için
    tutulan kayıt.

**Denetim noktası (checkpoint)**
:   Yazma-öncesi günlüğün, değişiklikler asıl depoya güvenle yansıdıktan
    sonra kısaltıldığı senkronizasyon anı.

**Disk-öncelikli (disk-first)**
:   OxiDB'nin, bellekte yalnızca kompakt bir kimlik-konum dizini tutup belge
    gövdelerini diske bırakan, bellek-tutumlu depolama kipi.

**Doğrusallaştırılabilirlik (linearizability)**
:   Her işlemin, çağrısıyla dönüşü arasında tek bir anda gerçekleşmiş gibi
    göründüğü, gerçek-zaman sırasına saygılı en güçlü tutarlılık.

**Dolum oranı (fill factor)**
:   Bir B-ağacı düğümünün ne kadarının dolu olduğu; düşük doluluk yer
    israfı, yüksek doluluk sık bölünme demektir.

**Erken sonlanma**
:   Bir sorgunun, gereken sonuca ulaştığı an durup geri kalanı üretmemesi;
    sıralı indeksli "ilk N" sorgularında ve tekil işlemlerde kullanılır.

**Eşzamansız replikasyon**
:   Liderin, bir yazmayı takipçilere iletmeyi beklemeden onayladığı, hızlı
    ama kayıp riski taşıyan replikasyon biçimi.

**Failover (devralma)**
:   Lider çöktüğünde, bir takipçinin yeni lider olarak yükseltilmesi süreci.

**Fanout (çıkış genişliği)**
:   Bir B-ağacı düğümünün çocuk sayısı; yüksek fanout ağacı sığlaştırır ve
    aramada gereken disk erişimini azaltır.

**fdatasync**
:   fsync'in, yalnızca veriyi diske işleyip üstveri güncellemesini
    atlayabilen, biraz daha hafif türevi.

**Fence pointer (sınır işaretçisi)**
:   Bir SSTable içinde aranan anahtara hızlıca atlamayı sağlayan seyrek
    indeks.

**FFI**
:   Bir dilde yazılmış çekirdeği, C uyumlu bir arayüz üzerinden başka
    dillerden çağrılabilir kılan köprü.

**FLP imkânsızlığı**
:   Tümüyle eşzamansız bir ağda tek bir düğüm bile sessizce durabiliyorsa,
    konsensüsün her durumda sonlanmasının garanti edilemeyeceğini gösteren
    sonuç.

**fsync**
:   Yazılmış verinin gerçekten kalıcı ortama işlendiğinden emin olmak için
    verilen, güçlü ama yavaş boşaltma emri.

**Gömme (embedding)**
:   İlişkili veriyi, ona ait olduğu belgenin içine yerleştirme; yerellik
    kazandırır, çoğaltma ve sınırsız büyüme riski getirir.

**Gönderim listesi (posting list)**
:   Ters indekste bir sözcüğün altında tutulan; o sözcüğü içeren belgelerin
    ve sıklık/konum bilgisinin listesi.

**Grup commit (group commit)**
:   Birçok işlemin tamamlamasını tek bir fsync'te toplayarak dayanıklılık
    maliyetini paylaştıran teknik; gecikme karşılığında iş hacmi kazandırır.

**Gruplama**
:   Belgeleri bir anahtara göre gruplara ayırıp her grup için
    biriktiricilerle özet hesaplayan, satırları çökerten toplama işlemi.

**GSN (küresel sıra numarası)**
:   Zaman-noktasına kurtarmada tüm yazmalara verilen, koleksiyonlar arası
    tek ve monoton artan sıralama ekseni.

**İki fazlı kilitleme (2PL)**
:   İşlemin önce yalnızca kilit aldığı (büyüme), sonra yalnızca bıraktığı
    (küçülme) iki fazla serileştirilebilirliği sağlayan kilit protokolü.

**İndeks**
:   Bir alanın değerlerinden belgelerin konumuna giden, aramayı hızlandıran,
    veriden türetilmiş yardımcı yapı.

**İşlem (transaction)**
:   Birçok okuma-yazma adımını tek bir bölünmez birim olarak ele alan, "ya
    hep ya hiç" güvencesi veren kavram.

**İyimser eşzamanlılık denetimi (OCC)**
:   Çatışmaların nadir olduğunu varsayan, kilit almadan çalışıp tamamlama
    anında sürüm doğrulayan ve çatışmada iptal eden yaklaşım; OxiDB'nin
    kullandığı model.

**JSON**
:   JavaScript nesne gösteriminden türeyen, dilden bağımsız, metinsel veri
    değiş-tokuş biçimi; belge yapısının yazıya dökülmüş hâli.

**Kapsayan indeks (covering)**
:   Bir sorgunun ihtiyaç duyduğu her şeyi tek başına sağlayan ve böylece
    belgeye hiç dokunmadan yanıt üretilmesini mümkün kılan indeks durumu.

**Kararlı sıralama (stable sort)**
:   Eşit anahtarlı öğelerin göreli sırasını koruyan sıralama; pencere
    fonksiyonlarında doğru sonuç için gereklidir.

**Kardinalite tahmini**
:   Bir koşulun kaç belgeyle eşleşeceğinin önceden kestirilmesi; sorgu
    eniyileyicinin indeks ve plan seçiminin temelidir.

**Kilitlenme (deadlock)**
:   İki işlemin her birinin, diğerinin tuttuğu kaynağı beklemesiyle oluşan,
    çözülmediğinde sonsuza dek süren döngüsel bekleme.

**Kirli sayfa tablosu (dirty page table)**
:   ARIES kurtarmasında, hangi değişikliklerin henüz diske yansımadığını
    izleyen ve yinelemenin nereden başlayacağını belirleyen yapı.

**Koleksiyon**
:   Belgelerin gruplandığı birim; ilişkisel tablonun karşılığıdır ama
    belgelerin aynı biçimde olmasını zorunlu kılmaz.

**Konsensüs**
:   Bir grup makinenin, bazıları çökse bile ortak bir karar üzerinde güvenle
    anlaşmasını sağlayan, çoğunluk oylamasına dayanan mekanizma.

**Kurtarma (recovery)**
:   Çökme sonrası, yazma-öncesi günlüğü oynatarak veritabanını tutarlı bir
    duruma getiren süreç.

**LSM ağacı**
:   Yazmaları bellekte biriktirip sıralı parçalar halinde diske yazan ve
    bunları arka planda birleştiren, yazma-yoğun log-yapılı depolama
    tasarımı.

**LSN (günlük sıra numarası)**
:   Her WAL kaydına verilen artan kimlik; sayfa-LSN'iyle kıyaslanarak bir
    değişikliğin uygulanıp uygulanmadığı anlaşılır (idempotent yineleme).

**Maliyet modeli (cost model)**
:   Sorgu eniyileyicinin, alternatif planların disk ve CPU maliyetini tahmin
    edip en ucuzunu seçmesini sağlayan model.

**memtable**
:   LSM'de yazmaların önce biriktiği bellekteki sıralı yapı; dolunca diske
    bir SSTable olarak boşaltılır.

**mmap (belleğe yansıtma)**
:   Bir dosyayı belleğin bir parçasıymış gibi erişilebilir kılan ve önbellek
    yönetimini büyük ölçüde işletim sistemine devreden teknik.

**MVCC**
:   Verinin birden çok sürümünü tutarak okuyucuların tutarlı bir anlık
    görüntü görmesini, okuyucu ile yazıcının birbirini beklememesini
    sağlayan yaklaşım.

**Nihai tutarlılık**
:   Kopyaların geçici olarak anlaşmazlığa düşmesine izin veren ama yazmalar
    durursa aynı değere yakınsamayı garanti eden tutarlılık biçimi.

**Normalleştirme**
:   Her bilgiyi tek bir yerde tutarak çoğaltmayı önleme; ilişkisel modelin
    güncellemeyi ucuzlatan ilkesi.

**Okuma anlık görüntüsü (read snapshot)**
:   OxiDB'nin, yalnızca okuma yolunu değiştiren hafif çok-sürümlü (MVCC)
    yaklaşımı; bir okuma, başladığı andaki tutarlı durumu görür ve süregelen
    yazmalardan etkilenmez. Toplama varsayılan olarak bu sayede anlık-görüntü
    tutarlıdır; okuyucular yazarları, yazarlar okuyucuları bekletmez.

**OxiPool**
:   OxiDB'nin sharding (parçalama) katmanı; bir koleksiyonu parça anahtarına
    göre birden çok bağımsız OxiDB düğümüne dağıtan ve sorguları dağıt-topla
    yöntemiyle yanıtlayan ön yüz.

**OxiWire**
:   OxiDB'nin sunucu iletişiminde kullandığı, uzunluk önekli, JSON ve daha
    hızlı bir ikili biçimi destekleyen tel protokolü.

**Ölü alan**
:   Append-only depolamada, güncellenen ya da silinen kayıtların geride
    bıraktığı, artık geçerli olmayan veri; sıkıştırmayla geri kazanılır.

**Önce-olur ilişkisi (happened-before)**
:   Dağıtık olaylar arasında nedensel sıralamayı tanımlayan; mantıksal ve
    vektör saatlerinin temelindeki bağıntı.

**Önek kuralı**
:   Bir bileşik indeksin, ancak alan sırasının baştan başlayan bir önekini
    kullanan sorgulara yaradığı kural.

**Paxos**
:   Çoğunluk anlaşmasıyla konsensüs sağlayan, Raft'tan önceki temel
    protokol; iki aşamalı (söz iste, sonra öner) bir yapıya dayanır.

**Pencere fonksiyonu**
:   Her belgeyi koruyarak, ona komşu belgelerden oluşan bir pencereye dayalı
    bir değer ekleyen analitik işlem; kümülatif toplam, hareketli ortalama,
    sıralama gibi.

**PITR (zaman-noktasına kurtarma)**
:   Veritabanını yalnızca son tutarlı duruma değil, geçmişteki belirli bir
    ana geri döndürebilme yeteneği.

**Pipeline**
:   Toplamanın, her biri akışı dönüştürüp bir sonrakine veren aşamalardan
    oluşan, bileşilebilir modeli.

**RBAC (rol tabanlı erişim denetimi)**
:   Yetkileri rollerde gruplayıp kullanıcılara rol atayarak yetkilendirmeyi
    yöneten model.

**Replikasyon**
:   Aynı veriyi birden çok makinede kopya halinde tutma; dayanıklılık,
    erişilebilirlik, okuma ölçeği ve gecikme için yapılır.

**Sağlama toplamı (checksum)**
:   Bir kaydın içeriğinden hesaplanan, yarım kalmış ya da bozulmuş kayıtları
    yakalamaya yarayan doğrulama değeri.

**Sayfa**
:   Diskten tek seferde okunup yazılan, sabit boyutlu blok; sayfa tabanlı
    depolama motorlarının temel birimi.

**Sayfa hatası (page fault)**
:   Belleğe yansıtılmış ama o an fiziksel bellekte bulunmayan bir sayfaya
    erişildiğinde, işletim sisteminin sayfayı diskten getirmesi.

**SCRAM**
:   Parolayı hattan hiç geçirmeden, çentik (nonce) ve tuzlu özet temelli bir
    meydan-okuma-yanıt ile kimlik doğrulayan mekanizma (OxiDB'de SCRAM-
    SHA-256).

**Seçicilik**
:   Bir indeks alanının değerlerinin ne kadar az belgeyle eşleştiği; yüksek
    seçicilik, indeksi daha yararlı kılar.

**Serileştirilebilir anlık görüntü yalıtımı (SSI)**
:   Anlık görüntü yalıtımına, tehlikeli okuma-yazma bağımlılıklarını
    saptayıp işlemi iptal eden bir denetim ekleyerek tam
    serileştirilebilirliği sağlayan yöntem.

**Seviyeli sıkıştırma (leveled)**
:   LSM'de parçaları örtüşmeyen seviyelere ayıran; okuma ve yer büyütmesini
    düşük tutan, ama daha çok yazma büyütmesi getiren birleştirme
    stratejisi.

**Sharding (parçalama)**
:   Veri kümesini makineler arasında bölerek kapasiteyi ve yazma hacmini
    ölçeklendirme tekniği.

**Sıfır-kopya**
:   Sıkıştırılmamış ve şifrelenmemiş veriye, belleğe yansıtılmış dosyadan
    hiç kopyalamadan ve çözmeden doğrudan erişebilme.

**Sıkıştırma (compaction)**
:   Append-only bir veri dosyasını baştan yazıp yalnızca yaşayan kayıtları
    tutarak ölü alanı geri kazanan bakım işlemi.

**Split-brain (ikiye bölünmüş beyin)**
:   Bir ağ bölünmesinde, aynı anda iki liderin ortaya çıkıp veriyi çelişkili
    iki gerçeğe ayırması tehlikesi; çoğunluk mutabakatı bunu önler.

**SSTable**
:   LSM'de diske yazılan, sıralı ve değişmez veri parçası (sorted string
    table).

**Sürüm numarası**
:   Her belgeye iliştirilen, her değişiklikte artan sayaç; iyimser
    eşzamanlılık denetiminde çatışmayı saptamaya yarar.

**Sürüm zinciri (version chain)**
:   MVCC'de bir belgenin ardışık sürümlerinin, okuyucuların doğru anlık
    görüntüyü bulabilmesi için birbirine bağlandığı yapı.

**Şema-okumada / şema-yazmada**
:   Şemanın okuma anında (uygulamada, örtük) mı yoksa yazma anında
    (veritabanında, açık) mı dayatıldığını ayıran kavramlar.

**Tahliye (eviction)**
:   Dolu bir önbellekte, yeni bir şeye yer açmak için bir öğenin atılması;
    yaygın bir politika "en uzun süredir kullanılmayanı at" (LRU) ilkesidir.

**Tam tarama (full scan)**
:   Aranan kaydı bulmak için tüm belgeleri tek tek okuma; uygun bir indeks
    yoksa başvurulan, maliyetli yol.

**Tampon havuzu (buffer pool)**
:   Veritabanının disk sayfalarını bellekte önbelleklediği; kirli sayfaları
    WAL kuralına göre diske yazan yönetilen havuz.

**Ters indeks (inverted index)**
:   "Sözcük, o sözcüğü içeren belgeler" eşlemesi; metin içinde sözcük
    aramayı ve alaka puanlamasını mümkün kılar.

**Tutarlı hash (consistent hashing)**
:   Düğüm eklenip çıktığında anahtarların yalnızca küçük bir kısmının yer
    değiştirmesini sağlayan, bir halka üzerine yerleştirmeye dayanan dağıtım
    yöntemi.

**Tutarlılık (tek makine / dağıtık)**
:   Tek makinede, verinin geçerli kurallara uyması; dağıtık sistemde,
    kopyaların ne ölçüde aynı değeri gösterdiği (güçlüden nihaiye uzanan bir
    tayf).

**Vektör saatleri (vector clocks)**
:   Dağıtık olaylar arasındaki neden-sonuç ilişkisini ve eşzamanlılığı
    saptayan, düğüm başına bir sayaç dizisi.

**Veri modeli**
:   Verinin nasıl yapılandırıldığını, ilişkilerin nasıl ifade edildiğini ve
    hangi işlemlerle erişildiğini belirleyen temel karar.

**WAL (yazma-öncesi günlük)**
:   Asıl veriyi değiştirmeden önce niyeti kaydedip dayanıklı kılan,
    dayanıklılık ve çökme güvenliğinin temelindeki günlük.

**Yalıtım (isolation)**
:   Aynı anda çalışan işlemlerin birbirini etkilememesi; sonucun, işlemler
    sırayla çalışmış gibi olması güvencesi.

**Yarı-eşzamanlı replikasyon**
:   Liderin, bir yazmayı en az bir takipçi onaylayana dek beklediği;
    eşzamansızın kayıp riskiyle eşzamanlının yavaşlığını dengeleyen orta
    yol.

**Yazma eğriltme (write skew)**
:   İki işlemin ayrı ayrı geçerli ama birlikte bir değişmezi bozan kararlar
    aldığı, anlık görüntü yalıtımında görülebilen anormallik.

**Yerellik**
:   İlişkili veriyi bir arada tutarak tek bir okumayla almayı sağlama; belge
    modelinin gömme yoluyla sunduğu kazanç.

**Yetersayı (W + R > N)**
:   Lidersiz replikasyonda, yazma (W) ve okuma (R) kopya sayılarının toplamı
    kopya sayısını (N) aşacak biçimde seçilerek okumanın en güncel yazmayı
    görmesinin sağlanması.

**Yetkilendirme (authorization)**
:   Kimliği bilinen bir kullanıcının hangi işlemleri yapabileceğine karar
    verme.

**Yırtık yazma (torn write)**
:   Bir sayfa diske yazılırken sistemin çökmesiyle sayfanın yarısının eski,
    yarısının yeni kalması; sağlama toplamı ve append-only tasarımla ele
    alınır.

**Yineleyici modeli (Volcano)**
:   Her plan düğümünün bir sonraki belgeyi "iste" çağrısıyla ürettiği,
    talep-güdümlü sorgu yürütme modeli; erken sonlanmayı doğal kılar.
