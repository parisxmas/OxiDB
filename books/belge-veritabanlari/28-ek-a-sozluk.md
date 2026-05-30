# Ek A — Sözlük {-}

Bu sözlük, kitap boyunca kullanılan başlıca terimleri kısaca tanımlar.
Tanımlar, terimin ilk geçtiği bölümdeki ayrıntılı anlatımın yerini tutmaz;
hızlı bir hatırlatma olarak düşünülmüştür.

**ACID**
:   Bir işlemin verdiği dört güvencenin kısaltması: atomiklik, tutarlılık,
    yalıtım ve dayanıklılık.

**Açma (unwind)**
:   Bir toplama aşaması; içinde liste barındıran bir belgeyi, listenin her öğesi
    için bir tane olmak üzere birçok belgeye genişletir.

**Anahtar-değer modeli**
:   Veriyi bir anahtardan opak bir değere giden eşleme olarak tutan, en yalın
    veri modeli.

**Append-only (yalnızca-ekleme)**
:   Var olan veriyi yerinde değiştirmeyen, her yazmayı dosyanın sonuna ekleyen
    depolama yaklaşımı; ardışık yazma hızlıdır, ölü alan biriktirir.

**Atomiklik**
:   Bir işlemin tüm adımlarının ya hep birlikte gerçekleşmesi ya da hiç
    gerçekleşmemesi güvencesi.

**Ayrıştırma (parsing)**
:   Ham bir sorguyu, sistemin üzerinde işlem yapabileceği yapısal bir koşul
    ağacına çevirme adımı.

**B-ağacı**
:   Veriyi sıralı ve dengeli tutan, hem tekil hem aralık aramalarında verimli bir
    ağaç yapısı; sayfa tabanlı depolama motorlarının ve sıralı indekslerin
    temelidir.

**Bayt düzeyinde süzme**
:   OxiDB'nin, bir belgeyi koşula uyup uymadığını anlamak için onu nesneye
    çevirmeden, kodlanmış baytları üzerinde denetleyen ve eşleşmeyenleri hiç
    çözmeden eleyen tekniği.

**Belge (doküman)**
:   Alanlardan oluşan, değerleri skaler, liste ya da iç içe belge olabilen,
    kendini tanımlayan veri birimi.

**Bileşik indeks**
:   Birden çok alanın birleşimi üzerine kurulu indeks; alan sırasının baştan
    başlayan bir önekini kullanan sorgulara yarar (önek kuralı).

**Biriktirici (accumulator)**
:   Gruplama sırasında her grup için bir özet değer hesaplayan işlem; sayma,
    toplama, ortalama, en büyük, en küçük gibi.

**CAP**
:   Ağ bölündüğünde, bir dağıtık sistemin tutarlılık ile erişilebilirlik
    arasında seçim yapmak zorunda olduğunu ifade eden içgörü.

**Çalışma kümesi (working set)**
:   Verinin, herhangi bir anda etkin biçimde kullanılan sıcak kısmı; belleğe
    sığıp sığmaması performansı belirler.

**Çoğunluk (quorum)**
:   Bir kararın geçerli sayılması için gereken düğüm çoğunluğu; herhangi iki
    çoğunluğun kesişmesi, konsensüsün güvenliğini sağlar.

**Dağıt-topla (scatter-gather)**
:   Sharding'de, parça anahtarı içermeyen bir isteğin tüm parçalara gönderilip
    kısmi yanıtların birleştirilmesi örüntüsü.

**Dayanıklılık (durability)**
:   Bir işlem tamamlandı dendikten sonra, hiçbir çökmenin onu geri alamaması
    güvencesi.

**Denetim (audit)**
:   "Kim, ne zaman, ne yaptı" sorusunu yanıtlayan, hesap verebilirlik için
    tutulan kayıt.

**Denetim noktası (checkpoint)**
:   Yazma-öncesi günlüğün, değişiklikler asıl depoya güvenle yansıdıktan sonra
    kısaltıldığı senkronizasyon anı.

**Disk-öncelikli (disk-first)**
:   OxiDB'nin, bellekte yalnızca kompakt bir kimlik-konum dizini tutup belge
    gövdelerini diske bırakan, bellek-tutumlu depolama kipi.

**Erken sonlanma**
:   Bir sorgunun, gereken sonuca ulaştığı an durup geri kalanı üretmemesi;
    sıralı indeksli "ilk N" sorgularında ve tekil işlemlerde kullanılır.

**Eşzamansız replikasyon**
:   Liderin, bir yazmayı takipçilere iletmeyi beklemeden onayladığı, hızlı ama
    kayıp riski taşıyan replikasyon biçimi.

**Failover (devralma)**
:   Lider çöktüğünde, bir takipçinin yeni lider olarak yükseltilmesi süreci.

**FFI**
:   Bir dilde yazılmış çekirdeği, C uyumlu bir arayüz üzerinden başka dillerden
    çağrılabilir kılan köprü.

**fsync**
:   Yazılmış verinin gerçekten kalıcı ortama işlendiğinden emin olmak için
    verilen, güçlü ama yavaş boşaltma emri.

**Gömme (embedding)**
:   İlişkili veriyi, ona ait olduğu belgenin içine yerleştirme; yerellik
    kazandırır, çoğaltma ve sınırsız büyüme riski getirir.

**Gruplama**
:   Belgeleri bir anahtara göre gruplara ayırıp her grup için biriktiricilerle
    özet hesaplayan, satırları çökerten toplama işlemi.

**İndeks**
:   Bir alanın değerlerinden belgelerin konumuna giden, aramayı hızlandıran,
    veriden türetilmiş yardımcı yapı.

**İşlem (transaction)**
:   Birçok okuma-yazma adımını tek bir bölünmez birim olarak ele alan, "ya hep ya
    hiç" güvencesi veren kavram.

**İyimser eşzamanlılık denetimi (OCC)**
:   Çatışmaların nadir olduğunu varsayan, kilit almadan çalışıp tamamlama anında
    sürüm doğrulayan ve çatışmada iptal eden yaklaşım; OxiDB'nin kullandığı model.

**JSON**
:   JavaScript nesne gösteriminden türeyen, dilden bağımsız, metinsel veri
    değiş-tokuş biçimi; belge yapısının yazıya dökülmüş hâli.

**Kapsayan indeks (covering)**
:   Bir sorgunun ihtiyaç duyduğu her şeyi tek başına sağlayan ve böylece belgeye
    hiç dokunmadan yanıt üretilmesini mümkün kılan indeks durumu.

**Kilitlenme (deadlock)**
:   İki işlemin her birinin, diğerinin tuttuğu kaynağı beklemesiyle oluşan,
    çözülmediğinde sonsuza dek süren döngüsel bekleme.

**Konsensüs**
:   Bir grup makinenin, bazıları çökse bile ortak bir karar üzerinde güvenle
    anlaşmasını sağlayan, çoğunluk oylamasına dayanan mekanizma.

**Koleksiyon**
:   Belgelerin gruplandığı birim; ilişkisel tablonun karşılığıdır ama belgelerin
    aynı biçimde olmasını zorunlu kılmaz.

**Kurtarma (recovery)**
:   Çökme sonrası, yazma-öncesi günlüğü oynatarak veritabanını tutarlı bir
    duruma getiren süreç.

**LSM ağacı**
:   Yazmaları bellekte biriktirip sıralı parçalar halinde diske yazan ve bunları
    arka planda birleştiren, yazma-yoğun log-yapılı depolama tasarımı.

**mmap (belleğe yansıtma)**
:   Bir dosyayı belleğin bir parçasıymış gibi erişilebilir kılan ve önbellek
    yönetimini büyük ölçüde işletim sistemine devreden teknik.

**MVCC**
:   Verinin birden çok sürümünü tutarak okuyucuların tutarlı bir anlık görüntü
    görmesini, okuyucu ile yazıcının birbirini beklememesini sağlayan yaklaşım.

**Nihai tutarlılık**
:   Kopyaların geçici olarak anlaşmazlığa düşmesine izin veren ama yazmalar
    durursa aynı değere yakınsamayı garanti eden tutarlılık biçimi.

**Normalleştirme**
:   Her bilgiyi tek bir yerde tutarak çoğaltmayı önleme; ilişkisel modelin
    güncellemeyi ucuzlatan ilkesi.

**OxiPool**
:   OxiDB'nin sharding (parçalama) katmanı; bir koleksiyonu parça anahtarına göre
    birden çok bağımsız OxiDB düğümüne dağıtan ve sorguları dağıt-topla yöntemiyle
    yanıtlayan ön yüz.

**OxiWire**
:   OxiDB'nin sunucu iletişiminde kullandığı, uzunluk önekli, JSON ve daha hızlı
    bir ikili biçimi destekleyen tel protokolü.

**Ölü alan**
:   Append-only depolamada, güncellenen ya da silinen kayıtların geride bıraktığı,
    artık geçerli olmayan veri; sıkıştırmayla geri kazanılır.

**Önek kuralı**
:   Bir bileşik indeksin, ancak alan sırasının baştan başlayan bir önekini
    kullanan sorgulara yaradığı kural.

**Pencere fonksiyonu**
:   Her belgeyi koruyarak, ona komşu belgelerden oluşan bir pencereye dayalı bir
    değer ekleyen analitik işlem; kümülatif toplam, hareketli ortalama, sıralama
    gibi.

**Pipeline**
:   Toplamanın, her biri akışı dönüştürüp bir sonrakine veren aşamalardan oluşan,
    bileşilebilir modeli.

**PITR (zaman-noktasına kurtarma)**
:   Veritabanını yalnızca son tutarlı duruma değil, geçmişteki belirli bir ana
    geri döndürebilme yeteneği.

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

**Seçicilik**
:   Bir indeks alanının değerlerinin ne kadar az belgeyle eşleştiği; yüksek
    seçicilik, indeksi daha yararlı kılar.

**Sharding (parçalama)**
:   Veri kümesini makineler arasında bölerek kapasiteyi ve yazma hacmini
    ölçeklendirme tekniği.

**Sıfır-kopya**
:   Sıkıştırılmamış ve şifrelenmemiş veriye, belleğe yansıtılmış dosyadan hiç
    kopyalamadan ve çözmeden doğrudan erişebilme.

**Sıkıştırma (compaction)**
:   Append-only bir veri dosyasını baştan yazıp yalnızca yaşayan kayıtları
    tutarak ölü alanı geri kazanan bakım işlemi.

**Split-brain (ikiye bölünmüş beyin)**
:   Bir ağ bölünmesinde, aynı anda iki liderin ortaya çıkıp veriyi çelişkili iki
    gerçeğe ayırması tehlikesi; çoğunluk mutabakatı bunu önler.

**Sürüm numarası**
:   Her belgeye iliştirilen, her değişiklikte artan sayaç; iyimser eşzamanlılık
    denetiminde çatışmayı saptamaya yarar.

**Şema-okumada / şema-yazmada**
:   Şemanın okuma anında (uygulamada, örtük) mı yoksa yazma anında (veritabanında,
    açık) mı dayatıldığını ayıran kavramlar.

**Tahliye (eviction)**
:   Dolu bir önbellekte, yeni bir şeye yer açmak için bir öğenin atılması;
    yaygın bir politika "en uzun süredir kullanılmayanı at" (LRU) ilkesidir.

**Tam tarama (full scan)**
:   Aranan kaydı bulmak için tüm belgeleri tek tek okuma; uygun bir indeks
    yoksa başvurulan, maliyetli yol.

**Ters indeks (inverted index)**
:   "Sözcük, o sözcüğü içeren belgeler" eşlemesi; metin içinde sözcük aramayı ve
    alaka puanlamasını mümkün kılar.

**Tutarlılık (tek makine / dağıtık)**
:   Tek makinede, verinin geçerli kurallara uyması; dağıtık sistemde,
    kopyaların ne ölçüde aynı değeri gösterdiği (güçlüden nihaiye uzanan bir tayf).

**Veri modeli**
:   Verinin nasıl yapılandırıldığını, ilişkilerin nasıl ifade edildiğini ve hangi
    işlemlerle erişildiğini belirleyen temel karar.

**WAL (yazma-öncesi günlük)**
:   Asıl veriyi değiştirmeden önce niyeti kaydedip dayanıklı kılan, dayanıklılık
    ve çökme güvenliğinin temelindeki günlük.

**Yalıtım (isolation)**
:   Aynı anda çalışan işlemlerin birbirini etkilememesi; sonucun, işlemler sırayla
    çalışmış gibi olması güvencesi.

**Yerellik**
:   İlişkili veriyi bir arada tutarak tek bir okumayla almayı sağlama; belge
    modelinin gömme yoluyla sunduğu kazanç.

**Yetkilendirme (authorization)**
:   Kimliği bilinen bir kullanıcının hangi işlemleri yapabileceğine karar verme.
