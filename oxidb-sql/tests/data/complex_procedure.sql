-- =====================================================================
-- donem_kapanisi — period settlement for an e-commerce schema.
-- A deliberately large, join- and math-heavy stored procedure used as a
-- stress test for the SQL engine's CREATE PROCEDURE / CALL support
-- (oxidb-sql/tests/t_procedures_stress.rs creates and runs it).
--
-- Tables: musteri(id, ad, segment, puan, bakiye)
--         urun(id, ad, kategori, fiyat, maliyet, stok)
--         siparis(id, musteri_id, urun_id, adet, brut, net, durum)
--         denetim(id, donem, olay, deger)   rapor(id, donem, kategori,
--         ciro, kar, siparis_sayisi)
-- =====================================================================
CREATE PROCEDURE donem_kapanisi(
  p_donem INT,          -- settlement period
  p_kdv DOUBLE,         -- VAT rate, e.g. 0.20
  p_sadakat DOUBLE,     -- loyalty multiplier
  p_esik DOUBLE,        -- balance threshold for penalties
  p_kargo DOUBLE,       -- shipping base fee
  p_ceza DOUBLE         -- penalty rate on negative balances
) AS BEGIN

  -- A) Opening audit snapshot: state before settlement, via joins.
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'acik_siparis',
          (SELECT COUNT(*) FROM siparis s JOIN musteri m ON s.musteri_id = m.id WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'acik_brut',
          (SELECT COALESCE(SUM(s.brut), 0) FROM siparis s JOIN musteri m ON s.musteri_id = m.id WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'acik_adet',
          (SELECT COALESCE(SUM(s.adet), 0) FROM siparis s JOIN urun u ON s.urun_id = u.id WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'stok_toplam',
          (SELECT COALESCE(SUM(u.stok), 0) FROM urun u));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'stok_deger',
          (SELECT COALESCE(SUM(u.stok * u.maliyet), 0) FROM urun u));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'musteri_bakiye',
          (SELECT COALESCE(SUM(m.bakiye), 0) FROM musteri m));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'ort_sepet',
          (SELECT COALESCE(AVG(s.brut), 0) FROM siparis s WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'en_buyuk_sepet',
          (SELECT COALESCE(MAX(s.brut), 0) FROM siparis s WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kar_potansiyeli',
          (SELECT COALESCE(SUM(s.brut - u.maliyet * s.adet), 0) FROM siparis s JOIN urun u ON s.urun_id = u.id WHERE s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'vip_ciro',
          (SELECT COALESCE(SUM(s.brut), 0) FROM siparis s JOIN musteri m ON s.musteri_id = m.id WHERE s.durum = 'acik' AND m.segment = 'vip'));

  -- B) Apply VAT to every open order's running net amount.
  UPDATE siparis
  SET net = brut * (1 + p_kdv)
  WHERE durum = 'acik';

  -- C) Shipping: a fee ladder over the gross amount — bigger baskets get
  --    progressively cheaper (per-100 bracket) shipping, floored by the
  --    base fee; tiny baskets pay double base.
  UPDATE siparis
  SET net = net + CASE
    WHEN brut >= 15000 THEN p_kargo * (1.0 + 0.0 / 150.0) - p_kargo * 150.0 / 160.0
    WHEN brut >= 14900 THEN p_kargo * (1.0 + 1.0 / 150.0) - p_kargo * 149.0 / 160.0
    WHEN brut >= 14800 THEN p_kargo * (1.0 + 2.0 / 150.0) - p_kargo * 148.0 / 160.0
    WHEN brut >= 14700 THEN p_kargo * (1.0 + 3.0 / 150.0) - p_kargo * 147.0 / 160.0
    WHEN brut >= 14600 THEN p_kargo * (1.0 + 4.0 / 150.0) - p_kargo * 146.0 / 160.0
    WHEN brut >= 14500 THEN p_kargo * (1.0 + 5.0 / 150.0) - p_kargo * 145.0 / 160.0
    WHEN brut >= 14400 THEN p_kargo * (1.0 + 6.0 / 150.0) - p_kargo * 144.0 / 160.0
    WHEN brut >= 14300 THEN p_kargo * (1.0 + 7.0 / 150.0) - p_kargo * 143.0 / 160.0
    WHEN brut >= 14200 THEN p_kargo * (1.0 + 8.0 / 150.0) - p_kargo * 142.0 / 160.0
    WHEN brut >= 14100 THEN p_kargo * (1.0 + 9.0 / 150.0) - p_kargo * 141.0 / 160.0
    WHEN brut >= 14000 THEN p_kargo * (1.0 + 10.0 / 150.0) - p_kargo * 140.0 / 160.0
    WHEN brut >= 13900 THEN p_kargo * (1.0 + 11.0 / 150.0) - p_kargo * 139.0 / 160.0
    WHEN brut >= 13800 THEN p_kargo * (1.0 + 12.0 / 150.0) - p_kargo * 138.0 / 160.0
    WHEN brut >= 13700 THEN p_kargo * (1.0 + 13.0 / 150.0) - p_kargo * 137.0 / 160.0
    WHEN brut >= 13600 THEN p_kargo * (1.0 + 14.0 / 150.0) - p_kargo * 136.0 / 160.0
    WHEN brut >= 13500 THEN p_kargo * (1.0 + 15.0 / 150.0) - p_kargo * 135.0 / 160.0
    WHEN brut >= 13400 THEN p_kargo * (1.0 + 16.0 / 150.0) - p_kargo * 134.0 / 160.0
    WHEN brut >= 13300 THEN p_kargo * (1.0 + 17.0 / 150.0) - p_kargo * 133.0 / 160.0
    WHEN brut >= 13200 THEN p_kargo * (1.0 + 18.0 / 150.0) - p_kargo * 132.0 / 160.0
    WHEN brut >= 13100 THEN p_kargo * (1.0 + 19.0 / 150.0) - p_kargo * 131.0 / 160.0
    WHEN brut >= 13000 THEN p_kargo * (1.0 + 20.0 / 150.0) - p_kargo * 130.0 / 160.0
    WHEN brut >= 12900 THEN p_kargo * (1.0 + 21.0 / 150.0) - p_kargo * 129.0 / 160.0
    WHEN brut >= 12800 THEN p_kargo * (1.0 + 22.0 / 150.0) - p_kargo * 128.0 / 160.0
    WHEN brut >= 12700 THEN p_kargo * (1.0 + 23.0 / 150.0) - p_kargo * 127.0 / 160.0
    WHEN brut >= 12600 THEN p_kargo * (1.0 + 24.0 / 150.0) - p_kargo * 126.0 / 160.0
    WHEN brut >= 12500 THEN p_kargo * (1.0 + 25.0 / 150.0) - p_kargo * 125.0 / 160.0
    WHEN brut >= 12400 THEN p_kargo * (1.0 + 26.0 / 150.0) - p_kargo * 124.0 / 160.0
    WHEN brut >= 12300 THEN p_kargo * (1.0 + 27.0 / 150.0) - p_kargo * 123.0 / 160.0
    WHEN brut >= 12200 THEN p_kargo * (1.0 + 28.0 / 150.0) - p_kargo * 122.0 / 160.0
    WHEN brut >= 12100 THEN p_kargo * (1.0 + 29.0 / 150.0) - p_kargo * 121.0 / 160.0
    WHEN brut >= 12000 THEN p_kargo * (1.0 + 30.0 / 150.0) - p_kargo * 120.0 / 160.0
    WHEN brut >= 11900 THEN p_kargo * (1.0 + 31.0 / 150.0) - p_kargo * 119.0 / 160.0
    WHEN brut >= 11800 THEN p_kargo * (1.0 + 32.0 / 150.0) - p_kargo * 118.0 / 160.0
    WHEN brut >= 11700 THEN p_kargo * (1.0 + 33.0 / 150.0) - p_kargo * 117.0 / 160.0
    WHEN brut >= 11600 THEN p_kargo * (1.0 + 34.0 / 150.0) - p_kargo * 116.0 / 160.0
    WHEN brut >= 11500 THEN p_kargo * (1.0 + 35.0 / 150.0) - p_kargo * 115.0 / 160.0
    WHEN brut >= 11400 THEN p_kargo * (1.0 + 36.0 / 150.0) - p_kargo * 114.0 / 160.0
    WHEN brut >= 11300 THEN p_kargo * (1.0 + 37.0 / 150.0) - p_kargo * 113.0 / 160.0
    WHEN brut >= 11200 THEN p_kargo * (1.0 + 38.0 / 150.0) - p_kargo * 112.0 / 160.0
    WHEN brut >= 11100 THEN p_kargo * (1.0 + 39.0 / 150.0) - p_kargo * 111.0 / 160.0
    WHEN brut >= 11000 THEN p_kargo * (1.0 + 40.0 / 150.0) - p_kargo * 110.0 / 160.0
    WHEN brut >= 10900 THEN p_kargo * (1.0 + 41.0 / 150.0) - p_kargo * 109.0 / 160.0
    WHEN brut >= 10800 THEN p_kargo * (1.0 + 42.0 / 150.0) - p_kargo * 108.0 / 160.0
    WHEN brut >= 10700 THEN p_kargo * (1.0 + 43.0 / 150.0) - p_kargo * 107.0 / 160.0
    WHEN brut >= 10600 THEN p_kargo * (1.0 + 44.0 / 150.0) - p_kargo * 106.0 / 160.0
    WHEN brut >= 10500 THEN p_kargo * (1.0 + 45.0 / 150.0) - p_kargo * 105.0 / 160.0
    WHEN brut >= 10400 THEN p_kargo * (1.0 + 46.0 / 150.0) - p_kargo * 104.0 / 160.0
    WHEN brut >= 10300 THEN p_kargo * (1.0 + 47.0 / 150.0) - p_kargo * 103.0 / 160.0
    WHEN brut >= 10200 THEN p_kargo * (1.0 + 48.0 / 150.0) - p_kargo * 102.0 / 160.0
    WHEN brut >= 10100 THEN p_kargo * (1.0 + 49.0 / 150.0) - p_kargo * 101.0 / 160.0
    WHEN brut >= 10000 THEN p_kargo * (1.0 + 50.0 / 150.0) - p_kargo * 100.0 / 160.0
    WHEN brut >= 9900 THEN p_kargo * (1.0 + 51.0 / 150.0) - p_kargo * 99.0 / 160.0
    WHEN brut >= 9800 THEN p_kargo * (1.0 + 52.0 / 150.0) - p_kargo * 98.0 / 160.0
    WHEN brut >= 9700 THEN p_kargo * (1.0 + 53.0 / 150.0) - p_kargo * 97.0 / 160.0
    WHEN brut >= 9600 THEN p_kargo * (1.0 + 54.0 / 150.0) - p_kargo * 96.0 / 160.0
    WHEN brut >= 9500 THEN p_kargo * (1.0 + 55.0 / 150.0) - p_kargo * 95.0 / 160.0
    WHEN brut >= 9400 THEN p_kargo * (1.0 + 56.0 / 150.0) - p_kargo * 94.0 / 160.0
    WHEN brut >= 9300 THEN p_kargo * (1.0 + 57.0 / 150.0) - p_kargo * 93.0 / 160.0
    WHEN brut >= 9200 THEN p_kargo * (1.0 + 58.0 / 150.0) - p_kargo * 92.0 / 160.0
    WHEN brut >= 9100 THEN p_kargo * (1.0 + 59.0 / 150.0) - p_kargo * 91.0 / 160.0
    WHEN brut >= 9000 THEN p_kargo * (1.0 + 60.0 / 150.0) - p_kargo * 90.0 / 160.0
    WHEN brut >= 8900 THEN p_kargo * (1.0 + 61.0 / 150.0) - p_kargo * 89.0 / 160.0
    WHEN brut >= 8800 THEN p_kargo * (1.0 + 62.0 / 150.0) - p_kargo * 88.0 / 160.0
    WHEN brut >= 8700 THEN p_kargo * (1.0 + 63.0 / 150.0) - p_kargo * 87.0 / 160.0
    WHEN brut >= 8600 THEN p_kargo * (1.0 + 64.0 / 150.0) - p_kargo * 86.0 / 160.0
    WHEN brut >= 8500 THEN p_kargo * (1.0 + 65.0 / 150.0) - p_kargo * 85.0 / 160.0
    WHEN brut >= 8400 THEN p_kargo * (1.0 + 66.0 / 150.0) - p_kargo * 84.0 / 160.0
    WHEN brut >= 8300 THEN p_kargo * (1.0 + 67.0 / 150.0) - p_kargo * 83.0 / 160.0
    WHEN brut >= 8200 THEN p_kargo * (1.0 + 68.0 / 150.0) - p_kargo * 82.0 / 160.0
    WHEN brut >= 8100 THEN p_kargo * (1.0 + 69.0 / 150.0) - p_kargo * 81.0 / 160.0
    WHEN brut >= 8000 THEN p_kargo * (1.0 + 70.0 / 150.0) - p_kargo * 80.0 / 160.0
    WHEN brut >= 7900 THEN p_kargo * (1.0 + 71.0 / 150.0) - p_kargo * 79.0 / 160.0
    WHEN brut >= 7800 THEN p_kargo * (1.0 + 72.0 / 150.0) - p_kargo * 78.0 / 160.0
    WHEN brut >= 7700 THEN p_kargo * (1.0 + 73.0 / 150.0) - p_kargo * 77.0 / 160.0
    WHEN brut >= 7600 THEN p_kargo * (1.0 + 74.0 / 150.0) - p_kargo * 76.0 / 160.0
    WHEN brut >= 7500 THEN p_kargo * (1.0 + 75.0 / 150.0) - p_kargo * 75.0 / 160.0
    WHEN brut >= 7400 THEN p_kargo * (1.0 + 76.0 / 150.0) - p_kargo * 74.0 / 160.0
    WHEN brut >= 7300 THEN p_kargo * (1.0 + 77.0 / 150.0) - p_kargo * 73.0 / 160.0
    WHEN brut >= 7200 THEN p_kargo * (1.0 + 78.0 / 150.0) - p_kargo * 72.0 / 160.0
    WHEN brut >= 7100 THEN p_kargo * (1.0 + 79.0 / 150.0) - p_kargo * 71.0 / 160.0
    WHEN brut >= 7000 THEN p_kargo * (1.0 + 80.0 / 150.0) - p_kargo * 70.0 / 160.0
    WHEN brut >= 6900 THEN p_kargo * (1.0 + 81.0 / 150.0) - p_kargo * 69.0 / 160.0
    WHEN brut >= 6800 THEN p_kargo * (1.0 + 82.0 / 150.0) - p_kargo * 68.0 / 160.0
    WHEN brut >= 6700 THEN p_kargo * (1.0 + 83.0 / 150.0) - p_kargo * 67.0 / 160.0
    WHEN brut >= 6600 THEN p_kargo * (1.0 + 84.0 / 150.0) - p_kargo * 66.0 / 160.0
    WHEN brut >= 6500 THEN p_kargo * (1.0 + 85.0 / 150.0) - p_kargo * 65.0 / 160.0
    WHEN brut >= 6400 THEN p_kargo * (1.0 + 86.0 / 150.0) - p_kargo * 64.0 / 160.0
    WHEN brut >= 6300 THEN p_kargo * (1.0 + 87.0 / 150.0) - p_kargo * 63.0 / 160.0
    WHEN brut >= 6200 THEN p_kargo * (1.0 + 88.0 / 150.0) - p_kargo * 62.0 / 160.0
    WHEN brut >= 6100 THEN p_kargo * (1.0 + 89.0 / 150.0) - p_kargo * 61.0 / 160.0
    WHEN brut >= 6000 THEN p_kargo * (1.0 + 90.0 / 150.0) - p_kargo * 60.0 / 160.0
    WHEN brut >= 5900 THEN p_kargo * (1.0 + 91.0 / 150.0) - p_kargo * 59.0 / 160.0
    WHEN brut >= 5800 THEN p_kargo * (1.0 + 92.0 / 150.0) - p_kargo * 58.0 / 160.0
    WHEN brut >= 5700 THEN p_kargo * (1.0 + 93.0 / 150.0) - p_kargo * 57.0 / 160.0
    WHEN brut >= 5600 THEN p_kargo * (1.0 + 94.0 / 150.0) - p_kargo * 56.0 / 160.0
    WHEN brut >= 5500 THEN p_kargo * (1.0 + 95.0 / 150.0) - p_kargo * 55.0 / 160.0
    WHEN brut >= 5400 THEN p_kargo * (1.0 + 96.0 / 150.0) - p_kargo * 54.0 / 160.0
    WHEN brut >= 5300 THEN p_kargo * (1.0 + 97.0 / 150.0) - p_kargo * 53.0 / 160.0
    WHEN brut >= 5200 THEN p_kargo * (1.0 + 98.0 / 150.0) - p_kargo * 52.0 / 160.0
    WHEN brut >= 5100 THEN p_kargo * (1.0 + 99.0 / 150.0) - p_kargo * 51.0 / 160.0
    WHEN brut >= 5000 THEN p_kargo * (1.0 + 100.0 / 150.0) - p_kargo * 50.0 / 160.0
    WHEN brut >= 4900 THEN p_kargo * (1.0 + 101.0 / 150.0) - p_kargo * 49.0 / 160.0
    WHEN brut >= 4800 THEN p_kargo * (1.0 + 102.0 / 150.0) - p_kargo * 48.0 / 160.0
    WHEN brut >= 4700 THEN p_kargo * (1.0 + 103.0 / 150.0) - p_kargo * 47.0 / 160.0
    WHEN brut >= 4600 THEN p_kargo * (1.0 + 104.0 / 150.0) - p_kargo * 46.0 / 160.0
    WHEN brut >= 4500 THEN p_kargo * (1.0 + 105.0 / 150.0) - p_kargo * 45.0 / 160.0
    WHEN brut >= 4400 THEN p_kargo * (1.0 + 106.0 / 150.0) - p_kargo * 44.0 / 160.0
    WHEN brut >= 4300 THEN p_kargo * (1.0 + 107.0 / 150.0) - p_kargo * 43.0 / 160.0
    WHEN brut >= 4200 THEN p_kargo * (1.0 + 108.0 / 150.0) - p_kargo * 42.0 / 160.0
    WHEN brut >= 4100 THEN p_kargo * (1.0 + 109.0 / 150.0) - p_kargo * 41.0 / 160.0
    WHEN brut >= 4000 THEN p_kargo * (1.0 + 110.0 / 150.0) - p_kargo * 40.0 / 160.0
    WHEN brut >= 3900 THEN p_kargo * (1.0 + 111.0 / 150.0) - p_kargo * 39.0 / 160.0
    WHEN brut >= 3800 THEN p_kargo * (1.0 + 112.0 / 150.0) - p_kargo * 38.0 / 160.0
    WHEN brut >= 3700 THEN p_kargo * (1.0 + 113.0 / 150.0) - p_kargo * 37.0 / 160.0
    WHEN brut >= 3600 THEN p_kargo * (1.0 + 114.0 / 150.0) - p_kargo * 36.0 / 160.0
    WHEN brut >= 3500 THEN p_kargo * (1.0 + 115.0 / 150.0) - p_kargo * 35.0 / 160.0
    WHEN brut >= 3400 THEN p_kargo * (1.0 + 116.0 / 150.0) - p_kargo * 34.0 / 160.0
    WHEN brut >= 3300 THEN p_kargo * (1.0 + 117.0 / 150.0) - p_kargo * 33.0 / 160.0
    WHEN brut >= 3200 THEN p_kargo * (1.0 + 118.0 / 150.0) - p_kargo * 32.0 / 160.0
    WHEN brut >= 3100 THEN p_kargo * (1.0 + 119.0 / 150.0) - p_kargo * 31.0 / 160.0
    WHEN brut >= 3000 THEN p_kargo * (1.0 + 120.0 / 150.0) - p_kargo * 30.0 / 160.0
    WHEN brut >= 2900 THEN p_kargo * (1.0 + 121.0 / 150.0) - p_kargo * 29.0 / 160.0
    WHEN brut >= 2800 THEN p_kargo * (1.0 + 122.0 / 150.0) - p_kargo * 28.0 / 160.0
    WHEN brut >= 2700 THEN p_kargo * (1.0 + 123.0 / 150.0) - p_kargo * 27.0 / 160.0
    WHEN brut >= 2600 THEN p_kargo * (1.0 + 124.0 / 150.0) - p_kargo * 26.0 / 160.0
    WHEN brut >= 2500 THEN p_kargo * (1.0 + 125.0 / 150.0) - p_kargo * 25.0 / 160.0
    WHEN brut >= 2400 THEN p_kargo * (1.0 + 126.0 / 150.0) - p_kargo * 24.0 / 160.0
    WHEN brut >= 2300 THEN p_kargo * (1.0 + 127.0 / 150.0) - p_kargo * 23.0 / 160.0
    WHEN brut >= 2200 THEN p_kargo * (1.0 + 128.0 / 150.0) - p_kargo * 22.0 / 160.0
    WHEN brut >= 2100 THEN p_kargo * (1.0 + 129.0 / 150.0) - p_kargo * 21.0 / 160.0
    WHEN brut >= 2000 THEN p_kargo * (1.0 + 130.0 / 150.0) - p_kargo * 20.0 / 160.0
    WHEN brut >= 1900 THEN p_kargo * (1.0 + 131.0 / 150.0) - p_kargo * 19.0 / 160.0
    WHEN brut >= 1800 THEN p_kargo * (1.0 + 132.0 / 150.0) - p_kargo * 18.0 / 160.0
    WHEN brut >= 1700 THEN p_kargo * (1.0 + 133.0 / 150.0) - p_kargo * 17.0 / 160.0
    WHEN brut >= 1600 THEN p_kargo * (1.0 + 134.0 / 150.0) - p_kargo * 16.0 / 160.0
    WHEN brut >= 1500 THEN p_kargo * (1.0 + 135.0 / 150.0) - p_kargo * 15.0 / 160.0
    WHEN brut >= 1400 THEN p_kargo * (1.0 + 136.0 / 150.0) - p_kargo * 14.0 / 160.0
    WHEN brut >= 1300 THEN p_kargo * (1.0 + 137.0 / 150.0) - p_kargo * 13.0 / 160.0
    WHEN brut >= 1200 THEN p_kargo * (1.0 + 138.0 / 150.0) - p_kargo * 12.0 / 160.0
    WHEN brut >= 1100 THEN p_kargo * (1.0 + 139.0 / 150.0) - p_kargo * 11.0 / 160.0
    WHEN brut >= 1000 THEN p_kargo * (1.0 + 140.0 / 150.0) - p_kargo * 10.0 / 160.0
    WHEN brut >= 900 THEN p_kargo * (1.0 + 141.0 / 150.0) - p_kargo * 9.0 / 160.0
    WHEN brut >= 800 THEN p_kargo * (1.0 + 142.0 / 150.0) - p_kargo * 8.0 / 160.0
    WHEN brut >= 700 THEN p_kargo * (1.0 + 143.0 / 150.0) - p_kargo * 7.0 / 160.0
    WHEN brut >= 600 THEN p_kargo * (1.0 + 144.0 / 150.0) - p_kargo * 6.0 / 160.0
    WHEN brut >= 500 THEN p_kargo * (1.0 + 145.0 / 150.0) - p_kargo * 5.0 / 160.0
    WHEN brut >= 400 THEN p_kargo * (1.0 + 146.0 / 150.0) - p_kargo * 4.0 / 160.0
    WHEN brut >= 300 THEN p_kargo * (1.0 + 147.0 / 150.0) - p_kargo * 3.0 / 160.0
    WHEN brut >= 200 THEN p_kargo * (1.0 + 148.0 / 150.0) - p_kargo * 2.0 / 160.0
    WHEN brut >= 100 THEN p_kargo * (1.0 + 149.0 / 150.0) - p_kargo * 1.0 / 160.0
    ELSE p_kargo * 2
  END
  WHERE durum = 'acik';

  -- D) Quantity discount: percentage off net by units ordered.
  UPDATE siparis
  SET net = net - net * CASE
    WHEN adet >= 90 THEN 0.30
    WHEN adet >= 89 THEN 0.30
    WHEN adet >= 88 THEN 0.30
    WHEN adet >= 87 THEN 0.30
    WHEN adet >= 86 THEN 0.29
    WHEN adet >= 85 THEN 0.29
    WHEN adet >= 84 THEN 0.29
    WHEN adet >= 83 THEN 0.28
    WHEN adet >= 82 THEN 0.28
    WHEN adet >= 81 THEN 0.28
    WHEN adet >= 80 THEN 0.27
    WHEN adet >= 79 THEN 0.27
    WHEN adet >= 78 THEN 0.27
    WHEN adet >= 77 THEN 0.26
    WHEN adet >= 76 THEN 0.26
    WHEN adet >= 75 THEN 0.26
    WHEN adet >= 74 THEN 0.25
    WHEN adet >= 73 THEN 0.25
    WHEN adet >= 72 THEN 0.25
    WHEN adet >= 71 THEN 0.24
    WHEN adet >= 70 THEN 0.24
    WHEN adet >= 69 THEN 0.24
    WHEN adet >= 68 THEN 0.23
    WHEN adet >= 67 THEN 0.23
    WHEN adet >= 66 THEN 0.23
    WHEN adet >= 65 THEN 0.22
    WHEN adet >= 64 THEN 0.22
    WHEN adet >= 63 THEN 0.22
    WHEN adet >= 62 THEN 0.21
    WHEN adet >= 61 THEN 0.21
    WHEN adet >= 60 THEN 0.21
    WHEN adet >= 59 THEN 0.20
    WHEN adet >= 58 THEN 0.20
    WHEN adet >= 57 THEN 0.20
    WHEN adet >= 56 THEN 0.19
    WHEN adet >= 55 THEN 0.19
    WHEN adet >= 54 THEN 0.19
    WHEN adet >= 53 THEN 0.18
    WHEN adet >= 52 THEN 0.18
    WHEN adet >= 51 THEN 0.18
    WHEN adet >= 50 THEN 0.17
    WHEN adet >= 49 THEN 0.17
    WHEN adet >= 48 THEN 0.17
    WHEN adet >= 47 THEN 0.16
    WHEN adet >= 46 THEN 0.16
    WHEN adet >= 45 THEN 0.16
    WHEN adet >= 44 THEN 0.15
    WHEN adet >= 43 THEN 0.15
    WHEN adet >= 42 THEN 0.15
    WHEN adet >= 41 THEN 0.14
    WHEN adet >= 40 THEN 0.14
    WHEN adet >= 39 THEN 0.14
    WHEN adet >= 38 THEN 0.13
    WHEN adet >= 37 THEN 0.13
    WHEN adet >= 36 THEN 0.13
    WHEN adet >= 35 THEN 0.12
    WHEN adet >= 34 THEN 0.12
    WHEN adet >= 33 THEN 0.12
    WHEN adet >= 32 THEN 0.11
    WHEN adet >= 31 THEN 0.11
    WHEN adet >= 30 THEN 0.11
    WHEN adet >= 29 THEN 0.10
    WHEN adet >= 28 THEN 0.10
    WHEN adet >= 27 THEN 0.10
    WHEN adet >= 26 THEN 0.09
    WHEN adet >= 25 THEN 0.09
    WHEN adet >= 24 THEN 0.09
    WHEN adet >= 23 THEN 0.08
    WHEN adet >= 22 THEN 0.08
    WHEN adet >= 21 THEN 0.08
    WHEN adet >= 20 THEN 0.07
    WHEN adet >= 19 THEN 0.07
    WHEN adet >= 18 THEN 0.07
    WHEN adet >= 17 THEN 0.06
    WHEN adet >= 16 THEN 0.06
    WHEN adet >= 15 THEN 0.06
    WHEN adet >= 14 THEN 0.05
    WHEN adet >= 13 THEN 0.05
    WHEN adet >= 12 THEN 0.05
    WHEN adet >= 11 THEN 0.04
    WHEN adet >= 10 THEN 0.04
    WHEN adet >= 9 THEN 0.04
    WHEN adet >= 8 THEN 0.03
    WHEN adet >= 7 THEN 0.03
    WHEN adet >= 6 THEN 0.03
    WHEN adet >= 5 THEN 0.02
    WHEN adet >= 4 THEN 0.02
    WHEN adet >= 3 THEN 0.02
    WHEN adet >= 2 THEN 0.01
    WHEN adet >= 1 THEN 0.01
    ELSE 0.0
  END
  WHERE durum = 'acik';

  -- E) Charge each customer the sum of their open orders' net.
  UPDATE musteri
  SET bakiye = bakiye - (SELECT COALESCE(SUM(s.net), 0)
                         FROM siparis s
                         WHERE s.musteri_id = musteri.id
                           AND s.durum = 'acik');

  -- F) Loyalty points: banded by remaining balance, scaled by the
  --    loyalty multiplier; the band math keeps every branch distinct.
  UPDATE musteri
  SET puan = puan + CAST(p_sadakat * CASE
    WHEN bakiye >= 30000 THEN 365.0 + bakiye / 30100.0
    WHEN bakiye >= 29750 THEN 362.0 + bakiye / 29850.0
    WHEN bakiye >= 29500 THEN 359.0 + bakiye / 29600.0
    WHEN bakiye >= 29250 THEN 356.0 + bakiye / 29350.0
    WHEN bakiye >= 29000 THEN 353.0 + bakiye / 29100.0
    WHEN bakiye >= 28750 THEN 350.0 + bakiye / 28850.0
    WHEN bakiye >= 28500 THEN 347.0 + bakiye / 28600.0
    WHEN bakiye >= 28250 THEN 344.0 + bakiye / 28350.0
    WHEN bakiye >= 28000 THEN 341.0 + bakiye / 28100.0
    WHEN bakiye >= 27750 THEN 338.0 + bakiye / 27850.0
    WHEN bakiye >= 27500 THEN 335.0 + bakiye / 27600.0
    WHEN bakiye >= 27250 THEN 332.0 + bakiye / 27350.0
    WHEN bakiye >= 27000 THEN 329.0 + bakiye / 27100.0
    WHEN bakiye >= 26750 THEN 326.0 + bakiye / 26850.0
    WHEN bakiye >= 26500 THEN 323.0 + bakiye / 26600.0
    WHEN bakiye >= 26250 THEN 320.0 + bakiye / 26350.0
    WHEN bakiye >= 26000 THEN 317.0 + bakiye / 26100.0
    WHEN bakiye >= 25750 THEN 314.0 + bakiye / 25850.0
    WHEN bakiye >= 25500 THEN 311.0 + bakiye / 25600.0
    WHEN bakiye >= 25250 THEN 308.0 + bakiye / 25350.0
    WHEN bakiye >= 25000 THEN 305.0 + bakiye / 25100.0
    WHEN bakiye >= 24750 THEN 302.0 + bakiye / 24850.0
    WHEN bakiye >= 24500 THEN 299.0 + bakiye / 24600.0
    WHEN bakiye >= 24250 THEN 296.0 + bakiye / 24350.0
    WHEN bakiye >= 24000 THEN 293.0 + bakiye / 24100.0
    WHEN bakiye >= 23750 THEN 290.0 + bakiye / 23850.0
    WHEN bakiye >= 23500 THEN 287.0 + bakiye / 23600.0
    WHEN bakiye >= 23250 THEN 284.0 + bakiye / 23350.0
    WHEN bakiye >= 23000 THEN 281.0 + bakiye / 23100.0
    WHEN bakiye >= 22750 THEN 278.0 + bakiye / 22850.0
    WHEN bakiye >= 22500 THEN 275.0 + bakiye / 22600.0
    WHEN bakiye >= 22250 THEN 272.0 + bakiye / 22350.0
    WHEN bakiye >= 22000 THEN 269.0 + bakiye / 22100.0
    WHEN bakiye >= 21750 THEN 266.0 + bakiye / 21850.0
    WHEN bakiye >= 21500 THEN 263.0 + bakiye / 21600.0
    WHEN bakiye >= 21250 THEN 260.0 + bakiye / 21350.0
    WHEN bakiye >= 21000 THEN 257.0 + bakiye / 21100.0
    WHEN bakiye >= 20750 THEN 254.0 + bakiye / 20850.0
    WHEN bakiye >= 20500 THEN 251.0 + bakiye / 20600.0
    WHEN bakiye >= 20250 THEN 248.0 + bakiye / 20350.0
    WHEN bakiye >= 20000 THEN 245.0 + bakiye / 20100.0
    WHEN bakiye >= 19750 THEN 242.0 + bakiye / 19850.0
    WHEN bakiye >= 19500 THEN 239.0 + bakiye / 19600.0
    WHEN bakiye >= 19250 THEN 236.0 + bakiye / 19350.0
    WHEN bakiye >= 19000 THEN 233.0 + bakiye / 19100.0
    WHEN bakiye >= 18750 THEN 230.0 + bakiye / 18850.0
    WHEN bakiye >= 18500 THEN 227.0 + bakiye / 18600.0
    WHEN bakiye >= 18250 THEN 224.0 + bakiye / 18350.0
    WHEN bakiye >= 18000 THEN 221.0 + bakiye / 18100.0
    WHEN bakiye >= 17750 THEN 218.0 + bakiye / 17850.0
    WHEN bakiye >= 17500 THEN 215.0 + bakiye / 17600.0
    WHEN bakiye >= 17250 THEN 212.0 + bakiye / 17350.0
    WHEN bakiye >= 17000 THEN 209.0 + bakiye / 17100.0
    WHEN bakiye >= 16750 THEN 206.0 + bakiye / 16850.0
    WHEN bakiye >= 16500 THEN 203.0 + bakiye / 16600.0
    WHEN bakiye >= 16250 THEN 200.0 + bakiye / 16350.0
    WHEN bakiye >= 16000 THEN 197.0 + bakiye / 16100.0
    WHEN bakiye >= 15750 THEN 194.0 + bakiye / 15850.0
    WHEN bakiye >= 15500 THEN 191.0 + bakiye / 15600.0
    WHEN bakiye >= 15250 THEN 188.0 + bakiye / 15350.0
    WHEN bakiye >= 15000 THEN 185.0 + bakiye / 15100.0
    WHEN bakiye >= 14750 THEN 182.0 + bakiye / 14850.0
    WHEN bakiye >= 14500 THEN 179.0 + bakiye / 14600.0
    WHEN bakiye >= 14250 THEN 176.0 + bakiye / 14350.0
    WHEN bakiye >= 14000 THEN 173.0 + bakiye / 14100.0
    WHEN bakiye >= 13750 THEN 170.0 + bakiye / 13850.0
    WHEN bakiye >= 13500 THEN 167.0 + bakiye / 13600.0
    WHEN bakiye >= 13250 THEN 164.0 + bakiye / 13350.0
    WHEN bakiye >= 13000 THEN 161.0 + bakiye / 13100.0
    WHEN bakiye >= 12750 THEN 158.0 + bakiye / 12850.0
    WHEN bakiye >= 12500 THEN 155.0 + bakiye / 12600.0
    WHEN bakiye >= 12250 THEN 152.0 + bakiye / 12350.0
    WHEN bakiye >= 12000 THEN 149.0 + bakiye / 12100.0
    WHEN bakiye >= 11750 THEN 146.0 + bakiye / 11850.0
    WHEN bakiye >= 11500 THEN 143.0 + bakiye / 11600.0
    WHEN bakiye >= 11250 THEN 140.0 + bakiye / 11350.0
    WHEN bakiye >= 11000 THEN 137.0 + bakiye / 11100.0
    WHEN bakiye >= 10750 THEN 134.0 + bakiye / 10850.0
    WHEN bakiye >= 10500 THEN 131.0 + bakiye / 10600.0
    WHEN bakiye >= 10250 THEN 128.0 + bakiye / 10350.0
    WHEN bakiye >= 10000 THEN 125.0 + bakiye / 10100.0
    WHEN bakiye >= 9750 THEN 122.0 + bakiye / 9850.0
    WHEN bakiye >= 9500 THEN 119.0 + bakiye / 9600.0
    WHEN bakiye >= 9250 THEN 116.0 + bakiye / 9350.0
    WHEN bakiye >= 9000 THEN 113.0 + bakiye / 9100.0
    WHEN bakiye >= 8750 THEN 110.0 + bakiye / 8850.0
    WHEN bakiye >= 8500 THEN 107.0 + bakiye / 8600.0
    WHEN bakiye >= 8250 THEN 104.0 + bakiye / 8350.0
    WHEN bakiye >= 8000 THEN 101.0 + bakiye / 8100.0
    WHEN bakiye >= 7750 THEN 98.0 + bakiye / 7850.0
    WHEN bakiye >= 7500 THEN 95.0 + bakiye / 7600.0
    WHEN bakiye >= 7250 THEN 92.0 + bakiye / 7350.0
    WHEN bakiye >= 7000 THEN 89.0 + bakiye / 7100.0
    WHEN bakiye >= 6750 THEN 86.0 + bakiye / 6850.0
    WHEN bakiye >= 6500 THEN 83.0 + bakiye / 6600.0
    WHEN bakiye >= 6250 THEN 80.0 + bakiye / 6350.0
    WHEN bakiye >= 6000 THEN 77.0 + bakiye / 6100.0
    WHEN bakiye >= 5750 THEN 74.0 + bakiye / 5850.0
    WHEN bakiye >= 5500 THEN 71.0 + bakiye / 5600.0
    WHEN bakiye >= 5250 THEN 68.0 + bakiye / 5350.0
    WHEN bakiye >= 5000 THEN 65.0 + bakiye / 5100.0
    WHEN bakiye >= 4750 THEN 62.0 + bakiye / 4850.0
    WHEN bakiye >= 4500 THEN 59.0 + bakiye / 4600.0
    WHEN bakiye >= 4250 THEN 56.0 + bakiye / 4350.0
    WHEN bakiye >= 4000 THEN 53.0 + bakiye / 4100.0
    WHEN bakiye >= 3750 THEN 50.0 + bakiye / 3850.0
    WHEN bakiye >= 3500 THEN 47.0 + bakiye / 3600.0
    WHEN bakiye >= 3250 THEN 44.0 + bakiye / 3350.0
    WHEN bakiye >= 3000 THEN 41.0 + bakiye / 3100.0
    WHEN bakiye >= 2750 THEN 38.0 + bakiye / 2850.0
    WHEN bakiye >= 2500 THEN 35.0 + bakiye / 2600.0
    WHEN bakiye >= 2250 THEN 32.0 + bakiye / 2350.0
    WHEN bakiye >= 2000 THEN 29.0 + bakiye / 2100.0
    WHEN bakiye >= 1750 THEN 26.0 + bakiye / 1850.0
    WHEN bakiye >= 1500 THEN 23.0 + bakiye / 1600.0
    WHEN bakiye >= 1250 THEN 20.0 + bakiye / 1350.0
    WHEN bakiye >= 1000 THEN 17.0 + bakiye / 1100.0
    WHEN bakiye >= 750 THEN 14.0 + bakiye / 850.0
    WHEN bakiye >= 500 THEN 11.0 + bakiye / 600.0
    WHEN bakiye >= 250 THEN 8.0 + bakiye / 350.0
    ELSE 1.0
  END AS INT);

  -- G) Reassign segments from the fresh point totals.
  UPDATE musteri
  SET segment = CASE
    WHEN puan >= 3000 THEN 'elmas'
    WHEN puan >= 2950 THEN 'elmas'
    WHEN puan >= 2900 THEN 'elmas'
    WHEN puan >= 2850 THEN 'elmas'
    WHEN puan >= 2800 THEN 'elmas'
    WHEN puan >= 2750 THEN 'elmas'
    WHEN puan >= 2700 THEN 'elmas'
    WHEN puan >= 2650 THEN 'elmas'
    WHEN puan >= 2600 THEN 'elmas'
    WHEN puan >= 2550 THEN 'elmas'
    WHEN puan >= 2500 THEN 'elmas'
    WHEN puan >= 2450 THEN 'elmas'
    WHEN puan >= 2400 THEN 'platin'
    WHEN puan >= 2350 THEN 'platin'
    WHEN puan >= 2300 THEN 'platin'
    WHEN puan >= 2250 THEN 'platin'
    WHEN puan >= 2200 THEN 'platin'
    WHEN puan >= 2150 THEN 'platin'
    WHEN puan >= 2100 THEN 'platin'
    WHEN puan >= 2050 THEN 'platin'
    WHEN puan >= 2000 THEN 'platin'
    WHEN puan >= 1950 THEN 'platin'
    WHEN puan >= 1900 THEN 'platin'
    WHEN puan >= 1850 THEN 'platin'
    WHEN puan >= 1800 THEN 'altin'
    WHEN puan >= 1750 THEN 'altin'
    WHEN puan >= 1700 THEN 'altin'
    WHEN puan >= 1650 THEN 'altin'
    WHEN puan >= 1600 THEN 'altin'
    WHEN puan >= 1550 THEN 'altin'
    WHEN puan >= 1500 THEN 'altin'
    WHEN puan >= 1450 THEN 'altin'
    WHEN puan >= 1400 THEN 'altin'
    WHEN puan >= 1350 THEN 'altin'
    WHEN puan >= 1300 THEN 'altin'
    WHEN puan >= 1250 THEN 'altin'
    WHEN puan >= 1200 THEN 'gumus'
    WHEN puan >= 1150 THEN 'gumus'
    WHEN puan >= 1100 THEN 'gumus'
    WHEN puan >= 1050 THEN 'gumus'
    WHEN puan >= 1000 THEN 'gumus'
    WHEN puan >= 950 THEN 'gumus'
    WHEN puan >= 900 THEN 'gumus'
    WHEN puan >= 850 THEN 'gumus'
    WHEN puan >= 800 THEN 'gumus'
    WHEN puan >= 750 THEN 'gumus'
    WHEN puan >= 700 THEN 'gumus'
    WHEN puan >= 650 THEN 'gumus'
    WHEN puan >= 600 THEN 'bronz'
    WHEN puan >= 550 THEN 'bronz'
    WHEN puan >= 500 THEN 'bronz'
    WHEN puan >= 450 THEN 'bronz'
    WHEN puan >= 400 THEN 'bronz'
    WHEN puan >= 350 THEN 'bronz'
    WHEN puan >= 300 THEN 'bronz'
    WHEN puan >= 250 THEN 'bronz'
    WHEN puan >= 200 THEN 'bronz'
    WHEN puan >= 150 THEN 'bronz'
    WHEN puan >= 100 THEN 'bronz'
    WHEN puan >= 50 THEN 'bronz'
    ELSE 'yeni'
  END;

  -- H) Negative balances beyond the threshold pay a penalty on the
  --    overdraft (ABS keeps the math sign-safe).
  UPDATE musteri
  SET bakiye = bakiye - ABS(bakiye + p_esik) * p_ceza
  WHERE bakiye < 0 - p_esik;

  -- I) Per category: deduct sold stock, then write the category report
  --    row (revenue, profit, order count — all via joins) and an audit
  --    marker. One block per category.
  -- kategori k01
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k01';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k01',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k01' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k01' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k01' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k01',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k01' AND s.durum = 'acik'));

  -- kategori k02
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k02';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k02',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k02' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k02' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k02' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k02',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k02' AND s.durum = 'acik'));

  -- kategori k03
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k03';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k03',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k03' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k03' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k03' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k03',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k03' AND s.durum = 'acik'));

  -- kategori k04
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k04';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k04',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k04' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k04' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k04' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k04',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k04' AND s.durum = 'acik'));

  -- kategori k05
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k05';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k05',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k05' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k05' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k05' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k05',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k05' AND s.durum = 'acik'));

  -- kategori k06
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k06';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k06',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k06' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k06' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k06' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k06',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k06' AND s.durum = 'acik'));

  -- kategori k07
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k07';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k07',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k07' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k07' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k07' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k07',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k07' AND s.durum = 'acik'));

  -- kategori k08
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k08';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k08',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k08' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k08' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k08' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k08',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k08' AND s.durum = 'acik'));

  -- kategori k09
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k09';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k09',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k09' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k09' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k09' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k09',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k09' AND s.durum = 'acik'));

  -- kategori k10
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k10';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k10',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k10' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k10' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k10' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k10',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k10' AND s.durum = 'acik'));

  -- kategori k11
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k11';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k11',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k11' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k11' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k11' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k11',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k11' AND s.durum = 'acik'));

  -- kategori k12
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k12';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k12',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k12' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k12' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k12' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k12',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k12' AND s.durum = 'acik'));

  -- kategori k13
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k13';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k13',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k13' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k13' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k13' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k13',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k13' AND s.durum = 'acik'));

  -- kategori k14
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k14';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k14',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k14' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k14' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k14' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k14',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k14' AND s.durum = 'acik'));

  -- kategori k15
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k15';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k15',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k15' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k15' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k15' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k15',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k15' AND s.durum = 'acik'));

  -- kategori k16
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k16';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k16',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k16' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k16' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k16' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k16',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k16' AND s.durum = 'acik'));

  -- kategori k17
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k17';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k17',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k17' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k17' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k17' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k17',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k17' AND s.durum = 'acik'));

  -- kategori k18
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k18';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k18',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k18' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k18' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k18' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k18',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k18' AND s.durum = 'acik'));

  -- kategori k19
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k19';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k19',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k19' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k19' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k19' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k19',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k19' AND s.durum = 'acik'));

  -- kategori k20
  UPDATE urun
  SET stok = stok - (SELECT COALESCE(SUM(s.adet), 0)
                     FROM siparis s
                     WHERE s.urun_id = urun.id
                       AND s.durum = 'acik')
  WHERE kategori = 'k20';
  INSERT INTO rapor (donem, kategori, ciro, kar, siparis_sayisi)
  VALUES (p_donem, 'k20',
          (SELECT COALESCE(SUM(s.net), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k20' AND s.durum = 'acik'),
          (SELECT COALESCE(SUM(s.net - u.maliyet * s.adet), 0)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k20' AND s.durum = 'acik'),
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k20' AND s.durum = 'acik'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kategori_k20',
          (SELECT COUNT(*)
           FROM siparis s JOIN urun u ON s.urun_id = u.id
           WHERE u.kategori = 'k20' AND s.durum = 'acik'));

  -- J) Close the period: orders processed, old audit rows pruned.
  UPDATE siparis SET durum = 'islendi' WHERE durum = 'acik';
  DELETE FROM denetim WHERE donem < p_donem - 12;

  -- K) Closing audit snapshot.
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kapanis_net',
          (SELECT COALESCE(SUM(s.net), 0) FROM siparis s WHERE s.durum = 'islendi'));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kapanis_bakiye',
          (SELECT COALESCE(SUM(m.bakiye), 0) FROM musteri m));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kapanis_stok',
          (SELECT COALESCE(SUM(u.stok), 0) FROM urun u));
  INSERT INTO denetim (donem, olay, deger)
  VALUES (p_donem, 'kapanis_puan',
          (SELECT COALESCE(SUM(m.puan), 0) FROM musteri m));

  -- L) Category ranking by revenue (window function over the report).
  SELECT r.kategori,
         r.ciro,
         r.kar,
         RANK() OVER (ORDER BY r.ciro DESC) AS sira
  FROM rapor r
  WHERE r.donem = p_donem;

  -- M) The result of the CALL: per-segment settlement summary joined
  --    across all four tables, a math-heavy projection, and a grand
  --    total via UNION ALL.
  SELECT m.segment AS segment,
         COUNT(*) AS siparis,
         SUM(s.net) AS ciro,
         SUM(s.net - u.maliyet * s.adet) AS kar,
         AVG(s.net / (1 + p_kdv)) AS ort_net_kdvsiz,
         SUM(s.net) / NULLIF(SUM(s.adet), 0) AS birim_ciro
  FROM siparis s
  JOIN musteri m ON s.musteri_id = m.id
  JOIN urun u ON s.urun_id = u.id
  WHERE s.durum = 'islendi'
  GROUP BY m.segment
  HAVING SUM(s.net) > 0
  UNION ALL
  SELECT 'TOPLAM' AS segment,
         COUNT(*) AS siparis,
         SUM(s2.net) AS ciro,
         SUM(s2.net - u2.maliyet * s2.adet) AS kar,
         AVG(s2.net / (1 + p_kdv)) AS ort_net_kdvsiz,
         SUM(s2.net) / NULLIF(SUM(s2.adet), 0) AS birim_ciro
  FROM siparis s2
  JOIN urun u2 ON s2.urun_id = u2.id
  WHERE s2.durum = 'islendi'
  ORDER BY 3 DESC;

END
