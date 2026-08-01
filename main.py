import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import re
import os
import sys
import time
import random
import glob
from datetime import datetime, timedelta

# for GitHub: banlanır mıyım bilmiyorum ama yasak olduğunu herhangi bir yerde görmedim.

# ============================================================
# DOSYA DÜZENİ (GitHub'ın tek dosya için ~100MB sınırı var, bu
# yüzden tek beyin.json yerine PARÇALI (sharded) depolama kullanılıyor.
# Ana indeks: beyin_meta.json (küçük, hep tek dosya).
# Gerçek veri: beyin_gram1_0.json, beyin_gram1_1.json, ... gibi
# otomatik olarak N parçaya bölünen dosyalar. Her tablo (her n-gram
# derecesi + bilgi bankası + cümle deposu) kendi parçalarına sahiptir.
# Bir parça MAX_SHARD_BAYT'ı geçerse otomatik olarak ikiye bölünür.
# ============================================================
BEYIN_META_DOSYASI = "byn/beyin_meta.json"
GRAM_DOSYA_ONEKI = "byn/beyin_gram"      # + n -> beyin_gram3_0.json, beyin_gram3_1.json ...
BILGI_DOSYA_ONEKI = "byn/beyin_bilgi"    # soru-cevap ters indeksi
CUMLE_DOSYA_ONEKI = "byn/beyin_cumleler" # gerçek cümle metinleri + kaynakları
ESKI_BEYIN_DOSYASI = "beyin.json"    # eski tek-dosyalı format (artık kullanılmıyor, sadece uyarı için)

MAX_SHARD_BAYT = int(os.environ.get("MAX_SHARD_MB", 80)) * 1024 * 1024  # 100MB sınırının altında güvenli pay

README_DOSYASI = "README.md"

MAKALE_SAYISI = int(os.environ.get("MAKALE_SAYISI", 20))
ISTEK_TEKRAR_SAYISI = int(os.environ.get("ISTEK_TEKRAR_SAYISI", 20))
ISTEKLER_ARASI_BEKLEME = float(os.environ.get("ISTEKLER_ARASI_BEKLEME", 1.2))
API_DENEME_SAYISI = int(os.environ.get("API_DENEME_SAYISI", 5))

# ÇOK DAHA FAZLA KİTAP: varsayılan 3'ten 15'e çıkarıldı. Her biri TAM
# METİN (exintro yok) olduğu için bu, modele gerçek anlamda çok daha
# derin/uzun metinler okutmak demektir.
KITAP_SAYISI = int(os.environ.get("KITAP_SAYISI", 30))
KITAP_DENEME_SAYISI = int(os.environ.get("KITAP_DENEME_SAYISI", 15))
KITAP_MIN_KARAKTER = int(os.environ.get("KITAP_MIN_KARAKTER", 500))

BAGLAM_UZUNLUGU = int(os.environ.get("BAGLAM_UZUNLUGU", 15))

# Model mimarisi değişti (parçalı depolama + bilgi bankası eklendi).
MODEL_VERSIYONU = f"4.0-parcali-soru-cevap-{BAGLAM_UZUNLUGU}gram"

KAYNAKLAR = [
    {"ad": "Vikipedi", "api_url": "https://tr.wikipedia.org/w/api.php"},
    {"ad": "Vikikaynak", "api_url": "https://tr.wikisource.org/w/api.php"},
]
VIKIKAYNAK_API_URL = "https://tr.wikisource.org/w/api.php"

USER_AGENT = os.environ.get(
    "BOT_USER_AGENT",
    "OtonomNLPBotu/1.0 (+https://github.com/emirattaa/SelftrainAi)"
)

# Soru-cevap indekslemesinde göz ardı edilecek, bilgi taşımayan çok yaygın
# Türkçe kelimeler. Bunlar hem sorudan hem de indekslenen cümlelerden
# elenir ki eşleşme "ve", "bir", "bu" gibi kelimeler yüzünden değil,
# gerçekten anlamlı/nadir kelimeler yüzünden olsun.
DURAK_KELIMELER = {
    "bir", "bu", "şu", "o", "ve", "veya", "ile", "için", "gibi", "de", "da",
    "ki", "mi", "mı", "mu", "mü", "ne", "nedir", "nasıl", "neden", "niçin",
    "kaç", "hangi", "midir", "mıdır", "mudur", "müdür", "çok", "daha", "en",
    "ama", "fakat", "ancak", "her", "hiç", "tüm", "bütün", "ise", "diye",
    "olan", "olarak", "olduğu", "oldu", "olmuş", "var", "yok", "değil",
    "kadar", "sonra", "önce", "üzere", "göre", "diğer", "başka", "aynı",
    "kendi", "biri", "birçok", "birkaç", "şey", "yer", "zaman", "kez",
    "onun", "onu", "ona", "bunun", "bunu", "buna", "şunun", "şunu",
}


def logla(mesaj, tip="BİLGİ"):
    zaman = datetime.utcnow().strftime('%H:%M:%S')
    renkler = {
        "BİLGİ": "\033[94m", "BAŞARILI": "\033[92m", "UYARI": "\033[93m",
        "HATA": "\033[91m", "SİSTEM": "\033[95m", "TOKEN": "\033[96m", "SIFIRLA": "\033[0m"
    }
    renk = renkler.get(tip, renkler["BİLGİ"])
    sifirla = renkler["SIFIRLA"]
    print(f"[{zaman}] {renk}[{tip}]{sifirla} {mesaj}")


def http_oturumu_olustur():
    oturum = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    oturum.mount("https://", adapter)
    oturum.mount("http://", adapter)
    oturum.headers.update({"User-Agent": USER_AGENT})
    return oturum


# ============================================================
# PARÇALI (SHARDED) DEPOLAMA YARDIMCILARI
# ============================================================

def _sozlugu_parcala_ve_kaydet(taban_ad, sozluk):
    """
    Bir sözlüğü, GitHub'ın tek dosya sınırının (100MB) güvenli şekilde
    altında kalacak parçalara böler: taban_ad_0.json, taban_ad_1.json ...
    Önce eski parçaları temizler (parça sayısı bir önceki çalıştırmadan
    azalmış olabilir, eski artık dosyalar kalmasın diye).
    """
    for dosya in glob.glob(f"{taban_ad}_*.json"):
        os.remove(dosya)

    if not sozluk:
        with open(f"{taban_ad}_0.json", "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False)
        return 1

    tam_json = json.dumps(sozluk, ensure_ascii=False)
    toplam_boyut = len(tam_json.encode("utf-8"))

    if toplam_boyut <= MAX_SHARD_BAYT:
        with open(f"{taban_ad}_0.json", "w", encoding="utf-8") as f:
            f.write(tam_json)
        return 1

    tahmini_parca = max(2, (toplam_boyut // MAX_SHARD_BAYT) + 2)
    anahtarlar = list(sozluk.keys())
    parca_boyutu = max(1, len(anahtarlar) // tahmini_parca + 1)
    ham_parcalar = [anahtarlar[i:i + parca_boyutu] for i in range(0, len(anahtarlar), parca_boyutu)]

    parca_no = 0
    for anahtar_grubu in ham_parcalar:
        alt_sozluk = {k: sozluk[k] for k in anahtar_grubu}
        alt_json = json.dumps(alt_sozluk, ensure_ascii=False)
        if len(alt_json.encode("utf-8")) > MAX_SHARD_BAYT and len(anahtar_grubu) > 1:
            # bu parça bile hâlâ çok büyük - ikiye böl
            yarim = len(anahtar_grubu) // 2
            for alt_grup in (anahtar_grubu[:yarim], anahtar_grubu[yarim:]):
                alt_sozluk2 = {k: sozluk[k] for k in alt_grup}
                with open(f"{taban_ad}_{parca_no}.json", "w", encoding="utf-8") as f:
                    json.dump(alt_sozluk2, f, ensure_ascii=False)
                parca_no += 1
        else:
            with open(f"{taban_ad}_{parca_no}.json", "w", encoding="utf-8") as f:
                f.write(alt_json)
            parca_no += 1

    logla(f"{taban_ad}: {parca_no} parçaya bölündü (toplam ~{toplam_boyut/1024/1024:.1f} MB)", "BAŞARILI")
    return parca_no


def _sozlugu_parcalardan_yukle(taban_ad, parca_sayisi):
    sozluk = {}
    for i in range(parca_sayisi):
        dosya_adi = f"{taban_ad}_{i}.json"
        if os.path.exists(dosya_adi):
            try:
                with open(dosya_adi, "r", encoding="utf-8") as f:
                    sozluk.update(json.load(f))
            except Exception:
                logla(f"{dosya_adi} okunamadı/bozuk, atlanıyor.", "UYARI")
    return sozluk


def _bos_beyin_olustur(eski_metadata=None):
    eski_metadata = eski_metadata or {}
    return {
        "metadata": {
            "model_versiyon": MODEL_VERSIYONU,
            "ilk_olusturulma": eski_metadata.get("ilk_olusturulma", str(datetime.utcnow())),
            "son_guncelleme": "",
            "islenen_toplam_makale": eski_metadata.get("islenen_toplam_makale", 0),
            "islenen_toplam_kitap": eski_metadata.get("islenen_toplam_kitap", 0),
        },
        "istatistikler": {"toplam_dugum_sayisi": 0, "toplam_baglanti_sayisi": 0},
        "nodes": {str(n): {} for n in range(1, BAGLAM_UZUNLUGU + 1)},
        "bilgi_bankasi": {},
        "cumleler": {},
        "sonraki_cumle_id": 0,
    }


def beyni_yukle():
    """Parçalı JSON indeksini (beyin_meta.json) ve ona bağlı tüm parçaları yükler."""
    if not os.path.exists(BEYIN_META_DOSYASI):
        if os.path.exists(ESKI_BEYIN_DOSYASI):
            logla(f"Eski tek-dosyalı {ESKI_BEYIN_DOSYASI} bulundu ama artık kullanılmıyor "
                  f"(GitHub'ın ~100MB dosya sınırı nedeniyle parçalı JSON formatına geçildi). "
                  f"Model sıfırdan başlıyor; istersen eski dosyayı silebilirsin.", "UYARI")
        else:
            logla("Kayıtlı model bulunamadı. Yeni bir Nöral Ağ Mimarisi başlatılıyor.", "UYARI")
        return _bos_beyin_olustur()

    logla("Model indeksi (beyin_meta.json) yükleniyor...", "SİSTEM")
    try:
        with open(BEYIN_META_DOSYASI, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        logla(f"{BEYIN_META_DOSYASI} bozulmuş, sıfırdan oluşturuluyor...", "UYARI")
        return _bos_beyin_olustur()

    if meta.get("metadata", {}).get("model_versiyon") != MODEL_VERSIYONU:
        logla(f"Model mimarisi değişti ({meta.get('metadata', {}).get('model_versiyon')} -> {MODEL_VERSIYONU}). "
              f"Tüm parçalar sıfırlanıyor, sayaçlar korunuyor.", "UYARI")
        for taban in [f"{GRAM_DOSYA_ONEKI}{n}" for n in range(1, 10)] + [BILGI_DOSYA_ONEKI, CUMLE_DOSYA_ONEKI]:
            for dosya in glob.glob(f"{taban}_*.json"):
                os.remove(dosya)
        return _bos_beyin_olustur(eski_metadata=meta.get("metadata", {}))

    parca_sayilari = meta.get("parca_sayilari", {})
    nodes = {}
    for n in range(1, BAGLAM_UZUNLUGU + 1):
        nodes[str(n)] = _sozlugu_parcalardan_yukle(f"{GRAM_DOSYA_ONEKI}{n}", parca_sayilari.get(f"gram{n}", 0))

    bilgi_bankasi = _sozlugu_parcalardan_yukle(BILGI_DOSYA_ONEKI, parca_sayilari.get("bilgi", 0))
    cumleler = _sozlugu_parcalardan_yukle(CUMLE_DOSYA_ONEKI, parca_sayilari.get("cumleler", 0))

    return {
        "metadata": meta.get("metadata", {}),
        "istatistikler": meta.get("istatistikler", {"toplam_dugum_sayisi": 0, "toplam_baglanti_sayisi": 0}),
        "nodes": nodes,
        "bilgi_bankasi": bilgi_bankasi,
        "cumleler": cumleler,
        "sonraki_cumle_id": meta.get("sonraki_cumle_id", 0),
    }


def beyni_kaydet(beyin):
    beyin["istatistikler"]["toplam_dugum_sayisi"] = sum(len(d) for d in beyin["nodes"].values())
    beyin["metadata"]["son_guncelleme"] = str(datetime.utcnow())

    parca_sayilari = {}
    for n in range(1, BAGLAM_UZUNLUGU + 1):
        parca_sayilari[f"gram{n}"] = _sozlugu_parcala_ve_kaydet(f"{GRAM_DOSYA_ONEKI}{n}", beyin["nodes"][str(n)])
    parca_sayilari["bilgi"] = _sozlugu_parcala_ve_kaydet(BILGI_DOSYA_ONEKI, beyin["bilgi_bankasi"])
    parca_sayilari["cumleler"] = _sozlugu_parcala_ve_kaydet(CUMLE_DOSYA_ONEKI, beyin["cumleler"])

    meta = {
        "metadata": beyin["metadata"],
        "istatistikler": beyin["istatistikler"],
        "baglam_uzunlugu": BAGLAM_UZUNLUGU,
        "sonraki_cumle_id": beyin["sonraki_cumle_id"],
        "parca_sayilari": parca_sayilari,
    }
    gecici = BEYIN_META_DOSYASI + ".tmp"
    with open(gecici, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(gecici, BEYIN_META_DOSYASI)

    toplam_parca = sum(parca_sayilari.values())
    logla(f"Model kaydedildi -> {BEYIN_META_DOSYASI} + {toplam_parca} veri parçası", "BAŞARILI")


# ============================================================
# METİN İŞLEME
# ============================================================

def turkce_kucult(metin):
    return metin.replace("İ", "i").replace("I", "ı").lower()


def turkce_ilk_harfi_buyut(metin):
    """
    str.capitalize() Türkçe 'i' harfini İngilizce kurallarla 'I' yapar
    (yanlış, örn. 'istanbul' -> 'Istanbul'). Türkçe kuralına göre küçük
    'i' -> büyük 'İ' olmalı, bu yüzden ilk harfi elle çeviriyoruz.
    """
    if not metin:
        return metin
    harita = {"i": "İ", "ı": "I"}
    ilk = harita.get(metin[0], metin[0].upper())
    return ilk + metin[1:]


def _kelimelere_ayir(metin):
    metin = turkce_kucult(metin)
    metin = re.sub(r'([.,!?;])', r' \1 ', metin)
    return re.findall(r'[a-zçğıöşü0-9]+|[.,!?;]', metin)


def _metni_temizle(metin):
    metin = re.sub(r'\[\d+\]', '', metin)
    metin = re.sub(r'[_<>\[\]{}\\/|#~*]', ' ', metin)
    metin = re.sub(r'\s+', ' ', metin).strip()
    return metin


# ============================================================
# ÜRETİM (BACKOFF N-GRAM) — serbest/yaratıcı metin üretimi
# ============================================================

def _agirlikli_sec(sonraki_ihtimaller):
    if not sonraki_ihtimaller:
        return None
    toplam_agirlik = sum(sonraki_ihtimaller.values())
    if toplam_agirlik <= 0:
        return None
    rastgele_deger = random.uniform(0, toplam_agirlik)
    kumulatif = 0
    for kelime, agirlik in sonraki_ihtimaller.items():
        kumulatif += agirlik
        if rastgele_deger <= kumulatif:
            return kelime
    return None


def _sonraki_kelimeyi_bul(beyin, sonuc):
    nodes = beyin["nodes"]
    for n in range(min(BAGLAM_UZUNLUGU, len(sonuc)), 0, -1):
        baglam = " ".join(sonuc[-n:])
        tablo = nodes.get(str(n), {})
        if baglam in tablo and tablo[baglam]:
            secim = _agirlikli_sec(tablo[baglam])
            if secim is not None:
                return secim, n
    return None, 0


def _durumdan_uret(beyin, baslangic_kelimeleri, max_kelime):
    sonuc = list(baslangic_kelimeleri)
    for _ in range(max_kelime):
        sonraki, _derece = _sonraki_kelimeyi_bul(beyin, sonuc)
        if sonraki is None or sonraki == "[BITIS]":
            break
        sonuc.append(sonraki)

    temiz_sonuc = [k for k in sonuc if k not in ("[BASLANGIC]", "[BITIS]")]
    cikti = " ".join(temiz_sonuc)
    cikti = re.sub(r' ([.,!?;])', r'\1', cikti)
    cikti = turkce_ilk_harfi_buyut(cikti) if cikti else cikti
    if cikti and cikti[-1] not in ('.', '!', '?'):
        cikti += "..."
    return cikti


def yapay_zeka_konus(beyin, max_kelime=30):
    nodes = beyin.get("nodes", {})
    en_uzun_tablo = nodes.get(str(BAGLAM_UZUNLUGU), {})
    if not en_uzun_tablo:
        dolu_derece = next((n for n in range(BAGLAM_UZUNLUGU, 0, -1) if nodes.get(str(n))), None)
        if dolu_derece is None:
            return "Yeterli veriye sahip değilim."
        en_uzun_tablo = nodes[str(dolu_derece)]

    baslangic_baglamlari = [b for b in en_uzun_tablo.keys() if b.startswith("[BASLANGIC]")]
    secilen_baglam = random.choice(baslangic_baglamlari) if baslangic_baglamlari else random.choice(list(en_uzun_tablo.keys()))
    return _durumdan_uret(beyin, secilen_baglam.split(), max_kelime)


def yapay_zeka_cevapla(beyin, girdi_metni, max_kelime=35):
    """Girdiye bağlamsal, ama YİNE DE İSTATİSTİKSEL/TAHMİNİ bir devam üretir (gerçek bilgi değil)."""
    nodes = beyin.get("nodes", {})
    if not nodes or not any(nodes.values()):
        return "Yeterli veriye sahip değilim."

    girdi_kelimeleri = _kelimelere_ayir(girdi_metni)
    if not girdi_kelimeleri:
        return yapay_zeka_konus(beyin, max_kelime)

    baslangic_kelimeleri = girdi_kelimeleri[-BAGLAM_UZUNLUGU:]
    return _durumdan_uret(beyin, baslangic_kelimeleri, max_kelime)


# ============================================================
# GERÇEK SORU-CEVAP (RETRIEVAL) — n-gram üretiminden TAMAMEN AYRI.
# Model burada bir şey "uydurmuyor", gerçekten okuduğu bir cümleyi
# anahtar kelime eşleşmesine göre bulup döndürüyor.
# ============================================================

def soruyu_cevapla(beyin, soru, en_fazla_cevap=2):
    bilgi_bankasi = beyin.get("bilgi_bankasi", {})
    cumleler = beyin.get("cumleler", {})
    if not bilgi_bankasi or not cumleler:
        return []

    soru_kelimeleri = _kelimelere_ayir(soru)
    anahtar_kelimeler = [
        k for k in soru_kelimeleri
        if k not in DURAK_KELIMELER and len(k) >= 3 and k not in (".", ",", "!", "?", ";")
    ]
    anahtar_kelimeler = list(dict.fromkeys(anahtar_kelimeler))
    if not anahtar_kelimeler:
        return []

    puanlar = {}
    for kelime in anahtar_kelimeler:
        eslesenler = bilgi_bankasi.get(kelime, {})
        if not eslesenler:
            continue
        # Basit IDF: kelime az cümlede geçiyorsa (nadirse) daha bilgilendiricidir,
        # ağırlığı artırılır. Çok yaygın kelimeler eşleşmeyi domine edemez.
        idf = 1.0 / (1.0 + len(eslesenler))
        for cumle_id, frekans in eslesenler.items():
            puanlar[cumle_id] = puanlar.get(cumle_id, 0) + frekans * idf

    if not puanlar:
        return []

    siralanmis = sorted(puanlar.items(), key=lambda x: x[1], reverse=True)[:en_fazla_cevap]
    sonuclar = []
    for cumle_id, puan in siralanmis:
        kayit = cumleler.get(cumle_id)
        if kayit:
            sonuclar.append({"metin": kayit["metin"], "kaynak": kayit["kaynak"], "puan": round(puan, 3)})
    return sonuclar


def soru_cevap_dene(beyin, soru):
    """
    Önce GERÇEK bilgiden cevap arar (retrieval). Bulamazsa n-gram backoff
    ile bir TAHMİN üretir ve bunu açıkça "tahmin" olarak etiketler - kullanıcı
    hangi cevabın kaynaklı, hangisinin istatistiksel tahmin olduğunu bilir.
    """
    bulunanlar = soruyu_cevapla(beyin, soru)
    if bulunanlar:
        en_iyi = bulunanlar[0]
        logla(f"Kaynaklı cevap bulundu (kaynak: {en_iyi['kaynak']}, puan: {en_iyi['puan']})", "BAŞARILI")
        return {"tip": "bilgi", "cevap": en_iyi["metin"], "kaynak": en_iyi["kaynak"]}

    logla("Soruyla eşleşen bilgi bulunamadı, n-gram tahmini üretiliyor...", "UYARI")
    tahmin = yapay_zeka_cevapla(beyin, soru)
    return {"tip": "tahmin", "cevap": tahmin, "kaynak": None}


def kendini_test_et(beyin, egitilen_basliklar, ornek_sayisi=6):
    """
    ÖZ-TEST: bu çalıştırmada okunan başlıklardan birkaçını seçip modele
    "<Başlık> nedir?" tarzı sorular sorar ve gerçek kaynaklı bir cevap dönüp
    dönmediğini kontrol eder. Böylece model her çalıştırmada kendi soru-cevap
    yeteneğini otomatik olarak doğrular; sonuçlar loglanır ve README'ye yazılır.
    """
    logla("Öz-test (self-test) başlıyor...", "SİSTEM")
    temiz_basliklar = []
    for b in egitilen_basliklar:
        temiz = b.split(": ", 1)[-1] if ": " in b else b
        if 0 < len(temiz.split()) <= 6:
            temiz_basliklar.append(temiz)

    secilenler = random.sample(temiz_basliklar, min(ornek_sayisi, len(temiz_basliklar))) if temiz_basliklar else []

    detaylar = []
    basarili = 0
    for baslik in secilenler:
        soru = f"{baslik} nedir?"
        sonuc = soru_cevap_dene(beyin, soru)
        gecti = sonuc["tip"] == "bilgi" and len(sonuc["cevap"]) > 0
        if gecti:
            basarili += 1
            logla(f"[TEST OK] '{soru}' -> \"{sonuc['cevap'][:70]}\"", "BAŞARILI")
        else:
            logla(f"[TEST UYARI] '{soru}' -> kaynaklı cevap bulunamadı.", "UYARI")
        detaylar.append({"soru": soru, "cevap": sonuc["cevap"], "basarili": gecti})

    logla(f"Öz-test tamamlandı: {basarili}/{len(secilenler)} soru kaynaklı cevaplandı.", "SİSTEM")
    return {"toplam": len(secilenler), "basarili": basarili, "detaylar": detaylar}


# ============================================================
# README GÜNCELLEME
# ============================================================

def readme_guncelle(beyin, okunan_makale, okunan_kitap, ozdenetim, demo_soru, demo_sonuc):
    toplam_hafiza = beyin["istatistikler"]["toplam_dugum_sayisi"]
    toplam_baglanti = beyin["istatistikler"]["toplam_baglanti_sayisi"]
    toplam_makale = beyin["metadata"]["islenen_toplam_makale"]
    toplam_kitap = beyin["metadata"]["islenen_toplam_kitap"]
    toplam_kelime_indeksi = len(beyin.get("bilgi_bankasi", {}))
    toplam_indekslenen_cumle = len(beyin.get("cumleler", {}))

    su_an = datetime.utcnow() + timedelta(hours=3)
    sonraki = su_an + timedelta(minutes=30)
    su_an_str = su_an.strftime('%d.%m.%Y %H:%M:%S')
    sonraki_str = sonraki.strftime('%d.%m.%Y %H:%M:%S')

    def guvenli(s):
        return s.replace("\\", "\\\\").replace('"', '\\"')

    demo_cevap_guvenli = guvenli(demo_sonuc["cevap"])
    demo_soru_guvenli = guvenli(demo_soru)
    demo_tip_etiketi = "📚 Kaynaklı bilgi" if demo_sonuc["tip"] == "bilgi" else "🎲 İstatistiksel tahmin"
    kaynak_satiri = f"\n> *Kaynak: {guvenli(demo_sonuc['kaynak'])}*" if demo_sonuc.get("kaynak") else ""

    ozdenetim_satiri = f"`{ozdenetim['basarili']}/{ozdenetim['toplam']}` soru kaynaklı cevaplandı" if ozdenetim["toplam"] > 0 else "yeterli veri yok"

    guncel_metin = f"""<!-- BİLGİ_BAŞLANGIÇ -->
| Model Metriği | Değer |
|:---|:---|
| ⏱️ **Son Eğitim (Epoch)** | `{su_an_str}` (TR) |
| ⏳ **Tahmini Sonraki Çalışma** | `{sonraki_str}` (TR) |
| 📚 **Bu Oturumda İncelenen Makale** | `{okunan_makale}` |
| 📖 **Bu Oturumda Okunan Kitap (Tam Metin)** | `{okunan_kitap}` |
| 📈 **Modelin Gördüğü Toplam Makale** | `{toplam_makale}` |
| 📗 **Modelin Gördüğü Toplam Kitap** | `{toplam_kitap}` |
| 🧩 **Bağlam Derecesi (n-gram)** | `1..{BAGLAM_UZUNLUGU} (backoff)` |
| 🔍 **Soru-Cevap İndeksindeki Kelime** | `{toplam_kelime_indeksi}` |
| 🗂️ **İndekslenen Gerçek Cümle** | `{toplam_indekslenen_cumle}` |
| 🔗 **Ağdaki Toplam Sinaps (Bağlantı)** | `{toplam_baglanti}` |
| 🧠 **Benzersiz Düğüm (Node) Sayısı** | `{toplam_hafiza}` |
| ✅ **Öz-Test Sonucu** | {ozdenetim_satiri} |

### 💬 Soru-Cevap Demosu ({demo_tip_etiketi})
*(Bu, modelin `beyin_meta.json` + parçalı veri dosyalarını kullanarak ürettiği gerçek bir örnektir.)*

**Soru:** "{demo_soru_guvenli}"

> "{demo_cevap_guvenli}"{kaynak_satiri}
<!-- BİLGİ_BİTİŞ -->"""

    if not os.path.exists(README_DOSYASI):
        logla("README.md yok, oluşturuluyor...", "SİSTEM")
        yeni_readme = f"# Otonom NLP Botu 🧠\n\nBu depo, kendi parçalı JSON veri tabanını eğiten ve soru-cevap yapabilen bir sisteme aittir.\n\n### Eğitim İstatistikleri 📊\n{guncel_metin}\n"
        with open(README_DOSYASI, "w", encoding="utf-8") as f:
            f.write(yeni_readme)
        return

    with open(README_DOSYASI, "r", encoding="utf-8") as f:
        icerik = f.read()

    if "<!-- BİLGİ_BAŞLANGIÇ -->" not in icerik:
        yeni_icerik = icerik + "\n\n### Eğitim İstatistikleri 📊\n" + guncel_metin
    else:
        yeni_icerik = re.sub(r'<!-- BİLGİ_BAŞLANGIÇ -->.*?<!-- BİLGİ_BİTİŞ -->', guncel_metin, icerik, flags=re.DOTALL)

    with open(README_DOSYASI, "w", encoding="utf-8") as f:
        f.write(yeni_icerik)
    logla("README.md model metrikleri ile güncellendi.", "BAŞARILI")


# ============================================================
# VERİ ÇEKME (Vikipedi + Vikikaynak)
# ============================================================

def _tek_api_istegi(oturum, adet, api_url, kaynak_adi):
    parametreler = {
        "action": "query", "format": "json", "generator": "random",
        "grnnamespace": 0, "grnlimit": adet, "prop": "extracts",
        "explaintext": 1, "exintro": 1, "exlimit": "max", "redirects": 1,
    }
    for deneme in range(1, API_DENEME_SAYISI + 1):
        try:
            response = oturum.get(api_url, params=parametreler, timeout=15)
            response.raise_for_status()
            veri = response.json()
            sayfalar = veri.get("query", {}).get("pages", {})
            if not sayfalar:
                logla(f"[{kaynak_adi}] API'den sayfa dönmedi, tekrar deneniyor...", "UYARI")
                time.sleep(2 * deneme)
                continue
            makaleler = []
            for sayfa in sayfalar.values():
                baslik = sayfa.get("title", "")
                metin = _metni_temizle(sayfa.get("extract", ""))
                if len(metin) > 20:
                    makaleler.append({"baslik": f"{kaynak_adi}: {baslik}", "icerik": metin})
            return makaleler
        except requests.exceptions.RequestException as e:
            logla(f"[{kaynak_adi}] API isteği hatası (deneme {deneme}/{API_DENEME_SAYISI}): {str(e)[:80]}", "HATA")
            time.sleep(2 * deneme)
        except (ValueError, KeyError) as e:
            logla(f"[{kaynak_adi}] API yanıtı işlenemedi: {str(e)[:80]}", "HATA")
            time.sleep(2 * deneme)
    return []


def _tek_kitap_cek(oturum, api_url=VIKIKAYNAK_API_URL):
    parametreler = {
        "action": "query", "format": "json", "generator": "random",
        "grnnamespace": 0, "grnlimit": 1, "prop": "extracts",
        "explaintext": 1, "redirects": 1,
    }
    for deneme in range(1, KITAP_DENEME_SAYISI + 1):
        try:
            response = oturum.get(api_url, params=parametreler, timeout=20)
            response.raise_for_status()
            veri = response.json()
            sayfalar = veri.get("query", {}).get("pages", {})
            if not sayfalar:
                time.sleep(1.5 * deneme)
                continue
            sayfa = next(iter(sayfalar.values()))
            baslik = sayfa.get("title", "")
            metin = _metni_temizle(sayfa.get("extract", ""))
            if len(metin) >= KITAP_MIN_KARAKTER:
                logla(f"[Kitap] Uygun uzunlukta metin bulundu: '{baslik}' ({len(metin)} karakter)", "BAŞARILI")
                return {"baslik": f"Vikikaynak (kitap): {baslik}", "icerik": metin}
            else:
                logla(f"[Kitap] '{baslik}' çok kısa ({len(metin)} karakter), başka aday deneniyor...", "UYARI")
        except requests.exceptions.RequestException as e:
            logla(f"[Kitap] API isteği hatası (deneme {deneme}/{KITAP_DENEME_SAYISI}): {str(e)[:80]}", "HATA")
        except (ValueError, KeyError) as e:
            logla(f"[Kitap] API yanıtı işlenemedi: {str(e)[:80]}", "HATA")
        time.sleep(1.5 * deneme)
    return None


def kitaplari_cek(oturum, adet=KITAP_SAYISI):
    kitaplar = []
    gorulen_basliklar = set()
    for i in range(1, adet + 1):
        logla(f"Kitap {i}/{adet} aranıyor (Vikikaynak, tam metin)...", "SİSTEM")
        kitap = _tek_kitap_cek(oturum)
        if kitap and kitap["baslik"] not in gorulen_basliklar:
            gorulen_basliklar.add(kitap["baslik"])
            kitaplar.append(kitap)
        elif kitap:
            logla(f"Kitap {i}: '{kitap['baslik']}' zaten alınmıştı, atlanıyor.", "UYARI")
        if i < adet:
            time.sleep(ISTEKLER_ARASI_BEKLEME)
    return kitaplar


def kaynaklardan_makale_cek(oturum, adet=MAKALE_SAYISI, tekrar=ISTEK_TEKRAR_SAYISI):
    tum_makaleler = []
    gorulen_basliklar = set()
    for i in range(1, tekrar + 1):
        kaynak = KAYNAKLAR[(i - 1) % len(KAYNAKLAR)]
        logla(f"İstek {i}/{tekrar} -> {kaynak['ad']} ({adet} makale isteniyor)...", "SİSTEM")
        makaleler = _tek_api_istegi(oturum, adet, kaynak["api_url"], kaynak["ad"])
        yeni_sayisi = 0
        for makale in makaleler:
            if makale["baslik"] not in gorulen_basliklar:
                gorulen_basliklar.add(makale["baslik"])
                tum_makaleler.append(makale)
                yeni_sayisi += 1
        logla(f"İstek {i} ({kaynak['ad']}): {len(makaleler)} makale döndü, {yeni_sayisi} tanesi yeni.", "BAŞARILI")
        if i < tekrar:
            time.sleep(ISTEKLER_ARASI_BEKLEME)
    return tum_makaleler


# ============================================================
# EĞİTİM: hem n-gram tablolarını hem de soru-cevap bilgi bankasını doldurur
# ============================================================

def modeli_egit(beyin, metin, kaynak_basligi=""):
    nodes = beyin["nodes"]
    cumleler_metin = re.split(r'(?<=[.!?]) +', metin)
    baglanti_sayaci = 0
    indekslenen_cumle = 0

    for cumle_metni in cumleler_metin:
        words = _kelimelere_ayir(cumle_metni)
        if len(words) < 3:
            continue

        # --- n-gram eğitimi (1..BAGLAM_UZUNLUGU), üretim/yaratıcı yazım için ---
        dolgulu = ["[BASLANGIC]"] * (BAGLAM_UZUNLUGU - 1) + words + ["[BITIS]"]
        for t in range(BAGLAM_UZUNLUGU - 1, len(dolgulu) - 1):
            hedef = dolgulu[t + 1]
            for n in range(1, BAGLAM_UZUNLUGU + 1):
                baslangic_idx = t - n + 1
                if baslangic_idx < 0:
                    break
                baglam = " ".join(dolgulu[baslangic_idx:t + 1])
                tablo = nodes[str(n)]
                if baglam not in tablo:
                    tablo[baglam] = {}
                if hedef not in tablo[baglam]:
                    tablo[baglam][hedef] = 0
                    baglanti_sayaci += 1
                tablo[baglam][hedef] += 1

        # --- soru-cevap bilgi bankası indekslemesi, GERÇEK cevaplar için ---
        if len(words) >= 4:
            cumle_id = str(beyin["sonraki_cumle_id"])
            beyin["sonraki_cumle_id"] += 1

            temiz_cumle = " ".join(words)
            temiz_cumle = turkce_ilk_harfi_buyut(re.sub(r' ([.,!?;])', r'\1', temiz_cumle))
            beyin["cumleler"][cumle_id] = {"metin": temiz_cumle, "kaynak": kaynak_basligi}

            kelime_frekanslari = {}
            for kelime in words:
                if kelime in DURAK_KELIMELER or len(kelime) < 3 or kelime in (".", ",", "!", "?", ";"):
                    continue
                kelime_frekanslari[kelime] = kelime_frekanslari.get(kelime, 0) + 1

            for kelime, frekans in kelime_frekanslari.items():
                if kelime not in beyin["bilgi_bankasi"]:
                    beyin["bilgi_bankasi"][kelime] = {}
                beyin["bilgi_bankasi"][kelime][cumle_id] = frekans

            indekslenen_cumle += 1

    logla(f"Tokenize tamam (1..{BAGLAM_UZUNLUGU} gram). Yeni sinaps: {baglanti_sayaci}, indekslenen cümle: {indekslenen_cumle}", "TOKEN")
    beyin["istatistikler"]["toplam_baglanti_sayisi"] += baglanti_sayaci
    return beyin


def main():
    baslangic_zamani = time.time()
    logla("MODEL EĞİTİMİ BAŞLADI (EPOCH START)", "SİSTEM")

    beyin = beyni_yukle()
    okunan_makale_sayisi = 0
    okunan_kitap_sayisi = 0
    okunan_basliklar = []
    oturum = http_oturumu_olustur()

    logla(f"{ISTEK_TEKRAR_SAYISI} istekte (Vikipedi + Vikikaynak dönüşümlü), istek başına {MAKALE_SAYISI} rastgele madde isteniyor...", "SİSTEM")
    try:
        yeni_makaleler = kaynaklardan_makale_cek(oturum, adet=MAKALE_SAYISI, tekrar=ISTEK_TEKRAR_SAYISI)
    except Exception as e:
        logla(f"Makale çekme işlemi tamamen başarısız oldu: {str(e)[:80]}", "HATA")
        yeni_makaleler = []

    for makale in yeni_makaleler:
        logla(f"İşleniyor: {makale['baslik']}", "BİLGİ")
        beyin = modeli_egit(beyin, makale["icerik"], kaynak_basligi=makale["baslik"])
        okunan_makale_sayisi += 1
        okunan_basliklar.append(makale["baslik"])

    logla(f"{KITAP_SAYISI} adet tam metinli kitap aranıyor (Vikikaynak)...", "SİSTEM")
    try:
        yeni_kitaplar = kitaplari_cek(oturum, adet=KITAP_SAYISI)
    except Exception as e:
        logla(f"Kitap çekme işlemi tamamen başarısız oldu: {str(e)[:80]}", "HATA")
        yeni_kitaplar = []

    for kitap in yeni_kitaplar:
        logla(f"Kitap işleniyor: {kitap['baslik']} ({len(kitap['icerik'])} karakter)", "BİLGİ")
        beyin = modeli_egit(beyin, kitap["icerik"], kaynak_basligi=kitap["baslik"])
        okunan_kitap_sayisi += 1
        okunan_basliklar.append(kitap["baslik"])

    beyin["metadata"]["islenen_toplam_makale"] += okunan_makale_sayisi
    beyin["metadata"]["islenen_toplam_kitap"] += okunan_kitap_sayisi

    logla("Model ağırlıkları JSON parçalarına yazılıyor...", "SİSTEM")
    beyni_kaydet(beyin)

    # ÖZ-TEST: model bu çalıştırmada okuduğu konulara gerçekten cevap
    # verebiliyor mu, otomatik olarak kontrol edilir.
    ozdenetim = kendini_test_et(beyin, okunan_basliklar)

    # Demo: kullanıcı CEVAP_PROMPT vermişse onu soruyu-cevapla dener;
    # vermemişse öz-testten başarılı bir örneği veya serbest üretimi gösterir.
    cevap_prompt = os.environ.get("CEVAP_PROMPT", "").strip()
    if cevap_prompt:
        logla(f"Model Test Ediliyor (Kullanıcı Sorusu) -> '{cevap_prompt}'", "SİSTEM")
        demo_sonuc = soru_cevap_dene(beyin, cevap_prompt)
        demo_soru = cevap_prompt
    else:
        basarili_test = next((d for d in ozdenetim["detaylar"] if d["basarili"]), None)
        if basarili_test:
            demo_soru = basarili_test["soru"]
            demo_sonuc = {"tip": "bilgi", "cevap": basarili_test["cevap"],
                          "kaynak": next((c["kaynak"] for c in [beyin["cumleler"].get(k, {}) for k in []]), None)}
            # kaynağı öz-testten tekrar çekelim (detaylarda saklanmadı, tekrar sorgula)
            tekrar = soru_cevap_dene(beyin, demo_soru)
            demo_sonuc = tekrar
        else:
            logla("Model Test Ediliyor (Serbest Üretim Modu)...", "SİSTEM")
            demo_soru = "(serbest üretim - belirli bir soru sorulmadı)"
            demo_sonuc = {"tip": "tahmin", "cevap": yapay_zeka_konus(beyin), "kaynak": None}

    readme_guncelle(beyin, okunan_makale_sayisi, okunan_kitap_sayisi, ozdenetim, demo_soru, demo_sonuc)
    logla(f"Süreç tamamlandı. Geçen süre: {round(time.time() - baslangic_zamani, 2)} saniye", "BAŞARILI")

    if okunan_makale_sayisi == 0 and okunan_kitap_sayisi == 0:
        logla("Bu çalıştırmada hiç makale/kitap işlenemedi (ağ sorunu olabilir).", "UYARI")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logla(f"KRİTİK HATA: {str(e)}", "HATA")
        sys.exit(1)
