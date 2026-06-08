# hpemsal_arama_v2.py — Aşama 1: Direkt API ile ID Toplama
# Selenium YOK — doğrudan /aramadetaylist API'sine istek atar.
# Her yıl ayrı CSV: hpemsal_2019.csv
#
# Yapılandırma bölümündeki tarihleri değiştirip çalıştırın:
#   python hpemsal_arama_v2.py

import time
import json
import os
import requests
import pandas as pd
import urllib3

# SSL uyarılarını kapat
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- YAPILANDIRMA ---
BASLANGIC_TARIHI = '01.11.2025'
BITIS_TARIHI = '31.12.2025'
SAYFA_BASI_KAYIT = 100
MAX_SAYFA = 10000
ISTEK_ARASI_BEKLEME = 0.1     # Sayfalar arası bekleme (saniye)
RATE_LIMIT_BEKLEME = 30     # 429 sonrası bekleme (saniye)

PROJE_DIZIN = os.path.dirname(os.path.abspath(__file__))
API_URL = "https://emsal.uyap.gov.tr/aramadetaylist"
SITE_URL = "https://emsal.uyap.gov.tr/"


# ============================================================
# SESSION YÖNETİMİ
# ============================================================

def session_baslat():
    """Siteyi ziyaret edip JSESSIONID cookie'si alır."""
    session = requests.Session()
    
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/148.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://emsal.uyap.gov.tr",
        "Referer": "https://emsal.uyap.gov.tr/index",
        "Connection": "keep-alive",
    })
    
    print("Session başlatılıyor...")
    try:
        resp = session.get(SITE_URL, verify=False, timeout=120)
        print(f"  Site yanıtı: {resp.status_code}")
        
        # /index sayfasını da ziyaret et (session'ı düzgün başlat)
        time.sleep(2)
        resp2 = session.get(SITE_URL + "index", verify=False, timeout=120)
        print(f"  Index yanıtı: {resp2.status_code}")
        
        cookies = session.cookies.get_dict()
        jsession = cookies.get("JSESSIONID", "")
        print(f"  JSESSIONID: {'✓ ' + jsession[:20] + '...' if jsession else '✗ Alınamadı!'}")
            
        return session
    except Exception as e:
        print(f"  Session hatası: {e}")
        return None


# ============================================================
# API İSTEĞİ
# ============================================================

def sayfa_cek(session, baslangic_tarihi, bitis_tarihi, sayfa_no):
    """Tek bir sayfa verisini API'den çeker."""
    
    # Payload'u "data" içinde sar (tarayıcının gönderdiği format)
    payload = {
        "data": {
            "arananKelime": "",
            "baslangicTarihi": baslangic_tarihi,
            "bitisTarihi": bitis_tarihi,
            "pageNumber": sayfa_no,
            "pageSize": SAYFA_BASI_KAYIT,
            "siralama": "1",
            "siralamaDirection": "desc",
            "esasYili": "",
            "esasIlkSiraNo": "",
            "esasSonSiraNo": "",
            "kararYili": "",
            "kararIlkSiraNo": "",
            "kararSonSiraNo": "",
            "birimiSiraNumara": "",
        }
    }
    
    try:
        resp = session.post(
            API_URL,
            json=payload,
            verify=False,
            timeout=15,
            headers={"Content-Type": "application/json; charset=UTF-8"}
        )
        
        if resp.status_code == 200:
            body = resp.text
            if not body or body.strip() == "":
                print(f"  ✗ Boş yanıt (0 byte)")
                return None
            try:
                return resp.json()
            except Exception as e:
                print(f"  ✗ JSON parse: {e}")
                print(f"    İlk 200 byte: {body[:200]}")
                return None
        elif resp.status_code in (429, 404, 403):
            # Rate limit veya geçici engel — bekle
            print(f"  ⚠ {resp.status_code} Rate Limit — {RATE_LIMIT_BEKLEME}sn bekleniyor...")
            time.sleep(RATE_LIMIT_BEKLEME)
            return "RATE_LIMIT"
        else:
            print(f"  ✗ HTTP {resp.status_code}: {resp.text[:200]}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"  ✗ Timeout (15sn)")
        return None
    except Exception as e:
        print(f"  ✗ İstek hatası: {e}")
        return None


def kayitlari_cikart(json_data):
    """API yanıtından kayıtları çıkartır."""
    if not json_data or json_data == "RATE_LIMIT":
        return [], 0
    
    # Toplam kayıt sayısını bul
    toplam = 0
    data_list = None
    
    if isinstance(json_data, dict):
        toplam = json_data.get("recordsTotal", json_data.get("total", 0))
        
        inner = json_data.get("data", {})
        if isinstance(inner, dict) and "data" in inner:
            data_list = inner["data"]
            toplam = inner.get("recordsTotal", toplam)
        elif isinstance(inner, list):
            data_list = inner
        
        # Alternatif alanlar
        if not data_list:
            for key in ["aaData", "result", "results", "list", "content"]:
                if key in json_data and isinstance(json_data[key], list):
                    data_list = json_data[key]
                    break
    elif isinstance(json_data, list):
        data_list = json_data
    
    if not data_list:
        return [], toplam
    
    kayitlar = []
    for item in data_list:
        if not isinstance(item, dict):
            continue
        kayit = {
            "id": str(item.get("id", "")),
            "daire": item.get("daire", ""),
            "esas_no": item.get("esasNo", ""),
            "karar_no": item.get("kararNo", ""),
            "karar_tarihi": item.get("kararTarihi", ""),
            "durum": item.get("durum", "")
        }
        if kayit["id"]:
            kayitlar.append(kayit)
    
    return kayitlar, toplam


# ============================================================
# CSV YÖNETİMİ
# ============================================================

def csv_dosya_adi(yil):
    return os.path.join(PROJE_DIZIN, f"hpemsal_{yil}.csv")


def mevcut_idleri_yukle(yil):
    dosya = csv_dosya_adi(yil)
    if os.path.exists(dosya):
        try:
            df = pd.read_csv(dosya, encoding="utf-8-sig", sep=";")
            idler = set(df["id"].dropna().astype(str).tolist())
            print(f"Mevcut CSV ({yil}): {len(df)} kayıt")
            return idler
        except Exception as e:
            print(f"CSV okuma hatası: {e}")
    return set()


def kayitlari_csv_yaz(yil, kayitlar, mevcut_idler):
    """Yeni kayıtları CSV'ye ekler. Duplikatları atlar."""
    dosya = csv_dosya_adi(yil)
    yeni_kayitlar = [k for k in kayitlar if k["id"] not in mevcut_idler]
    
    if not yeni_kayitlar:
        return 0
    
    df = pd.DataFrame(yeni_kayitlar)
    dosya_var = os.path.exists(dosya)
    df.to_csv(dosya, mode='a', header=not dosya_var,
              index=False, encoding="utf-8-sig", sep=";")
    
    for k in yeni_kayitlar:
        mevcut_idler.add(k["id"])
    
    return len(yeni_kayitlar)


# ============================================================
# ANA FONKSİYON
# ============================================================

def main():
    baslangic_tarihi = BASLANGIC_TARIHI
    bitis_tarihi = BITIS_TARIHI
    yil = bitis_tarihi.split('.')[-1]
    
    print(f"{'='*60}")
    print(f"UYAP Emsal — Aşama 1: Direkt API ile ID Toplama")
    print(f"📅 YIL: {yil}")
    print(f"Tarih: {baslangic_tarihi} - {bitis_tarihi}")
    print(f"Sayfa başı: {SAYFA_BASI_KAYIT} | Max: {MAX_SAYFA}")
    print(f"CSV: {csv_dosya_adi(yil)}")
    print(f"{'='*60}")
    
    # Mevcut CSV kontrol
    mevcut_idler = mevcut_idleri_yukle(yil)
    
    # Kaldığı sayfadan devam et
    baslangic_sayfa = (len(mevcut_idler) // SAYFA_BASI_KAYIT) + 1
    if baslangic_sayfa > 1:
        print(f"\n→ {len(mevcut_idler)} mevcut kayıt, sayfa {baslangic_sayfa}'dan devam ediliyor...")
    
    # Session başlat
    session = session_baslat()
    if not session:
        print("Session başlatılamadı!")
        return
    
    toplam_yeni = 0
    toplam_kayit = 0
    baslangic = time.time()
    ardisik_hata = 0
    bos_sayfa_sayaci = 0
    
    try:
        sayfa_no = baslangic_sayfa
        while sayfa_no <= MAX_SAYFA:
            # API isteği
            json_data = sayfa_cek(session, baslangic_tarihi, bitis_tarihi, sayfa_no)
            
            # Rate limit — bekle, session yenile ve tekrar dene
            if json_data == "RATE_LIMIT":
                ardisik_hata += 1
                if ardisik_hata >= 10:
                    print(f"\n  ✗ 10 ardışık rate limit — durduruluyor.")
                    break
                print(f"  Session yenileniyor... (deneme {ardisik_hata}/10)")
                session = session_baslat()
                if not session:
                    break
                continue  # Aynı sayfayı tekrar dene
            
            if json_data is None:
                ardisik_hata += 1
                if ardisik_hata >= 10:
                    print(f"\n  ✗ 10 ardışık hata — durduruluyor.")
                    break
                # Giderek artan bekleme: 10, 20, 30, 60, 60...
                bekleme = min(ardisik_hata * 10, 60)
                print(f"  Tekrar deneniyor... ({ardisik_hata}/10) — {bekleme}sn bekleniyor")
                time.sleep(bekleme)
                continue  # Aynı sayfayı tekrar dene
            
            ardisik_hata = 0  # Başarılı → sıfırla
            
            # Kayıtları çıkart
            kayitlar, sayfa_toplam = kayitlari_cikart(json_data)
            if sayfa_toplam > 0:
                toplam_kayit = sayfa_toplam
            
            if not kayitlar:
                # Gerçekten son sayfa mı yoksa geçici hata mı?
                beklenen = toplam_kayit if toplam_kayit > 0 else float('inf')
                toplanan = len(mevcut_idler)
                if toplanan >= beklenen:
                    print(f"  Sayfa {sayfa_no}: Boş — tüm kayıtlar toplandı ✓")
                    break
                # Geçici hata — retry
                bos_sayfa_sayaci += 1
                if bos_sayfa_sayaci >= 10:
                    print(f"\n  ✗ 10 boş sayfa denemesi — durduruluyor.")
                    break
                bekleme = min(bos_sayfa_sayaci * 10, 60)
                print(f"  Sayfa {sayfa_no}: Boş — beklenmedik! ({bos_sayfa_sayaci}/10) — {bekleme}sn bekleniyor")
                time.sleep(bekleme)
                # Sadece 3+ denemede session yenile
                if bos_sayfa_sayaci >= 3:
                    session = session_baslat()
                    if not session:
                        break
                continue
            
            bos_sayfa_sayaci = 0  # Veri geldi → boş sayfa sayacı sıfırla
            
            # CSV'ye yaz
            eklenen = kayitlari_csv_yaz(yil, kayitlar, mevcut_idler)
            toplam_yeni += eklenen
            
            # Progress
            gecen = time.time() - baslangic
            islem_sayfa = sayfa_no - baslangic_sayfa + 1
            hiz = islem_sayfa / gecen if gecen > 0 else 0
            toplam_sayfa = (toplam_kayit // SAYFA_BASI_KAYIT) + 1 if toplam_kayit > 0 else "?"
            kalan = ""
            if isinstance(toplam_sayfa, int) and hiz > 0:
                kalan_sn = (toplam_sayfa - sayfa_no) / hiz
                kalan = f" | ~{kalan_sn/60:.1f}dk kaldı"
            
            print(f"  Sayfa {sayfa_no}/{toplam_sayfa}: "
                  f"{len(kayitlar)} kayıt, {eklenen} yeni "
                  f"({hiz:.1f} sayfa/sn{kalan})")
            
            # Son sayfa kontrolü
            if toplam_kayit > 0 and sayfa_no * SAYFA_BASI_KAYIT >= toplam_kayit:
                print(f"\n  ✓ Tüm {toplam_kayit:,} kayıt toplandı!")
                break
            
            sayfa_no += 1
            time.sleep(ISTEK_ARASI_BEKLEME)
    
    except KeyboardInterrupt:
        print("\n\n[!] Durduruldu.")
    except Exception as e:
        print(f"\n[HATA] {e}")
        import traceback
        traceback.print_exc()
    
    gecen = time.time() - baslangic
    
    print(f"\n{'='*60}")
    print(f"📅 {yil} TAMAMLANDI ({gecen:.0f}sn = {gecen/60:.1f}dk)")
    print(f"  Toplam API kayıt : {toplam_kayit:,}")
    print(f"  Yeni eklenen     : {toplam_yeni:,}")
    print(f"  CSV toplam       : {len(mevcut_idler):,}")
    print(f"  CSV dosyası      : {csv_dosya_adi(yil)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
