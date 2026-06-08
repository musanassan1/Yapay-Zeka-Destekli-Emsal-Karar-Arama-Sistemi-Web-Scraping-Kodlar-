#!/usr/bin/env python3
"""
hpemsal_metin_v2.py — Aşama 2: Karar Metni Çekme
Bright Data proxy + aiohttp ile paralel metin çekme.
SQLite'dan okur, metni çeker, DB'ye yazar.
Tarihe göre sıralı, kaldığı yerden devam eder.
"""

import sys
from collections import deque

import time
import os
import re
import json
import random
import asyncio
import sqlite3
import aiohttp
from dotenv import load_dotenv

# .env dosyasından proxy bilgilerini yükle
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# --- YAPILANDIRMA ---
BASE_URL = "https://emsal.uyap.gov.tr/getDokuman?id="
PARALEL_ISTEK = 150         # Aynı anda kaç istek (Dengeli Yük)
HATA_BEKLEME = 10           # Hata sonrası bekleme (saniye)
MAX_RETRY = 5               # Başarısız kayıt için max deneme
TIMEOUT = 150               # İstek timeout (saniye) - UYAP çok geç yanıt verirse bile bekle
TEST_LIMIT = 0             # 0 = Limit yok
PROXY_AKTIF = True         # Proxy kapalı — direkt bağlantı testi
CB_ESIK = 10                # Circuit breaker: kaç ardışık kötü batch
CB_HATA_ORAN = 80           # Circuit breaker: batch hata oranı eşiği (%)

PROJE_DIZIN = os.path.dirname(os.path.abspath(__file__))
DB_DOSYA = os.path.join(PROJE_DIZIN, 'hpemsal.db')

# Proxy ayarları (.env'den)
PROXY_HOST = os.getenv("PROXY_HOST", "brd.superproxy.io")
PROXY_PORT = os.getenv("PROXY_PORT", "33335")
PROXY_USER = os.getenv("PROXY_USER", "brd-customer-hl_a20378b2-zone-emsaldevam-country-tr")
PROXY_PASS = os.getenv("PROXY_PASS", "tt4eopea3e59")

# Bot tespiti önleme — rastgele User-Agent havuzu
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]


# ============================================================
# VERİTABANI İŞLEMLERİ
# ============================================================

def db_istatistik():
    """DB'deki kayıt durumunu yazdırır."""
    conn = sqlite3.connect(DB_DOSYA)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM kararlar")
    toplam = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM kararlar WHERE scrape_durumu = 'bekliyor'")
    bekleyen = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM kararlar WHERE scrape_durumu = 'basarili'")
    basarili = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM kararlar WHERE scrape_durumu = 'basarisiz'")
    basarisiz = c.fetchone()[0]
    
    conn.close()
    
    print(f"  Toplam    : {toplam:,}")
    print(f"  Bekleyen  : {bekleyen:,}")
    print(f"  Başarılı  : {basarili:,}")
    print(f"  Başarısız : {basarisiz:,}")
    return toplam, bekleyen, basarili, basarisiz


def bekleyen_idleri_al():
    """Scrape edilmemiş ID'leri tarihe göre sıralı döndürür."""
    conn = sqlite3.connect(DB_DOSYA)
    c = conn.cursor()
    c.execute("""
        SELECT id FROM kararlar 
        WHERE scrape_durumu = 'bekliyor' 
        ORDER BY rowid ASC
    """)
    idler = [row[0] for row in c.fetchall()]
    conn.close()
    return idler


def batch_kaydet(sonuclar):
    """Batch sonuçlarını DB'ye yazar."""
    conn = sqlite3.connect(DB_DOSYA)
    c = conn.cursor()
    
    basarili_kayitlar = []
    basarisiz_kayitlar = []
    
    for karar_id, metin, basarili in sonuclar:
        if basarili and metin:
            basarili_kayitlar.append((metin, 'basarili', str(karar_id)))
        else:
            basarisiz_kayitlar.append(('basarisiz', str(karar_id)))
    
    if basarili_kayitlar:
        c.executemany(
            "UPDATE kararlar SET metin = ?, scrape_durumu = ? WHERE id = ?",
            basarili_kayitlar
        )
    
    if basarisiz_kayitlar:
        c.executemany(
            "UPDATE kararlar SET scrape_durumu = ? WHERE id = ?",
            basarisiz_kayitlar
        )
    
    conn.commit()
    conn.close()
    return len(basarili_kayitlar), len(basarisiz_kayitlar)


# ============================================================
# METİN TEMİZLEME
# ============================================================

def metin_temizle(metin):
    if not metin:
        return ""
    metin = re.sub(r'<br\s*/?>', '\n', metin, flags=re.IGNORECASE)
    metin = re.sub(r'<[^>]+>', '', metin)
    metin = metin.replace('&nbsp;', ' ')
    metin = metin.replace('&amp;', '&')
    metin = metin.replace('&lt;', '<')
    metin = metin.replace('&gt;', '>')
    metin = metin.replace('&quot;', '"')
    metin = re.sub(r'\n{3,}', '\n\n', metin)
    metin = re.sub(r' {2,}', ' ', metin)
    return metin.strip()


# ============================================================
# PROXY
# ============================================================

def proxy_url_al():
    """Oxylabs proxy URL'i döndürür. Session ID yok → her istek otomatik farklı IP alır."""
    if not PROXY_AKTIF or not PROXY_USER:
        return None
    # Oxylabs: sessid olmadan her istek otomatik rotasyon yapar (1sn vs 45sn)
    return f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"


# ============================================================
# ASYNC FETCHER
# ============================================================

async def karar_metni_cek(session, karar_id, semaphore, istatistik, proxy_url):
    """Tek bir karar ID'si için metin çeker (async).
    Her kayıt MAX_RETRY kez denenir. Hata sonrası bekleme + retry."""
    url = f"{BASE_URL}{karar_id}"
    
    
    for deneme in range(1, MAX_RETRY + 1):
        async with semaphore:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, */*;q=0.1",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            }
            
            try:
                timeout = aiohttp.ClientTimeout(total=TIMEOUT)
                async with session.get(url, headers=headers, proxy=proxy_url, timeout=timeout) as response:
                    
                    if response.status == 429:
                        istatistik["rate_limits"] += 1
                        proxy_url = proxy_url_al()  # Proxy'i değiştir
                        await asyncio.sleep(2)  # Proxy değiştiği için uzun beklemeye gerek yok
                        continue
                    
                    if response.status != 200:
                        istatistik["http_hatalari"] += 1
                        proxy_url = proxy_url_al()
                        await asyncio.sleep(2)
                        continue
                    
                    text = await response.text()
                    
                    try:
                        json_data = json.loads(text)
                    except Exception:
                        istatistik["parse_hatalari"] += 1
                        proxy_url = proxy_url_al()
                        await asyncio.sleep(2)
                        continue
                    
                    metadata = json_data.get("metadata", {})
                    if metadata.get("FMTY") != "SUCCESS":
                        istatistik["no_success"] += 1
                        proxy_url = proxy_url_al()
                        await asyncio.sleep(2)
                        continue
                    
                    html_metin = json_data.get("data", "")
                    if isinstance(html_metin, str) and html_metin.startswith("null"):
                        html_metin = html_metin[4:]
                    metin = metin_temizle(html_metin)
                    
                    if metin and len(metin) > 50:
                        istatistik["basarili"] += 1
                        return karar_id, metin, True
                    else:
                        istatistik["bos_metin"] += 1
                        proxy_url = proxy_url_al()
                        await asyncio.sleep(2)
                        continue
                        
            except asyncio.TimeoutError:
                istatistik["timeouts"] += 1
                break  # 150sn bekledikten sonra yanıt gelmezse pes et, zaman kaybetmeden diğer kayda geç.
            except Exception as e:
                istatistik["baglanti_hatalari"] += 1
                proxy_url = proxy_url_al()
                await asyncio.sleep(2)
                continue
    
    istatistik["basarisiz"] += 1
    return karar_id, None, False


# ============================================================
# ANA İŞLEM
# ============================================================

async def ana_islem(bekleyen_idler, istatistik):
    
    semaphore = asyncio.Semaphore(PARALEL_ISTEK)
    proxy_url = proxy_url_al()
    
    connector = aiohttp.TCPConnector(
        limit=PARALEL_ISTEK * 2,
        limit_per_host=PARALEL_ISTEK * 2,
        ssl=False,
        ttl_dns_cache=300,
    )
    
    baslangic = time.time()
    toplam = len(bekleyen_idler)
    son_batch_sureleri = deque(maxlen=5)  # Son 5 batch süresi
    
    async with aiohttp.ClientSession(connector=connector) as session:
        batch_boyut = 150   # Her batch'te 150 kayıt
        
        ardisik_kotu = 0  # Circuit breaker sayacı
        cb_dinlenme_sayisi = 0  # Maksimum 3 dinlenme hakkı için sayaç
        
        for batch_start in range(0, toplam, batch_boyut):
            batch_baslangic = time.time()
            batch = bekleyen_idler[batch_start:batch_start + batch_boyut]
            
            gorevler = [
                karar_metni_cek(session, kid, semaphore, istatistik, proxy_url_al())
                for kid in batch
            ]
            
            sonuclar = await asyncio.gather(*gorevler, return_exceptions=True)
            
            # Sonuçları işle
            batch_sonuc = []
            batch_basarili = 0
            batch_basarisiz = 0
            for sonuc in sonuclar:
                if isinstance(sonuc, Exception):
                    istatistik["basarisiz"] += 1
                    batch_basarisiz += 1
                    continue
                batch_sonuc.append(sonuc)
                if sonuc[2]:  # basarili flag
                    batch_basarili += 1
                else:
                    batch_basarisiz += 1
            
            # DB'ye yaz — crash'te veri kaybı sıfır
            if batch_sonuc:
                ok, fail = batch_kaydet(batch_sonuc)
            
            # Circuit Breaker — ardışık kötü batch kontrolü
            batch_toplam = batch_basarili + batch_basarisiz
            batch_hata_oran = (batch_basarisiz / batch_toplam * 100) if batch_toplam > 0 else 0
            if batch_hata_oran >= CB_HATA_ORAN:
                ardisik_kotu += 1
            else:
                ardisik_kotu = 0
            
            if ardisik_kotu >= CB_ESIK:
                cb_dinlenme_sayisi += 1
                if cb_dinlenme_sayisi <= 3:
                    print(f"\n\n🛑 CIRCUIT BREAKER TETİKLENDİ ({cb_dinlenme_sayisi}/3)!")
                    print(f"  UYAP Kırmızı Alarma Geçti. Güvenlik duvarının sıfırlanması için 600 saniye (10 dakika) bekleniyor...")
                    await asyncio.sleep(600)
                    print(f"\n▶ Dinlenme bitti, operasyona kaldığı yerden devam ediliyor...")
                    ardisik_kotu = 0  # Sigortayı geri kaldır
                    continue
                else:
                    print(f"\n\n🛑 SİSTEM KAPANDI: Circuit Breaker 4. kez tetiklendi!")
                    print(f"  Blokaj aşılamadı. Script normal olarak sonlandırılıyor.")
                    print(f"  Son batch hata oranı: %{batch_hata_oran:.0f}")
                    break
            
            # Progress
            batch_sure = time.time() - batch_baslangic
            anlik_hiz = len(batch) / batch_sure if batch_sure > 0 else 0
            
            yapilan = min(batch_start + batch_boyut, toplam)
            gecen = time.time() - baslangic
            hiz = yapilan / gecen if gecen > 0 else 0
            kalan = (toplam - yapilan) / anlik_hiz if anlik_hiz > 0 else 0
            basari_orani = istatistik["basarili"] / yapilan * 100 if yapilan > 0 else 0
            
            cb_gosterge = f"⚠CB:{ardisik_kotu}/{CB_ESIK}" if ardisik_kotu >= 3 else ""
            
            print(f"  [{yapilan:,}/{toplam:,}] "
                  f"✓{istatistik['basarili']:,} ✗{istatistik['basarisiz']:,} "
                  f"429:{istatistik['rate_limits']} "
                  f"| ort:{hiz:.1f}/sn anlık:{anlik_hiz:.1f}/sn "
                  f"| ~{kalan/60:.1f}dk kaldı "
                  f"| %{basari_orani:.0f} {cb_gosterge}")
            
            await asyncio.sleep(0.1)
    
    return istatistik


class LogTee:
    """stdout'u hem terminale hem log dosyasına yazar."""
    def __init__(self, log_dosya):
        self.terminal = sys.stdout
        self.log = open(log_dosya, 'a', encoding='utf-8')
    def write(self, mesaj):
        self.terminal.write(mesaj)
        self.log.write(mesaj)
        self.log.flush()
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    def close(self):
        self.log.close()


def main():
    # Log dosyası — tüm çıktı buraya da yazılır
    log_dosya = os.path.join(PROJE_DIZIN, 'metin_scrape_log.txt')
    tee = LogTee(log_dosya)
    sys.stdout = tee
    
    proxy_url = proxy_url_al()
    proxy_durum = f"✓ {PROXY_HOST}:{PROXY_PORT}" if proxy_url else "✗ KAPALI"
    
    print(f"{'='*60}")
    print(f"UYAP Emsal — Aşama 2: Karar Metni Çekme")
    print(f"  Başlangıç: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"  Proxy   : {proxy_durum}")
    print(f"  Paralel : {PARALEL_ISTEK}")
    print(f"  DB      : {DB_DOSYA}")
    print(f"  Log     : {log_dosya}")
    print(f"{'='*60}")
    
    # DB durumu
    print(f"\n📊 Veritabanı durumu:")
    toplam, bekleyen, basarili, basarisiz = db_istatistik()
    
    if bekleyen == 0:
        print("\n✅ Tüm metinler zaten çekilmiş!")
        sys.stdout = tee.terminal
        tee.close()
        return
    
    # Bekleyen ID'leri al
    bekleyen_idler = bekleyen_idleri_al()
    print(f"\n→ {len(bekleyen_idler):,} kayıt bekliyor (tarihe göre sıralı)")
    
    if TEST_LIMIT > 0:
        bekleyen_idler = bekleyen_idler[:TEST_LIMIT]
        print(f"  Hedef: {TEST_LIMIT:,} kayıt")
    
    if not proxy_url:
        print(f"\n  ⚠ PROXY KAPALI — 429 rate limit riski var!")
    
    baslangic = time.time()
    toplam_basarili = 0
    tur_sayisi = 0
    istatistik = {
        "basarili": 0, "basarisiz": 0,
        "rate_limits": 0, "timeouts": 0,
        "http_hatalari": 0, "parse_hatalari": 0,
        "no_success": 0, "bos_metin": 0,
        "baglanti_hatalari": 0
    }
    
    # Ana döngü — hata durumunda otomatik yeniden başlatır
    while True:
        tur_sayisi += 1
        
        try:
            print(f"\n--- Tur {tur_sayisi} başlıyor ({time.strftime('%H:%M:%S')}) ---")
            asyncio.run(ana_islem(bekleyen_idler, istatistik))
            toplam_basarili += istatistik.get('basarili', 0)
            break  # Normal bitiş
            
        except KeyboardInterrupt:
            print("\n\n[!] Durduruldu.")
            print("  ✅ Son batch'e kadar tüm veriler DB'ye yazıldı.")
            toplam_basarili = istatistik.get('basarili', 0)
            break
            
        except Exception as e:
            print(f"\n[HATA] Tur {tur_sayisi}: {e}")
            import traceback
            traceback.print_exc()
            
            # Kalan bekleyenleri tekrar al, devam et
            print(f"  30sn beklenip tekrar denenecek...")
            time.sleep(30)
            
            bekleyen_idler = bekleyen_idleri_al()
            if TEST_LIMIT > 0:
                # Kalan hedef kadar al
                toplam_basarili += istatistik.get('basarili', 0)
                istatistik = {
                    "basarili": 0, "basarisiz": 0,
                    "rate_limits": 0, "timeouts": 0,
                    "http_hatalari": 0, "parse_hatalari": 0,
                    "no_success": 0, "bos_metin": 0,
                    "baglanti_hatalari": 0
                }
                kalan_hedef = TEST_LIMIT - toplam_basarili
                if kalan_hedef <= 0:
                    print(f"  ✅ Hedef ({TEST_LIMIT:,}) zaten tamamlandı!")
                    break
                bekleyen_idler = bekleyen_idler[:kalan_hedef]
            
            if not bekleyen_idler:
                print(f"  ✅ Bekleyen kayıt kalmadı!")
                break
            
            print(f"  → {len(bekleyen_idler):,} kayıt ile devam ediliyor...")
            continue
    
    gecen = time.time() - baslangic
    
    # Final rapor
    print(f"\n{'='*60}")
    print(f"TAMAMLANDI ({gecen:.1f}sn = {gecen/60:.1f}dk = {gecen/3600:.1f}sa)")
    print(f"  Bitiş: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    if istatistik:
        print(f"  Bu tur başarılı : {istatistik.get('basarili', 0):,}")
        print(f"  Bu tur başarısız: {istatistik.get('basarisiz', 0):,}")
        print(f"  429 Rate Lmt   : {istatistik.get('rate_limits', 0):,}")
        print(f"  Timeout        : {istatistik.get('timeouts', 0):,}")
        print(f"  Bağlantı Hata  : {istatistik.get('baglanti_hatalari', 0):,}")
    print(f"  Toplam başarılı: {toplam_basarili:,}")
    if gecen > 0:
        print(f"  Ortalama hız   : {toplam_basarili/gecen:.1f} kayıt/sn")
    
    # Son durum
    print(f"\n📊 Son durum:")
    db_istatistik()
    print(f"{'='*60}")
    
    # Log'u kapat
    sys.stdout = tee.terminal
    tee.close()
    print(f"\n📄 Log dosyası: {log_dosya}")


if __name__ == "__main__":
    main()
