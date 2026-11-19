# Kararlar ve çalışma kuralları

Bu belge, bot sahibiyle yapılan sohbetlerde verilen kararların özetidir. Yeni bir sunucuda ya da yeni bir Claude oturumunda iş devam edecekse **önce bu belge okunur**. Burada yazmayan bir konuda tahmin yürütülmez, sahibine sorulur.

Gizli bilgiler (API anahtarları, şifreler, Telegram oturumu, hesap numaraları) bu belgede **yoktur** ve hiçbir zaman buraya yazılmaz. Onlar yalnızca sunucudaki `.env` ve `config.yaml` dosyalarında durur.

---

## 1. Sahibiyle çalışma kuralları (en önemlisi)

1. **Kesin cevap almadan hiçbir şey yapılmaz.** Sahibinin sözü: "Bundan sonra benden bir sorunun kesin cevabını almadan bir şey yapmayacaksın." Kendi kafana göre ayar, tasarım ya da "iyileştirme" ekleme.
2. **Tek adım.** Sahibine her seferinde tek bir adım verilir. Uzun tablolar ve uzun açıklamalar onu yorar.
3. **Hitap:** "kralım". Cevaplar **kısa ve Türkçe**. Raporlar her zaman Türkçe.
4. **Tahmin yok.** Emin değilsen "bilmiyorum" de, araştır.
5. **Duymak istediğini söyleme.** Bir fikri değerlendirirken akıllı bir şüphecinin ne diyeceğini de söyle; riskleri açıkça yaz.
6. **Bir konunun iki tarafı varsa** karşı görüşün en güçlü savunmasını da yap.
7. **Sevkiyattan önce:** bağımsız kontrol (ayrı bir inceleme) ve tam test. Testler geçmeden gönderilmez.
8. **Arkadaşı:** Tickmill hesabı sahibinin arkadaşına aittir. Sahibi, "arkadaşım Tickmill için ne derse yap" demiştir. Arkadaşın adı, e-postası, giriş bilgisi ve sohbet kimliği hiçbir yere yazılmaz ve tekrarlanmaz.
9. **Prop firma kuralları** (FundingPips, BEM) sahibinin sorumluluğundadır; kural atlatmaya yönelik özellik yapılmaz.

## 2. Sunucuda çalışan Claude için kurallar

(Ayrıntı: `docs/SUNUCU_CLAUDE.md`.)

- `.env` asla okunmaz ve yazdırılmaz.
- Bot asla zorla kapatılmaz; yalnızca `python -m copytrader stop` ile durdurulur.
- `data\` klasörüne dokunulmaz.
- `config.yaml` elle değiştirilmez ve yazdırılmaz; ayar değişiklikleri güncelleme betiği (`guncelle.py`, içindeki `migrate`) ile gelir.
- Hesap numarası ve giriş bilgisi yazdırılmaz.
- Sağlık kontrolü ve deneme komutları bot çalışırken çalıştırılmaz.
- Sunucudaki başka işler (örneğin arkadaşın GTA-6 video görevleri ve dosyaları) bize ait değildir; sahibi "elleme" dedi.

## 3. Hesaplar

| Hesap (config adı) | Ne | 1R | Notlar |
|---|---|---|---|
| `binance` | Binance vadeli (ccxt) | 25 USD | Günlük zarar freni yok (0). Ters yönlü işlem açılmaz (no_hedge). |
| `breakout` | Breakout (elle uygulanan) | 50 USD | Bot talimat verir. |
| `fx` | FundingPips (MT5) | 100 USD | Günlük %3, açık riskler dahil. Kripto konusunda da var; referans fiyat Binance vadeli. ETH en fazla 1 lot (risk düşük kalır, kabul edildi). SOL bu hesapta yok: SOL sinyalinde atlanır, diğer hesaplarda açılır; güncellemede "SOL bulunamadı" uyarısı bilinen durumdur. |
| `tickmill` | Arkadaşın Tickmill hesabı (MT5, yardımcı süreç) | 50 USD | Ayrıntılar aşağıda (bölüm 5). |
| `fx2` | BEM (MT5, yardımcı süreç) | 1000 USD | Günlük %2,5, açık riskler dahil. no_hedge. Grubun TP'si korunur, grup kapatınca kapanır. 1,8R'de otomatik kapatma YOK (sahibi böyle kalsın dedi). Tutarlılık kuralı bot tarafından takip edilmez. |

## 4. Konular (hangi sinyal nereye gider)

| Telegram konusu | Hesaplar |
|---|---|
| Kasım Ayı Kripto İşlemleri (`kripto`) | binance, breakout, tickmill, fx |
| Kasım Ayı Fx İşlemleri (`fx`) | fx, tickmill |
| Kasım Challenge 2 (`challenge2`, her tür varlık) | fx2 |

Diğer konular (Sohbet, Analiz, Duyuru...) yok sayılır.

## 5. Onay sorusu ("AÇAYIM MI?")

- **binance, fx, fx2, tickmill:** bot sinyali okurken emin olamasa da (güven düşük, TP yok/okunamadı, yüksek R...) **sormadan açar** (`confirm_open: false`).
- **breakout:** soru sorulmaya devam eder.
- Hiçbir hesapta açılmayanlar: stopu olmayan sinyal, aynı sinyalin tekrarı, açık işlemin tersi, hedefi girişin yanlış tarafında olan sinyal.
- FundingPips ve BEM'in günlük zarar kuralı (açık risk dahil) aşırı riskli sinyali yine reddeder. Binance'in günlük sınırı yoktur.
- "Evet" denirse yalnızca henüz açılmamış hesaplar açılır; önceden açılmış hesapta ikinci işlem açılmaz.

## 6. Tickmill (arkadaşın hesabı) – özel kurallar

- **Soru yok:** her durumda sorgusuz açılır.
- **Limit emir atılmaz, fiyat tetikleyince girer:**
  - Kripto: Binance vadeli fiyatı giriş seviyesine gelince.
  - Fx: FundingPips'teki emir dolunca; FundingPips'te emir yoksa FundingPips fiyatı seviyeye gelince (`reference_broker: fx`).
  - Fiyat girişe gelmeden stopa ya da ilk hedefe giderse bekleyen giriş iptal edilir. Seviyeye değip hızla uzaklaşan fiyat kovalanmaz, geri gelmesi beklenir.
- **Lot:** sinyaldeki giriş fiyatına göre hesaplanır (sinyali yazan gibi). Stopta kayıp 1R'nin (50 USD) altına inmez, 1R + 10 USD'yi geçmez (`size_from_signal_entry: true`, `lot_rounding: up`, `fees_in_risk: false`).
- Kanaldan gelen yönetim mesajları (stop taşı, TP değiştir, kapat) Tickmill'e de uygulanır.
- Bir onay sorusunda sahibi yönü/pariteyi düzeltirse Tickmill'in ilk işlemi ayrı kalır; onu `/pozkapat` ile elle yönetmek gerekir.
- Arkadaş, Tickmill için kendi komutlarını ve kendi karnesini kullanır; diğer hesapları görmez.
- Uyarı (sahibine iletildi): Tickmill bakiyesi küçük (yaklaşık 126 USD), 1R = 50 USD bakiyenin çok büyük bir kısmı.

## 7. Kartlar ve karne

- Komutlar: `/binancekart`, `/fxkart`, `/fx2kart`, `/breakoutkart` (Tickmill yok, arkadaş için değil). Otomatik kart gönderilmez.
- Komut bu haftanın işlemlerini numaralı listeler; numara yazılınca kart gelir (açık işlemde giriş kartı, kapanmışta sonuç kartı). Liste 5 dakika geçerli.
- Zaman dilimi sinyal görselinin başlığından okunur, yoksa 15 dakika.
- Tasarım (palet 26): yükselen mum #EDEFF2, düşen #D7263D; stop bölgesi kırmızı, hedef bölgesi açık gri. Etiket, renkli işaret, "ÖRNEK" yazısı yok. Oklar TradingView tarzı, ince.
- Kartta yazan R = **cebe giren net para** (komisyon dahil) ÷ ilk risk. Elle kapatılan kısım da sayılır (Binance'te borsadaki işlem geçmişinden okunur). Aynı anda aynı yönde aynı coinde botun başka işlemi varsa ve elle kapatılan kısım ayırt edilemiyorsa kart verilmez, nedeni yazılır.
- Kutu kapanışta biter; mumlar TP ya da stop dokunuşuna kadar devam eder.

## 8. Bekçi

- Bot durursa Telegram'dan haber verir.
- Disk 5 GB'ın altına inerse uyarır ve 30 dakikada bir hatırlatır ("geri geldi" mesajı yok).
- Sunucudaki disk dolmasının sebebi şimdiye kadar hep arkadaşın video kayıtları oldu; sahibi bunlara dokunulmasını istemedi.

## 9. Güncelleme

- Kod GitHub'daki `claude/telegram-crypto-automation-o6xo7l` dalındadır.
- Sunucuda güncelleme `docs/SUNUCU_CLAUDE.md` adımlarıyla yapılır: zip indir → `python -m copytrader stop` → `python guncelle.py --baslatma --bekci-yok` → botu ayrı pencerede başlat → kontrol (Telegram'da "🚀 Kopya-trade motoru başladı").
- Ayar değişiklikleri güncelleme sırasında `copytrader/migrate.py` tarafından yazılır (yedek alınır). Bu adımlar her güncellemede çalışır: elle silinen bir satır geri gelir; kapatmak için tersini yazmak gerekir.

## 10. Taşıma

Yeni sunucuya geçiş: `docs/TASIMA.md`.
