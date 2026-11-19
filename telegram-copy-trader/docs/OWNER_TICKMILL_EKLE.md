# Sahip Rehberi: Tickmill Hesabını Bota Ekleme (MetaApi)

Bu rehber **bot sahibi** içindir. Sunucuda (Windows VPS) PowerShell ile yapılır.

## Nasıl çalışır?

- Sinyal grubunda sadece sen varsın. Tek bot, senin sunucunda, senin Telegram oturumunla grubu okur.
- Arkadaşın grupta değil. Kendi botunu da kurmaz.
- Arkadaşın Tickmill hesabını metaapi.cloud'a kendisi bağlar. Sana iki bilgi verir: **token** ve **account id**.
- Sen bu iki bilgiyi `.env` dosyana yazarsın.
- Sen config'e `tickmill` adında bir broker eklersin. Sinyaller ona da gider.

## Güvenlik: bot durmaz

Broker `optional: true` ile eklenir. Bu şu demektir:

- Tickmill bağlanamazsa bot **yine başlar**.
- Binance, Breakout ve FundingPips **aynen çalışır**.
- Sana Telegram'dan **bir kez** uyarı gelir.
- Tickmill için gelen sinyaller **atlanır**. Sonraya bırakılmaz.
- Bot arka planda 5 dakikada bir tekrar bağlanmayı dener.
- Bağlanınca sana haber verir.

**Bot bir kez yeniden başlatılmalıdır.** Config'i değiştirince bot kendiliğinden okumaz.

## Başlamadan önce

1. Arkadaşın `docs/ARKADAS_METAAPI.md` rehberini bitirmiş olmalı.
2. Sana **account id** ve **token** göndermiş olmalı (özel kanaldan).
3. Mevcut `config.yaml` ve `.env` dosyalarının **yedeğini** al.

### Yedek al

Bot klasörüne gir. Klasör yolunu kendi sunucundakine göre değiştir.

```powershell
cd C:\bot\telegram-copy-trader
```

Config yedeği:

```powershell
Copy-Item config.yaml config.yaml.yedek
```

`.env` yedeği:

```powershell
Copy-Item .env .env.yedek
```

## Adım 1: MetaApi paketini kur

Sanal ortamı aç. Satırın başında `(venv)` yazmalı.

```powershell
.\venv\Scripts\Activate.ps1
```

Paketi kur:

```powershell
pip install metaapi-cloud-sdk
```

## Adım 2: `.env` dosyasına iki satır ekle

`.env` dosyasını Not Defteri ile aç:

```powershell
notepad .env
```

Dosyanın **en altına** şu iki satırı ekle. Değerleri arkadaşından gelenlerle değiştir. Tırnak kullanma. Boşluk bırakma.

```
METAAPI_TOKEN=arkadasindan_gelen_token
METAAPI_ACCOUNT_ID=arkadasindan_gelen_account_id
```

Kaydet ve Not Defteri'ni kapat.

Token'ı kimseyle paylaşma. `.env` dosyasını herkese açık yere koyma.

## Adım 3: `config.yaml` içine broker'ı ekle

Önce örnek parçayı yan pencerede aç:

```powershell
notepad config.tickmill.example.yaml
```

Sonra asıl ayar dosyanı aç:

```powershell
notepad config.yaml
```

### 3a. Broker bloğunu ekle

`config.yaml` içinde `brokers:` satırını bul. Listenin **en sonuna** (son brokerın altına) şu bloğu yapıştır. Girinti (soldaki boşluk) aynı kalmalı. Diğer brokerların `- name:` satırlarıyla aynı hizada başlasın.

```yaml
  - name: tickmill
    type: metaapi
    enabled: true
    optional: true
    token: ${METAAPI_TOKEN:-}
    account_id: ${METAAPI_ACCOUNT_ID:-}
    risk_usd: 5
    reference_exchange: binanceusdm
    offset_mode: additive
    max_offset_pct: 1.0
    entry_trigger: reference_touch
    virtual_trigger_max_hours: 48
    symbol_map: {BTC: BTCUSD, ETH: ETHUSD}
    allowed_assets: [BTC, ETH, EUR, GBP, XAU]
    no_hedge: true
    daily_loss_limit_pct: 3
```

Notlar:

- `optional: true` satırı **çok önemli**. Silme.
- `risk_usd: 5` arkadaşın için 1R'de en fazla 5 dolar demektir. Küçük başla.
- `symbol_map` ve `allowed_assets` örnektir. Arkadaşından gelen sembol adlarına göre düzelt.
- `confirm_open: false`, `fees_in_risk: false`, `lot_rounding: up`, `reference_broker: fx` ve
  `size_from_signal_entry: true` satırlarını
  güncelleme (`guncelle.py`, 6. adım) `tickmill` bloğuna kendisi ekler; elle yazmanız gerekmez. Yalnızca blokta
  henüz yoksa yazılır (`lot_rounding: nearest` ise `up` olur); elle yazdığınız başka bir değer kalır.
  - `confirm_open: false`: bot bir sinyalden emin olamayıp "AÇAYIM MI?" diye sorduğunda Tickmill'de işlem
    beklemeden, okunduğu gibi açılır (LIMIT girişte sanal tetik her zamanki gibi). Soru sizin hesaplarınız için
    aynen gelir; Evet derseniz onlar aynı işleme eklenir, Tickmill ikinci kez açılmaz. Hayır derseniz Tickmill'deki
    işlem açık kalır ve normal yönetilir.
  - `reference_broker: fx`: FX konusundaki LIMIT sinyallerde Tickmill'e limit emir hiç konmaz. Sizin fx
    hesabınızdaki giriş emri dolunca Tickmill'de piyasa emriyle açılır; fx'te emir yoksa (onayınızı bekliyor,
    atlandı, reddedildi) fx hesabının kendi fiyatı giriş seviyesine gelince açılır. fx'in fiyatı okunamazsa (bağlı
    değil) bekler ve size uyarı gelir; limit emre dönmez. Kripto ve piyasa emirli sinyaller aynen kalır. Yalnızca
    `fx` etkin bir MetaTrader hesabıysa (type mt5 / mt5_bridge) yazılır.
  - `fees_in_risk: false` ve `lot_rounding: up`: lot stoptaki fiyat kaybına göre hesaplanır (komisyon 1R'nin
    üstüne ödenir) ve yukarı yuvarlanır; stopta kayıp 1R'nin altına düşmez, en çok 10 üstüne çıkar.
  - `size_from_signal_entry: true`: piyasa emrinde lot sinyaldeki giriş fiyatına göre hesaplanır (trader'ın lotu
    gibi: 100 USD'ye 0.94 lot ise 50 USD'ye 0.47). Fiyat girişten kötü dolarsa da lot bunun altına inmez; stopta
    kayıp gerçek girişten en çok 1R + 10 olur (fazlası olacaksa o sınıra kadar küçültülür).

### 3b. Konu yollarına `tickmill` ekle

Aynı dosyada `topics:` bölümünü bul. İki konunun `brokers:` satırına `tickmill` ekle. Başka hiçbir şeyi değiştirme.

Kripto konusu, önceki hali:

```yaml
      brokers: [binance, breakout]
```

Kripto konusu, yeni hali:

```yaml
      brokers: [binance, breakout, tickmill]
```

Fx konusu, önceki hali:

```yaml
      brokers: [fx]
```

Fx konusu, yeni hali:

```yaml
      brokers: [fx, tickmill]
```

Kendi broker adların farklıysa onları olduğu gibi bırak. Sadece sona `tickmill` ekle.

Kaydet ve Not Defteri'ni kapat.

## Adım 4: Ayarı kontrol et

Bu komut işlem açmaz. Ayarları ve bağlantıları kontrol eder.

```powershell
python -m copytrader check
```

Beklenen: hata yok. Tickmill bağlanamazsa orada sebebi yazar. Bu durumda bile bot başlar.

## Adım 5: Botu yeniden başlat

Bot şu an çalışıyorsa önce durdur. Bot penceresinde `Ctrl+C` bas. Servis olarak çalışıyorsa servisi durdurup başlat.

Sonra botu başlat:

```powershell
python -m copytrader run
```

## Ne görmelisin?

### Her şey yolundaysa

- Log'da şuna benzer bir satır olur: `tickmill: MetaApi connected platform=mt5 ...`
- Telegram'daki başlangıç mesajında `Brokerlar:` satırında `tickmill` yazar.
- Ayrı bir uyarı gelmez.

### Bağlanamazsa

Telegram'da şuna benzer bir **uyarı** gelir (bir kez):

> ⚠️ **tickmill** bağlanamadı (isteğe bağlı hesap). Bot ve diğer hesaplar normal çalışıyor; tickmill için gelen sinyaller atlanacak. Arka planda 5 dakikada bir tekrar denenecek.
> Sebep: ...

Başlangıç mesajında `❌ Bağlanamayan: tickmill` yazar. Bot ve diğer brokerlar çalışmaya devam eder.

Sebep satırına göre ne yapılır:

- `metaapi-cloud-sdk paketi kurulu değil`: Adım 1'i yap. Botu yeniden başlatmana gerek yok. Bot 5 dakika içinde kendisi bağlanır.
- `METAAPI_TOKEN ... boş`: `.env` dosyasını kontrol et. Sonra botu yeniden başlat.
- Token veya hesap hatası: Arkadaşından yeni bilgi iste. `.env` dosyasını düzelt. Botu yeniden başlat.
- Hesap deploy edilmemiş: Arkadaşın MetaApi panelinde Deploy yapmalı.
- `işlem izni yok`: Arkadaş yatırımcı şifresi girmiş. İşlem şifresiyle yeniden eklemeli.

### Sonradan bağlanırsa

Telegram'da şu mesaj gelir:

> ✅ **tickmill** bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak (atlanan eski sinyaller açılmaz).

### Bağlı değilken sinyal gelirse

Her sinyal için kısa bir not gelir:

> ℹ️ tickmill bağlı değil, bu işlem atlandı (BTC LONG).

Binance, Breakout ve FundingPips kopyaları **normal açılır**.

## Arkadaşının komutları

Arkadaşın (`/arkadas` ile eklediğin sohbet) bota kendi sohbetinden şu komutları yazabilir. Hepsi yalnızca
kendi hesabı (`tickmill`) içindir; senin hesaplarını görmez. Tam listeyi ona `/yardim` gösterir.

- `/pozisyonlar`, `/bakiye`, `/pozkapat`, `/karne`
- `/tickmillkart`: bu haftaki Tickmill işlemlerinin numaralı listesi. Numara yazınca o işlemin kartı (resim)
  gelir: açık işlemde giriş kartı, kapanmışta sonuç kartı. Liste 5 dakika geçerli, bir kez kullanılır.
  - Kart senin kartlarınla aynıdır (aynı tasarım, R komisyon dahil, zaman dilimi sinyal görselinden ya da 15 dakika).
  - Kartta ve yazılarda başka hesap adı geçmez. `/binancekart` gibi senin komutların ona kapalıdır.
  - Kart yalnızca isteyen sohbete gelir; kendiliğinden gönderilmez. Sen de kendi sohbetinde `/tickmillkart`
    yazabilirsin.
  - Mumlar Tickmill terminalinden okunur (yalnızca `type: mt5_bridge` ile; MetaApi ile bağlıysa kart yapılamaz,
    nedeni yazılır). İşlem yürütülürken okunmaz; o zaman "kartı birazdan tekrar isteyin" yazar.

## Önce küçük dene

`dry_run` ayarı tüm brokerları etkiler. Canlı çalışan botunda bunu değiştirme. Tickmill'i canlıda küçük riskle (`risk_usd: 5`) izlemek daha güvenlidir. Arkadaşın demo hesapla başlarsa daha da güvenli olur.

## Tickmill'i kaldırma

1. Botu durdur (`Ctrl+C`).
2. `config.yaml` dosyasını aç:

```powershell
notepad config.yaml
```

3. `- name: tickmill` ile başlayan bloğu sil (tüm satırlarıyla).
4. İki konudaki `brokers:` satırından `, tickmill` yazısını sil.
5. İstersen `.env` dosyasından `METAAPI_TOKEN` ve `METAAPI_ACCOUNT_ID` satırlarını sil.
6. Botu yeniden başlat:

```powershell
python -m copytrader run
```

Dikkat: Tickmill'de açık işlem varsa, broker config'ten çıkınca bot onu **yönetemez**. Önce açık işlemleri kapat veya arkadaşından MT5'ten kapatmasını iste.

Geçici durdurmak için bloğu silmek yerine `enabled: false` yazabilirsin.

## Hızlı özet

1. Yedek al.
2. `pip install metaapi-cloud-sdk`
3. `.env` içine iki satır ekle.
4. `config.yaml` içine broker bloğunu ve iki `brokers:` satırını ekle.
5. `python -m copytrader check`
6. Botu bir kez yeniden başlat.
