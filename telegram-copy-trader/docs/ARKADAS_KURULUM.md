# Kendi Bilgisayarında Kurulum Rehberi (Tickmill MT5)

Bu rehber, sinyal grubundaki işlemleri **kendi** Tickmill hesabında otomatik açan botu **kendi** bilgisayarında kurmak içindir. Kodlama bilmen gerekmez. Her adım tek komuttur.

## Neden kendi bilgisayarında?

- Şifreni ve hesabını **kimseye vermezsin**. Her şey senin bilgisayarında kalır.
- Başka birinin sunucusunda çalışsa, o kişi hesabına erişebilirdi. Burada kimse erişemez.
- Telegram hesabın, API anahtarların ve MT5 şifren sadece senin `.env` dosyanda durur.
- İstediğin an durdurursun (Ctrl+C).

## Bot ne yapar?

Sinyal grubunu okur. İşlemi anlar. Tickmill hesabında senin risk ayarınla açar. Sinyaller TradingView'da Binance fiyatıyla çizilir (ör. BTCUSDT.P). Tickmill'de fiyat biraz farklıdır (ör. 73400, Binance 73200). Bot bu farkı hesaplar ve giriş, stop ve hedefleri aynı farkla kaydırır. Böylece grafikteki aynı bölgeden işlem alırsın.

## Gerekenler

1. **Windows** bilgisayar (veya Windows VPS). MT5 sadece Windows'ta çalışır. Bot çalışırken bilgisayar açık olmalı.
2. **Python 3.12**. python.org adresinden indir. Kurarken **"Add Python to PATH"** kutusunu işaretle.
3. **MT5** (Tickmill'in sitesinden indir). Kendi Tickmill hesabınla giriş yap. Üstteki **Algo Trading** düğmesi **yeşil** olmalı.
4. **Telegram hesabın**. Sinyal grubunun üyesi olmalı.
5. **Anthropic API anahtarı** (console.anthropic.com). Bu **ücretlidir**. Her sinyal okuması küçük bir ücrettir. Sinyal başına genelde birkaç sent. Hesaba az bir bakiye yükle.
6. **Bildirim botu**. Telegram'da **@BotFather**'a yaz, `/newbot` de. Gelen **token**'ı sakla. Botun sohbetini aç ve **Start**'a bas.
7. **Telegram kullanıcı ID'n**. Telegram'da **@userinfobot**'a yaz. Sana bir sayı verir.
8. **API id ve API hash**. my.telegram.org adresine telefonunla gir. **API development tools** bölümünde bir uygulama oluştur. Ad önemli değil.

## Kurulum adımları

Komutları **PowerShell**'de çalıştır (Başlat menüsünde "PowerShell" yaz).

**1. Zip dosyasını indir.**

```powershell
Invoke-WebRequest -Uri "https://github.com/YekkyBoi/harmonic-bot/archive/refs/heads/claude/telegram-crypto-automation-o6xo7l.zip" -OutFile bot.zip
```

**2. Zip'i aç.**

```powershell
Expand-Archive bot.zip -DestinationPath .
```

**3. Bot klasörüne gir.** (Klasör adı farklıysa `dir` ile bak; içinde `telegram-copy-trader` olan klasörü bul.)

```powershell
cd harmonic-bot-*\telegram-copy-trader
```

**4. Sanal ortam oluştur.**

```powershell
python -m venv venv
```

**5. Sanal ortamı aç.** Satırın başında `(venv)` yazmalı.

```powershell
.\venv\Scripts\Activate.ps1
```

Hata verirse bir kez şunu çalıştırıp tekrar dene:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

**6. Paketleri kur.**

```powershell
pip install -r requirements.txt
```

**7. MT5 paketini kur.**

```powershell
pip install MetaTrader5
```

**8. Ayar dosyasını kopyala.**

```powershell
Copy-Item config.tickmill.example.yaml config.yaml
```

**9. Gizli bilgi dosyasını kopyala.**

```powershell
Copy-Item .env.example .env
```

**10. `.env` dosyasını aç ve doldur.**

```powershell
notepad .env
```

Şunları yaz (`=` işaretinden sonra, boşluksuz):

- `TG_API_ID` ve `TG_API_HASH`: my.telegram.org'dan.
- `TG_BOT_TOKEN`: @BotFather'dan.
- `TG_OWNER_ID`: @userinfobot'tan.
- `ANTHROPIC_API_KEY`: Anthropic'ten.
- `MT5_LOGIN`: Tickmill hesap numaran.
- `MT5_PASSWORD`: Tickmill **işlem** şifren (yatırımcı şifresi değil).
- `MT5_SERVER`: MT5'te gördüğün sunucu adı (ör. Tickmill-Live).

Kaydet ve kapat. **Bu dosyayı kimseyle paylaşma. Ekran görüntüsü alma.**

**11. Telegram'a giriş yap.**

```powershell
python -m copytrader login
```

Ekranda bir **QR kod** çıkar. Telefonundaki Telegram'dan okutman gerekir (Ayarlar, Cihazlar, Cihaz Bağla). Bunun için **ikinci bir ekran** lazım: QR kodu bilgisayar ekranında görürsün, telefonla okutursun. İkinci ekran yoksa (ör. VPS) kodla giriş yap:

```powershell
python -m copytrader login --code
```

**12. Grubun ID'sini bul.**

```powershell
python -m copytrader chats
```

Listede sinyal grubunu bul. `-100` ile başlayan sayıyı kopyala.

**13. Ayar dosyasını aç.**

```powershell
notepad config.yaml
```

Bu yerleri düzenle:

- `source_chats` altına grubun ID'sini yaz.
- `symbol_map`: MT5'te **Market Watch** penceresine bak (Ctrl+M). Kripto adlarını tam olarak orada nasıl yazıyorsa öyle yaz. Örnek: `{BTC: BTCUSD, ETH: ETHUSD}`.
- `allowed_assets`: **Sadece Tickmill'in listelediği coinler** işlem görebilir. Listede olmayan coini silmen yeterli. Bot o sinyali atlar.
- `risk_usd: 10`: Bir işlemde stop olursa kaybedeceğin tutar (dolar). **Küçük başla.**

Kaydet ve kapat.

**14. Her şeyi kontrol et.**

```powershell
python -m copytrader check
```

Hata yoksa devam et. Hata varsa mesajı oku ve ilgili ayarı düzelt.

**15. Botu çalıştır.**

```powershell
python -m copytrader run
```

## Güvenli başlangıç

- `config.yaml` içinde `dry_run: true` yazar. Bu durumda **gerçek emir gitmez**. Birkaç gün böyle izle. Bildirim botundan gelen mesajları oku.
- Her şey doğru görünüyorsa `dry_run: false` yap. Botu durdurup yeniden başlat.
- Küçük `risk_usd` ile başla (ör. 10).
- **İlk gerçek işlemi mutlaka başında izle.** MT5'te giriş, stop ve hedefi kontrol et.
- Bot sana sorarsa (emin olamadığında) bildirim botunda **Onayla** veya **İptal** düğmesine bas.

## Durdurmak

PowerShell penceresinde **Ctrl+C**. Açık işlemlerin stop ve hedefleri MT5'te durmaya devam eder. Bot kapalıyken yeni sinyaller kaçar.

Sonra tekrar çalıştırmak için klasöre gir, sanal ortamı aç (adım 5) ve `python -m copytrader run` yaz.

## Bilmen gerekenler

- **Fiyat farkı:** Bot her işlemde Binance ile Tickmill fiyatını karşılaştırır. Fark %1'den büyükse veya Binance fiyatı alınamazsa işlem **açılmaz** ve sana uyarı gelir. Bu bilerek böyledir.
- **Limit girişler (sanal tetik):** Config'te `entry_trigger: reference_touch` varsa limit girişli sinyalde Tickmill'e bekleyen emir konmaz. Bot Binance fiyatını izler. Fiyat giriş seviyesine değince farkı o an ölçer ve işlemi piyasa emriyle açar. Tetiklenmeden önce stop veya ilk hedef geçilirse ya da 48 saat dolarsa giriş iptal olur. Bot kapanıp açılsa da bekleyen tetik kaybolmaz.
- **Fark değişir:** Fark işlem açıldığı andaki değerle sabitlenir. İşlem boyunca fark değişebilir. Stop ve hedef biraz farklı bölgeye denk gelebilir.
- **Spread:** CFD'de alış ve satış fiyatı arasında fark vardır. Stop spread yüzünden biraz erken çalışabilir.
- **Hafta sonu ve boşluk:** Stop bazen beklenenden kötü fiyattan kapanır. Kayıp 1R'yi aşabilir.
- Bot yatırım tavsiyesi değildir. Her işlemin sorumluluğu sana aittir.

## Sorun olursa

- `python` bulunamadı: Python'u yeniden kur, "Add Python to PATH" işaretle, PowerShell'i kapatıp aç.
- MT5'e bağlanamadı: MT5 açık mı? Hesaba giriş yapılmış mı? **Algo Trading** yeşil mi?
- Sembol bulunamadı: Market Watch'taki adı `symbol_map`'e yaz.
- Telegram giriş hatası: adım 11'i tekrarla.
