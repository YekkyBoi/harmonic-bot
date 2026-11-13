# Sunucudaki Claude Code oturumu: botu güncelleme

Bu sayfa, botun çalıştığı Windows sunucusunda açılan bir Claude Code oturumu içindir. Güncelleme, kimsenin botun
penceresinde Ctrl+C'ye basmasına gerek kalmadan aşağıdaki 5 adımla yapılır. Komutlar PowerShell'de, **bot
klasöründe** (içinde `copytrader`, `config.yaml`, `venv` klasörü olan klasör) sırayla çalıştırılır.

Her kod bloğunu tek bir PowerShell çağrısı olarak çalıştırın (kabuk Git Bash ise blok için
`powershell -NoProfile -Command "..."` kullanın). Kabuk değişkenleri çağrılar arasında korunmaz; bu yüzden aşağıdaki
komutlar değişken taşımaz. 2. ve 3. adım uzun sürebilir (en çok 4 dakika ve 5-10 dakika): bu komutları aracınızın
izin verdiği en uzun süreyle (örneğin 600 sn) çalıştırın ya da arka planda çalıştırıp çıktıyı bir dosyaya yazdırarak
izleyin. Bir güncelleme sürerken ikincisini asla başlatmayın.

## Kurallar

- `.env` dosyasını **asla** okumayın, açmayın, yazdırmayın, kopyalamayın (hesap anahtarları oradadır). Botun
  komutları onu kendileri okur.
- Botu **asla zorla kapatmayın**: `Stop-Process`, `taskkill`, botun penceresini kapatmak yok. Bot yalnızca 2. adımdaki
  `stop` komutuyla durdurulur. O komut başarısız olursa durun ve sahibine haber verin.
- `data\` klasöründeki dosyalara (veritabanı, Telegram oturumu, `bot.lock`) dokunmayın.
- Bir adım beklenmedik bir şey yazarsa ekrandaki çıktıyı olduğu gibi sahibine iletin.

## 1. Yeni sürümü indirin

```powershell
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$Zip = "https://github.com/YekkyBoi/harmonic-bot/archive/refs/heads/claude/telegram-crypto-automation-o6xo7l.zip"
Invoke-WebRequest -Uri $Zip -OutFile "$env:USERPROFILE\Downloads\harmonic-bot-guncel.zip" -UseBasicParsing
```

- Dosya `İndirilenler` (`$env:USERPROFILE\Downloads`) klasörüne inmeli ve adı `harmonic-bot` ile başlayıp `.zip` ile bitmeli:
  güncelleme betiği oradaki en yeni `harmonic-bot*.zip` dosyasını kullanır. Zip'i açmayın.
- İndirme hata verirse (ör. 404: depo gizli olabilir) devam etmeyin; sahibinden zip dosyasını o klasöre koymasını
  isteyin. Bot bu sırada çalışmaya devam eder.

## 2. Çalışan botu durdurun

```powershell
.\venv\Scripts\python.exe -m copytrader stop
$LASTEXITCODE
```

- Bot, Ctrl+C'ye basılmış gibi kendi kapanışını yapar: süren emir işlemlerini bitirir, bağlantıları kapatır, sahibine
  Telegram'dan `🛑 Kopya-trade botu durduruldu.` yazar, penceresinde `Durduruldu.` yazar.
- Komut, bot gerçekten kapanana kadar bekler (en çok 4 dakika) ve arada ne olduğunu yazar.
- `Bot durduruldu.` ya da `Çalışan bir bot bulunamadı` yazıp `$LASTEXITCODE` **0** ise 3. adıma geçin.
- 0 değilse **devam etmeyin** ve botu zorla kapatmayın: ekrandaki çıktıyı ve aşağıdaki kaydın son satırlarını
  (5. adımdaki komut) sahibine iletin.
- `invalid choice: 'stop'` yazarsa kurulu sürüm bu komutu henüz bilmiyordur (bu komut, bu sayfayla aynı sürümde
  geldi): bot bu yolla durdurulamaz. Sahibinden botun penceresinde **bir kez** Ctrl+C'ye basıp `Durduruldu.`
  yazısını beklemesini isteyin, sonra 3. adımla devam edin.

## 3. Güncelleyin (botu başlatmadan)

```powershell
.\venv\Scripts\python.exe guncelle.py --baslatma --bekci-yok
$LASTEXITCODE
```

- Betik 1. adımdaki zip'i bulur, eskiyi yedekler, yeni sürümü kurar, ayarları ve hesap bağlantılarını kontrol eder;
  her adımın sonuna `TAMAM` ya da `HATA` yazar. Birkaç dakika sürebilir.
- `--baslatma`: bot bu pencerede başlatılmaz (4. adımda ayrı bir pencerede başlatılır). `--bekci-yok`: bekçi görevi
  yeniden kurulmaz (yönetici yetkisi ister); daha önce kurulmuş bekçi yerinde kalır.
- 4. adıma (botu başlatmaya) **yalnızca** şu durumlarda geçin:
  - çıkış kodu `0` (yeni sürüm kuruldu);
  - çıkış kodu `1` **ve** çıktıda `ESKİ SÜRÜM GERİ YÜKLENDİ` yazıyor (bir adım başarısız oldu, eski sürüm geri
    geldi; botu başlatın, sonra çıktıyı sahibine iletin);
  - çıkış kodu `2` **ve** çıktıda `bot çalışıyor` geçmiyor (hiçbir şey değiştirilmedi, ör. zip bulunamadı; botu
    başlatın, sonra çıktıyı sahibine iletin).
- Başka her durumda (ör. `2` ile `bot çalışıyor`: bot zaten çalışıyor, ikinci bir bot başlatılırsa sinyaller iki kez
  kopyalanır; `ESKİ SÜRÜM GERİ YÜKLENEMEDİ`; başka bir kod) botu **başlatmayın**; çıktıyı olduğu gibi sahibine
  iletin.

## 4. Botu ayrı bir pencerede başlatın

```powershell
Start-Process powershell -WorkingDirectory (Get-Location).Path -ArgumentList '-NoExit', '-NoProfile', '-Command', '.\venv\Scripts\python.exe -m copytrader run'
```

- Bot yeni açılan pencerede çalışır; o pencere açık kalmalıdır. Bu Claude oturumu kapandıktan sonra da çalışmaya
  devam ettiğini bir kez sahibinden doğrulamasını isteyin (bekçi kuruluysa bot kapanınca 5 dakika içinde haber verir). Bir sonraki güncellemede yine 2. adımdaki `stop`
  komutuyla durdurulur.

## 5. Kontrol edin

Bir-iki dakika bekleyip kaydın son satırlarına bakın:

```powershell
Test-Path .\data\bot.lock
Get-Content .\data\logs\copytrader.log -Tail 40
```

- `Test-Path` **True** olmalı (bot çalışırken bu dosyayı kendisi yazar).
- Kayıtta `Engine started: ... broker(s) connected` ve `Copy trader running (LIVE)` satırları görünmeli; sahibine
  Telegram'dan `🚀 Kopya-trade motoru başladı` mesajı gider.
- `ERROR`, `Traceback` ya da `bağlanamadı` geçen satırları olduğu gibi sahibine iletin. (Kayıtta hesap anahtarları
  `***` ile gizlenir; yine de `.env` okunmaz.)
- Bekçi kuruluysa bot kapalıyken sahibine bir `Bot çalışmıyor` mesajı gidebilir; bot başlayınca
  `Bot tekrar çalışıyor` gelir.
