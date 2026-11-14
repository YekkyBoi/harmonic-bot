# Botu güncelleme (3 adım)

Botun çalıştığı sunucuda (Windows, PowerShell) yapılır. Toplam süre 5-10 dakikadır; bu sürede bot kapalıdır.

## 1. Yeni sürümü indirin

GitHub'da projenin sayfasını tarayıcıda açın, dalı (branch) seçin ve **Code > Download ZIP** ile indirin.
Dosya `İndirilenler` (Downloads) klasörüne `harmonic-bot-....zip` adıyla iner (daha önce indirdiyseniz adı
`... (1).zip` olur; sorun değil, en yeni indirilen kullanılır). Zip dosyasını **açmayın**, olduğu yerde kalsın.

## 2. Botu durdurun

Botun çalıştığı PowerShell sekmesine tıklayın, **Ctrl+C**'ye **bir kez** basın ve `Durduruldu.` yazısını
bekleyin (1-2 dakika sürebilir; ikinci kez basmayın).

## 3. Aşağıdaki satırı aynı sekmeye yapıştırıp Enter'a basın

```
.\venv\Scripts\python.exe -c "import glob,os,runpy,tempfile,zipfile;z=glob.glob(os.path.expanduser('~/Downloads/harmonic-bot*.zip')) or exit('Zip bulunamadi: once 1. adimdaki dosyayi indirin');d=tempfile.mkdtemp();zipfile.ZipFile(max(z,key=os.path.getmtime)).extractall(d);g=glob.glob(d+'/*/telegram-copy-trader/guncelle.py') or exit('Bu zip eski: yenisini indirin');runpy.run_path(g[0],run_name='__main__')"
```

Hepsi bu kadar. Satır şunları kendisi yapar ve her adımın sonuna `TAMAM` ya da `HATA` yazar:

| Adım | Ne yapılır |
|---|---|
| 1 | Doğru klasörde olunduğuna ve indirilen sürümün tam olduğuna bakılır |
| 2 | Bot kapalı mı diye bakılır (açıksa hiçbir şeye dokunulmaz) |
| 3 | Eski sürüm `yedek\<tarih-saat>\` klasörüne yedeklenir; eski kayıt dosyalarındaki hesap anahtarı gizlenir (aşağıda) |
| 4 | Yeni sürüm kurulur |
| 5 | Gerekli paketler değiştiyse kurulur (değişmediyse atlanır) |
| 6 | `config.yaml` yeni sürüme göre güncellenir (eski hali `config.yaml.yedek-<tarih-saat>` olarak saklanır) |
| 7 | Ayarlar ve hesap bağlantıları kontrol edilir |
| 8 | Sağlık kontrolü yapılır: her hesap her varlıkta fiyat alıp miktar hesaplayabiliyor mu; sinyal grubuna üyeler de yazabiliyor mu |
| – | Bekçi kurulur ve Telegram'ınıza bir deneme mesajı gönderir (aşağıda) |
| 9 | Bot aynı pencerede başlatılır |

Bot bundan sonra bu pencerede çalışır; durdurmak için yine **Ctrl+C**.

Dokunulmayanlar: `.env`, `data\` klasöründeki veritabanı ve Telegram oturumu, `venv\`, `config.yaml.yedek*`
dosyalarınız ve kendi yazdığınız dosyalar. (`data\logs\` içindeki kayıt dosyalarında yalnızca aşağıdaki gizleme
yapılır; bekçi de `data\` klasörüne kendi küçük not dosyasını yazar.)

**Eski kayıt dosyaları:** eski sürüm, arkadaşınızın MetaApi hesap anahtarını (token) `data\logs\copytrader.log`
dosyalarına yazabiliyordu. Güncelleme bu dosyalarda anahtar bulursa yerine `***` koyar ve kaç satırda gizlediğini
yazar; yeni sürüm bu tür anahtarları kayda yazmadan önce gizler. Bu dosyaları daha önce birine gönderdiyseniz
MetaApi sitesinde yeni bir token oluşturup ayarlarınızdaki eskisiyle değiştirin.

## Güncelleme `config.yaml` dosyasında neleri değiştirir

Yalnızca aşağıdakiler değişir; her biri zaten istenen değerdeyse dokunulmaz. Dosyadaki diğer her şey (açıklama
satırları, sıralama, `${...}` ile yazılmış anahtarlar) olduğu gibi kalır ve ekranda her değişiklik için bir satır
yazar.

| Ayar | Yeni değer | Anlamı |
|---|---|---|
| `binance` > `daily_loss_limit_pct` | `0` | Binance'te günlük zarar freni kapalı |
| `binance` > `no_hedge` | `true` | Aynı coinde açık işlem varken ters yönlü sinyal atlanır ve size bildirilir |
| `notifier` > `notify_on_execution` | `true` | Açılan ve kapanan her işlem bildirilir |
| `notifier` > `notify_on_ignored` | `true` | İşlem sayılmayan mesajlar da bildirilir |
| `risk` > `max_open_trades` | `25` | Daha düşükse 25'e çıkarılır (daha yüksekse dokunulmaz) |
| `risk` > `allow_min_qty_round_up` | `true` | 1R en küçük emir miktarının altında kalırsa en küçük miktarla açılır |
| `fx` > `daily_loss_include_open_risk` | `true` | Günlük zarar sınırına açık işlemlerin riski de sayılır |
| kripto konusu > `explicit_market_drift` | `follow` | "market" yazan sinyalde fiyat uzaklaşmışsa sormadan devam edilir |
| `tickmill` > `risk_usd` | `50` | Arkadaşınızın hesabında 1R 45 → 50 USD (yalnızca şu an tam 45 ise ve bir kez; sonra elle yazdığınız değer kalır) |
| `tickmill` > `lot_rounding` | `nearest` | Lot en yakın adıma yuvarlanır (0.125 → 0.13); stopta kayıp hedefi %15'ten fazla aşacaksa aşağı yuvarlanır. Sizin hesaplarınız aşağı yuvarlamada kalır |
| `tickmill` > `type` | `mt5_bridge` | Bilgisayarda Tickmill MT5 terminali bulunursa MetaApi yerine o terminal kullanılır (bir kez; bulunamazsa MetaApi'de kalır ve bunu bir satırla yazar) |
| `tickmill` > `terminal_path` | bulunan `terminal64.exe` yolu | Program Files içinde adında "Tickmill" geçen klasördeki terminal |
| `tickmill` > `broker_company` | `Tickmill` | Bot yalnızca Tickmill hesabı açık olan terminale bağlanır |

### Tickmill MT5 terminaline geçiş

Arkadaşınızın hesabı (tickmill) MetaApi bulutu yerine bu bilgisayara kurulan Tickmill MetaTrader 5 terminaliyle
çalışabilir. Sizin yapmanız gereken yalnızca şu: Tickmill'in MT5 terminalini kurun, açın ve arkadaşınızın hesabıyla
giriş yapın (şifre yalnızca terminale yazılır, "şifreyi kaydet" işaretli olsun). FundingPips terminalinizde olduğu
gibi bu terminalde de üst araç çubuğundaki "Algo Trading" düğmesi yeşil olmalı ve Araçlar > Seçenekler > Uzman
Danışmanlar sekmesinde "Python API" geçen devre dışı bırakma kutusu işaretli olmamalı. Sonra her zamanki gibi güncelleyin:
6. adım terminali bulur ve `tickmill` bloğunu ona çevirir. MetaApi ayarları (`token`, `account_id`) blokta kalır;
BU sürümde MetaApi'ye geri dönmek için bloktaki `type: mt5_bridge` satırını `type: metaapi` yapmanız yeterlidir.

Eski bir sürüme dönmek için yalnızca `.\venv\Scripts\python.exe guncelle.py --geri-al` kullanın: kodla birlikte
o zamanki `config.yaml` dosyasını da yedekten geri koyar. Eski kodu elle (yalnızca kod dosyalarını) geri koymayın:
eski sürümler bu güncellemenin eklediği `lot_rounding`, `terminal_path` ve `broker_company` ayarlarını tanımaz ve
bot hiç açılmaz (sizin hesaplarınız dahil). `type: metaapi` yazmak bunu çözmez; elle dönülecekse `config.yaml` da
güncellemeden önceki yedeğiyle (`config.yaml.yedek-...` ya da `yedek\<tarih>\config.yaml`) değiştirilmelidir.
Bu ayarlar Tickmill terminali bulunamasa da eklenir (`risk_usd` ve `lot_rounding`).

Önerilen sıra (sizin fx hesabınız için en güvenlisi): önce Tickmill terminalini KURMADAN bu sürüme güncelleyin ve
botun fx hesabına bağlandığını görün (bot o anda FundingPips terminalini `data\mt5_terminal_fx.json` dosyasına
kaydeder). Tickmill terminalini ondan sonra kurup giriş yapın ve güncellemeyi bir kez daha çalıştırın: 6. adım
terminali o zaman bulur ve `tickmill` bloğunu çevirir. Geçişi, arkadaşınızın tickmill hesabında açık işlem yokken
yapın.

Terminal Program Files dışında bir yere kurulduysa 3. adımdaki satırın sonuna bir boşluk bırakıp
`--tickmill-terminal "C:\...\terminal64.exe"` yazın. MetaApi'de kalmak için `--tickmill-terminal yok` yazın.

Bot, sizin FundingPips terminalinizi (fx) ilk bağlandığı terminal olarak `data\mt5_terminal_fx.json` dosyasına
kaydeder ve sonra hep o terminale bağlanır; başka bir terminale (örneğin Tickmill'inkine) asla bağlanmaz, bağlanmak
zorunda kalırsa bağlantıyı keser ve size yazar.

## Bekçi: bot çalışmıyorsa haber verir

Güncelleme, sunucuya "bekçi" adında küçük bir zamanlanmış görev kurar (`CopytraderBekci`). Bekçi 5 dakikada bir
botun çalışıp çalışmadığına bakar. Bot çalışmıyorsa (sunucu yeniden başlamış, pencere kapanmış, oturum kapatılmış
ya da bot hata verip durmuşsa) botun Telegram hesabından size yazar:

- `⚠️ Bot çalışmıyor ...` bir kez gelir; bot kapalı kaldıkça 30 dakikada bir hatırlatılır.
- `✅ Bot tekrar çalışıyor.` böyle bir uyarıdan sonra bot yeniden çalışmaya başlayınca (birkaç dakika içinde) bir
  kez gelir.

Bilmeniz gerekenler:

- Bekçi yalnızca haber verir, botu kendisi başlatmaz. Mesaj gelince sunucuya bağlanın ve bot klasöründe şunu
  çalıştırın: `.\venv\Scripts\python.exe -m copytrader run`
- Sunucu yeniden başladığında siz oturum açmadan da çalışacak şekilde kurulur. Sunucu tamamen kapalıysa ya da
  interneti yoksa haber veremez. Bot açık ama takılıp kalmışsa bekçi bunu anlamaz.
- Kurulurken (her güncellemede) Telegram'ınıza `Bekçi kuruldu ve çalışıyor` deneme mesajı gelir. Bu mesaj
  gelmezse ya da ekranda `Bekçi kurulamadı` yazarsa bot yine çalışır ama bekçi yoktur; ekran görüntüsünü gönderin.
  Sebep olarak "yönetici yetkisiyle açılmamış" yazıyorsa: PowerShell'e sağ tıklayıp "Yönetici olarak çalıştır"
  ile açın, bot klasörüne gidin ve şunu çalıştırın (botu durdurmanız gerekmez):
  `.\venv\Scripts\python.exe guncelle.py --bekci-kur`
- Güncelleme sırasında bekçi susar. Botu durdurduktan sonra satırı yapıştırmanız birkaç dakika sürerse bir
  `Bot çalışmıyor` mesajı gelebilir; güncelleme bitip bot başlayınca `Bot tekrar çalışıyor` gelir.
- Bot klasörünü başka bir yere taşırsanız yukarıdaki `--bekci-kur` komutunu yeni klasörde bir kez çalıştırın
  (bekçi eski yere bakar).
- Bot penceresinde çalışırken `Bot çalışmıyor` mesajı gelirse bekçi yanılıyordur: aşağıdaki satırla kaldırın ve
  ekran görüntüsünü gönderin (bekçi bu sunucuda ilk kez çalışıyor).
- Botu bilerek uzun süre kapalı tutacaksanız hatırlatmalar sürer. Bekçiyi kaldıran tek satır:

```
schtasks /Delete /TN CopytraderBekci /F
```

Sonraki güncelleme bekçiyi yeniden kurar. Hiç kurulmasını istemiyorsanız 3. adımdaki satırın sonuna bir boşluk
bırakıp `--bekci-yok` yazın.

## Bir adım HATA verirse

- **2. adım HATA (bot çalışıyor):** botu 2. adımdaki gibi durdurun, satırı yeniden yapıştırın.
- **3-7. adımlardan biri HATA:** ekranda `GÜNCELLEME YAPILMADI ... ESKİ SÜRÜM GERİ YÜKLENDİ` yazar. Eski sürüm
  olduğu gibi geri konur ve bot **eski sürümle** başlatılır; hiçbir ayarınız değişmez. Ekranın görüntüsünü
  gönderin; sorun giderilince aynı satırı tekrar yapıştırmanız yeterlidir.
- **7. adım HATA ama `GERİ ALINMADI` yazıyor:** kontrolün bulduğu sorun güncellemeden önce de vardı (örneğin bir
  hesap o an bağlanamıyor). Yeni sürüm kurulu kalır; yazan sorunu ayrıca giderin.
- **8. adım HATA:** güncelleme geri alınmaz, bot yeni sürümle başlar. Ekrandaki `SONUÇ` bölümünde hangi hesapta
  hangi varlığın sorunlu olduğu yazar; ekran görüntüsünü gönderin.
- `Zip bulunamadi` yazarsa 1. adımdaki dosya `İndirilenler` klasöründe değildir; yeniden indirin.
- `BadZipFile` yazarsa indirme yarım kalmıştır; dosyayı yeniden indirip satırı tekrar yapıştırın.
- **Bot kapalı olduğu halde `Bot şu anda çalışıyor` / `bot çalışıyor (... kilit dosyası)` yazarsa:** botun
  hiçbir pencerede çalışmadığından emin olun, sonra ekranda adı yazan `data\bot.lock` dosyasını silip tekrar
  deneyin. (Bu dosyayı bot çalışırken kendisi yazar, kapanırken siler; bekçi de botun çalıştığını bu dosyadan
  anlar.)

Ekranda yazan her şey ayrıca bot klasöründeki `guncelleme_<tarih-saat>.log` dosyasına da kaydedilir.
Satırı iki kez çalıştırmanın zararı yoktur. Sonraki güncellemelerde aynı satır kullanılır; kısa hali de
çalışır: `.\venv\Scripts\python.exe guncelle.py`

## Eski sürüme dönmek (gerekirse)

Her güncelleme, değiştirdiği her şeyi bot klasöründeki `yedek\<tarih-saat>\` klasörüne koyar. Yeni sürümle bir
sorun yaşarsanız bir önceki sürüme tek satırla dönebilirsiniz. Önce botu 2. adımdaki gibi durdurun, sonra aynı
sekmeye şunu yazıp Enter'a basın:

```
.\venv\Scripts\python.exe guncelle.py --geri-al
```

Bu komut son güncellemeden hemen önceki sürümü ve o günkü `config.yaml` dosyasını `yedek\` klasöründen geri
koyar, ayarları ve hesap bağlantılarını kontrol eder ve botu o sürümle başlatır. O andaki (yeni) sürüm de
`yedek\<tarih-saat>-geri\` klasörüne yedeklenir. `.env`, `data\` ve `venv\` klasörüne dokunulmaz. Hangi
tarihteki güncellemenin geri alındığı ekranda yazar; daha da eskiye dönmek için komutu bir kez daha çalıştırın.
Yeni sürüme yeniden geçmek için 3. adımdaki satırı tekrar yapıştırmanız yeterlidir.

Bu güncellemeden önceki sürüm, çalışıp çalışmadığını bekçiye bildirmez: o sürüm kuruluyken bekçi uyarı
göndermez (ekranda da yazar).

## İsteğe bağlı komutlar (bot kapalıyken)

Sağlık kontrolünü tek başına çalıştırmak (hiçbir emir göndermez, Telegram'a hiçbir şey yazmaz; ekran görüntüsü
almak için uygundur):

```
.\venv\Scripts\python.exe -m copytrader selftest
```

**Gerçek** küçük bir deneme işlemi. Güncelleme bunu asla kendiliğinden yapmaz; yalnızca siz çalıştırırsanız
yapılır. Tek bir hesapta açılabilecek **en küçük gerçek işlemi** açar, stop ve kâr al emirlerinin hesapta durduğuna
bakar, stopu bir kez taşır ve işlemi kapatır. Göndermeden önce hesabı, sembolü, miktarı ve en kötü durumdaki kaybı
gösterir; ancak `EVET` yazarsanız devam eder:

```
.\venv\Scripts\python.exe -m copytrader canli-test --broker fx --sembol EURUSD --bekle 120
.\venv\Scripts\python.exe -m copytrader canli-test --broker binance --sembol BTC
.\venv\Scripts\python.exe -m copytrader canli-test --broker tickmill --sembol EURUSD
```

Bilmeniz gerekenler:

- Bu gerçek bir emirdir: maliyeti yaklaşık olarak alış-satış farkı ve komisyon kadardır; deneme sırasında stop
  çalışırsa kaybedilecek tutar onaydan önce ekranda yazar. `EVET` dışında bir şey yazarsanız hiçbir emir gönderilmez.
- `--bekle 120` işlemi kapatmadan önce 120 saniye açık tutar. Prop firma hesabında (`fx`) birkaç saniyede açılıp
  kapanan işlemlerle ilgili kuralın ne olduğunu bilmiyoruz; bu yüzden o hesapta denemeyi `--bekle` ile yapın ve
  emin değilseniz önce firmanın kurallarına bakın.
- `tickmill` arkadaşınızın gerçek hesabıdır; onun haberi olmadan deneme yapmayın.
- Deneme yapılan sembolde o hesapta başka açık pozisyon ya da bekleyen emir **olmamalı**.
- Binance'te deneme, o coin için kaldıraç ve marjin modunu botun ayarına getirir (bot ilk işleminde de aynısını
  yapar).
- Deneme sırasında Ctrl+C'ye basarsanız önce işlem kapatılır, sonra çıkılır.
- Bir adım başarısız olursa ekranda yazar ve işlem yine kapatılır. İşlemin kapandığı doğrulanamazsa (ya da emir
  gönderilirken hata olursa) `DİKKAT ... AÇIK KALMIŞ OLABİLİR` yazısı çıkar: o hesabın kendi ekranından bakın,
  açık işlem ya da bekleyen emir varsa elle kapatın.
- Elle işlem hesabında (ör. `breakout`) deneme yapılmaz, çünkü bot o hesaba emir göndermez.

Bekçinin Telegram'a yazabildiğini tek başına denemek: `.\venv\Scripts\python.exe bekci.py --dene`

Botu güncelleme yapmadan yeniden başlatmak için: `.\venv\Scripts\python.exe -m copytrader run`
