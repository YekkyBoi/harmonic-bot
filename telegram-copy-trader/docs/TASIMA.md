# Botu yeni bir sunucuya taşıma

Bot, ayarları, veritabanı ve Telegram oturumuyla birlikte tek bir dosyaya konur, o dosya yeni sunucuya
kopyalanır ve orada tek komutla kurulur. Toplam süre 30-60 dakikadır (çoğu MetaTrader terminallerini kurmak);
bu sürede bot kapalıdır, sinyaller kopyalanmaz.

## Önce bilin

- **İki sunucuda aynı anda bot çalışmamalı.** İki bot aynı hesaplarda her işlemi iki kez açar ve aynı Telegram
  oturumunu iki yerden kullanır. Eski sunucudaki bot, yenisi başlamadan **önce** ve **bir daha açılmamak üzere**
  durdurulur.
- **Paket dosyası gizli bilgi taşır:** hesap şifreleri ve anahtarları (`.env`), Telegram oturumu, veritabanı.
  Yalnızca Uzak Masaüstü'nde (RDP) kopyala-yapıştır ile doğrudan yeni sunucuya kopyalayın. Hiçbir siteye
  yüklemeyin, e-postayla, Telegram'dan, WhatsApp'tan ya da sohbette (Claude dahil) göndermeyin.
- Telegram oturumu paketle birlikte taşınır: yeni sunucuda yeniden giriş (QR kod) gerekmemesi beklenir. Telegram
  yeni bir yerden girişi fark edip size "yeni giriş" bildirimi gönderebilir. Oturum yine de kapanırsa yeni sunucuda
  bot klasöründe `.\venv\Scripts\python.exe -m copytrader login` ile yeniden giriş yapılır.

## A. Eski sunucuda

**1. Botu son sürüme güncelleyin** (`docs/GUNCELLEME.md`). Bot klasöründe `tasi.py` dosyası yoksa bu adım
yapılmamıştır.

**2. Botu durdurun.** Botun penceresinde **bir kez** Ctrl+C'ye basıp `Durduruldu.` yazısını bekleyin, ya da bot
klasöründe şunu çalıştırın:

```
.\venv\Scripts\python.exe -m copytrader stop
```

**3. Paketi yapın** (bot klasöründe, aynı PowerShell penceresinde):

```
.\venv\Scripts\python.exe tasi.py paketle
```

Bot çalışıyorsa hiçbir şey yapılmaz ve nasıl durdurulacağı yazar. Kurulu sürüm eskiyse "Önce güncelleyin"
yazar. Bitince ekranda paketin yeri yazar:
bot klasöründe `copytrader-tasima-<tarih>-<saat>.zip`. İçinde `venv`, `yedek` (eski sürümlerin yedekleri) ve 7
günden eski kayıt dosyaları yoktur; yedekleri de almak için komutun sonuna ` --yedekli` ekleyin.

Paket bitince eski sunucuya `data\TASINDI.txt` dosyası yazılır. Bu dosya durdukça bot bu sunucuda başlamaz
(`copytrader run` da güncelleme de botu başlatmaz, nedenini yazar). Bu dosya pakete girmez; yeni sunucuda bot
normal başlar.

**4. Eski sunucunun bekçisini kaldırın** (yoksa 30 dakikada bir "Bot çalışmıyor" yazar). PowerShell'i "Yönetici
olarak çalıştır" ile açıp:

```
schtasks /Delete /TN CopytraderBekci /F
```

**5. İki dosyayı kopyalayın:** bot klasöründeki `copytrader-tasima-....zip` ve `tasi.py`. Uzak Masaüstü'nde
seçip Ctrl+C, yeni sunucunun Masaüstü'nde Ctrl+V.

Bu sunucuda botu **bir daha başlatmayın** (geri dönüş için C bölümüne bakın).

## B. Yeni sunucuda

**1. Python 3.12'yi kurun:** python.org > Downloads > Windows > "Windows installer (64-bit)". Kurulumun ilk
ekranında **"Add python.exe to PATH"** kutusunu işaretleyin.

**2. Üç MetaTrader 5 terminalini kurun ve giriş yapın:** FundingPips (`fx`), BEM (`fx2`) ve arkadaşınızın
Tickmill terminali (`tickmill`). Hangi hesabın hangi terminali istediğini şu komut gösterir (3. adımdan sonra,
Masaüstü'nde): `py -3.12 tasi.py kontrol .\copytrader-tasima-....zip`

- Mümkünse eski sunucudaki kurulum klasörlerini kullanın (kontrol komutu eski yolları yazar). Başka bir klasöre
  kurulursa kurulum onu Program Files içinde arar ve sorar.
- Her terminali açın, hesabınızla giriş yapın ("şifreyi kaydet" işaretli olsun), üst çubuktaki **Algo Trading**
  düğmesi yeşil olsun, Araçlar > Seçenekler > Uzman Danışmanlar sekmesinde "Python API" geçen devre dışı bırakma
  kutusu işaretli olmasın. Terminaller açık kalsın.

**3. Paketi ve `tasi.py`'yi Masaüstü'ne kopyalayın** (A.5).

**4. Kurun.** PowerShell'i **"Yönetici olarak çalıştır"** ile açın (bekçi için gerekir) ve:

```
cd $env:USERPROFILE\Desktop
py -3.12 tasi.py kur .\copytrader-tasima-20261119-1530.zip
```

(Dosya adını kendi paketinizinkiyle değiştirin; `py` yoksa `python` yazın.)

- Önce botun kurulacağı klasör sorulur: eski sunucudaki yol bu sunucuda varsa o, yoksa `C:\copytrader`.
  Uygunsa Enter'a basın.
- Bir terminal eski yerinde değil de başka bir klasörde bulunursa sorulur; doğruysa `EVET` yazın. Yalnızca o
  hesabın terminal yolu değişir (`config.yaml`'da `terminal_path` satırı ya da `data\mt5_terminal_fx.json`); eski
  hali `.yedek-<tarih>` adıyla yanında kalır.

Her adımın sonunda `TAMAM` ya da `SORUN` yazar, en sonda bir özet gelir:

| Adım | Ne yapılır |
|---|---|
| 1 | Paket kontrol edilir: her dosya listeyle karşılaştırılır (bozuk ya da değişmişse hiçbir şey yapılmaz) |
| 2 | Bot klasörü seçilir (içinde başka bir bot varsa durur; aşağıya bakın) |
| 3 | Dosyalar açılır ve tek tek kontrol edilir |
| 4 | Botun Python ortamı (`venv`) oluşturulur |
| 5 | Gerekli paketler kurulur (MetaTrader 5 paketi dahil; internet gerekir) |
| 6 | Bot programı denenir |
| 7 | MetaTrader 5 terminallerine bakılır; eksik varsa listelenir ve durulur |
| 8 | Ayarlar ve hesap bağlantıları kontrol edilir; sorun varsa bot başlatılmaz (yalnızca isteğe bağlı bir hesap, ör. tickmill, bağlanamadıysa SORUN yazar ama bot başlatılır) |
| 9 | Sağlık kontrolü (sorun bulursa yazar, bot yine başlatılır) |
| 10 | Bekçi kurulur, Telegram'a deneme mesajı gelir |
| 11 | Bot **ayrı bir pencerede** başlatılır ve çalıştığı doğrulanır |

**5. Kontrol edin:** Telegram'a `🚀 Kopya-trade motoru başladı` mesajı gelmeli (bekçi kurulduysa önce
`Bekçi kuruldu ve çalışıyor`). Açılan bot penceresini kapatmayın.

### Bir adım SORUN yazarsa

Ekrandaki "Ne yapmalı" satırını yapın ve **aynı komutu tekrar çalıştırın**: dosyalar ikinci kez açılmaz, kalan
adımlardan devam edilir.

- **1. adım:** paket bozuk ya da yarım kopyalanmış. Eski sunucudan yeniden kopyalayın.
- **2. adım (klasör dolu):** başka bir klasör seçin ya da komutun sonuna ` --uzerine-yaz` ekleyin (o klasör
  silinmez, adı `<ad>.eski-<tarih>` olur).
- **5. adım:** internet bağlantısına bakıp tekrar çalıştırın.
- **7. adım:** listede yazan terminali kurun, açın, hesapla giriş yapın; sonra tekrar çalıştırın.
- **8. adım:** `❌` ile başlayan satırdaki hesaba bakın (terminal açık mı, hesaba giriş yapılmış mı); tekrar
  çalıştırın.
- **10. adım (bekçi):** isteğe bağlı. Pencere yönetici olarak açılmamışsa sonra yönetici PowerShell'inde bot
  klasöründe `.\venv\Scripts\python.exe guncelle.py --bekci-kur` çalıştırın.
- **11. adım:** açılan pencerede yazanların ekran görüntüsünü gönderin.

Ekranda yazan her şey bot klasöründeki `tasima_<tarih-saat>.log` dosyasına da yazılır.

**Çıkış kodları** (otomatik kullanım için): `0` tamam; `1` elle yapılacak bir şey var (ekranda yazar), bot
başlatılmadı; `2` hiçbir şey yapılmadı (bot çalışıyor, paket bozuk, klasör dolu, yanlış komut); `3` kuruldu ve
bot başlatıldı (`--baslatma` ile başlatılmadı), ama bazı adımlar SORUN yazıyor (sağlık kontrolü, bağlanamayan
isteğe bağlı hesap, bekçi): özetteki o satırlara bakın; `130` Ctrl+C ile durduruldu.

Diğer seçenekler: `--hedef "C:\klasör"` (klasörü sormadan kullan), `--evet` (soruları evet diye yanıtla),
`--baslatma` (botu başlatma), `--bekci-yok` (bekçiyi kurma).

## C. Taşımadan sonra eski sunucu

- Eski sunucuda botu **bir daha başlatmayın**. Bekçisini A.4'teki gibi kaldırın.
- Yeni sunucuda bot birkaç gün sorunsuz çalıştıktan sonra eski sunucudaki bot klasörünü silin (en azından
  `.env`, `data\` klasörü ve paket dosyası) ya da sunucuyu kapatın.
- Paket dosyasını (`copytrader-tasima-....zip`) **iki sunucudan da** silin; Geri Dönüşüm Kutusu'nu da boşaltın.
- **Geri dönüş:** yeni sunucuda bot hiç başlamadıysa eski sunucuda botu yeniden başlatabilirsiniz: önce
  `data\TASINDI.txt` dosyasını silin, sonra `.\venv\Scripts\python.exe -m copytrader run` (bekçi için
  `.\venv\Scripts\python.exe guncelle.py --bekci-kur`).
  Yeni sunucuda bot bir kez çalıştıysa eskisine dönmeyin: eski veritabanında o arada açılan işlemler yoktur.
  Önce yeni sunucudaki botu durdurun ve ekran görüntüsünü gönderin.

## D. Yeni sunucudaki Claude Code oturumu için

Bu bölüm, yeni sunucuda kurulumu yapan bir Claude Code oturumu içindir. Komutlar PowerShell'de, paketin ve
`tasi.py`'nin bulunduğu klasörde çalıştırılır; her blok tek bir PowerShell çağrısıdır. `kur` uzun sürebilir (pip
ve kontroller: 10-20 dakika): aracınızın izin verdiği en uzun süreyle (ör. 600 sn) çalıştırın ya da arka planda
çalıştırıp çıktıyı bir dosyaya yazdırarak izleyin.

### Kurallar

- `.env` dosyasını **asla** okumayın, açmayın, yazdırmayın, kopyalamayın. Paketin içini de açıp okumayın
  (`kontrol` komutu yeterlidir).
- `config.yaml` dosyasını ekrana yazdırmayın; hesap numaralarını, bakiyeleri ve sahibin Telegram kimliğini
  sahibine iletirken bile tekrar etmeyin (check çıktısında bunlar yazar: yalnızca `✅` / `❌` satırlarının
  hesap adını ve sebebini aktarın).
- Paket dosyasını hiçbir yere yüklemeyin, göndermeyin, kopyalamayın (yeni sunucudaki yerinde kalır).
- Botu **asla zorla kapatmayın** (`Stop-Process`, `taskkill`, pencereyi kapatmak yok). Durdurmak gerekirse bot
  klasöründe `.\venv\Scripts\python.exe -m copytrader stop`; başarısız olursa durun ve sahibine haber verin.
- `data\` klasöründeki dosyalara dokunmayın.
- Eski sunucuda bot kapalı değilse (sahibi A bölümünü bitirmediyse) yeni sunucuda botu başlatmayın.
- MetaTrader terminallerini sahibi kendisi kurar ve giriş yapar; şifre hiçbir zaman sizden geçmez.

### 1. Paketi kontrol edin

```powershell
py -3.12 tasi.py kontrol .\copytrader-tasima-20261119-1530.zip
$LASTEXITCODE
```

`0` değilse devam etmeyin; paket yeniden kopyalanmalı.

### 2. Botu başlatmadan kurun

```powershell
py -3.12 tasi.py kur .\copytrader-tasima-20261119-1530.zip --hedef "C:\copytrader" --baslatma --bekci-yok
$LASTEXITCODE
```

- `0`: kuruldu, kontroller geçti; 3. adıma geçin.
- `3`: kuruldu, ama özette SORUN satırları var (sağlık kontrolü ya da bağlanamayan isteğe bağlı hesap, ör.
  tickmill). Bu satırları olduğu gibi sahibine iletin; sahibi isterse 3. adıma geçin.
- `1`: ekrandaki SORUN satırlarını sahibine iletin. 7. adım bir terminalin başka bir klasörde bulunduğunu ve
  onay verilmediğini yazıyorsa sahibine sorun; "evet" derse aynı komutu sonuna ` --evet` ekleyerek tekrar
  çalıştırın. Eksik terminal yazıyorsa sahibi kurup giriş yapınca komutu tekrar çalıştırın.
- `2`: hiçbir şey yapılmadı; çıktıyı olduğu gibi iletin (`bot çalışıyor` yazıyorsa ikinci bir bot
  başlatmayın).

### 3. Botu ayrı bir pencerede başlatın

Sahibi eski sunucudaki botun durduğunu doğruladıktan sonra:

```powershell
Start-Process powershell -WorkingDirectory "C:\copytrader" -ArgumentList '-NoExit', '-NoProfile', '-Command', '.\venv\Scripts\python.exe -m copytrader run'
```

Sonra `docs/SUNUCU_CLAUDE.md` dosyasının 5. adımındaki gibi kontrol edin (`Test-Path .\data\bot.lock` True,
kayıtta `Copy trader running (LIVE)`, sahibine `🚀 Kopya-trade motoru başladı`). Bekçiyi sahibi yönetici
PowerShell'inde bot klasöründe `.\venv\Scripts\python.exe guncelle.py --bekci-kur` ile kurar.
