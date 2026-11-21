# Vantage hesabını bota ekleme

Bu rehber **bot sahibi** içindir. Sunucuda (Windows VPS) yapılır. Hesap güncellemeyle kendiliğinden eklenir;
`config.yaml` elle değiştirilmez.

Eklenen hesap (`vantage`): 1R = 20 GBP, günlük zarar freni yok, "açayım mı?" diye sormaz, kripto ve fx
konularındaki sinyalleri alır. Limit girişte Vantage'a emir konmaz: Vantage'ın kendi fiyatı giriş seviyesine
gelince piyasa emriyle açılır. Ayrıntı: `docs/KARARLAR.md` bölüm 7.

## 1. Vantage MT5'i kurun

Vantage'ın sitesinden **MetaTrader 5 (Windows)** kurulum dosyasını indirip kurun. Kurulum klasörünü
değiştirmeyin: güncelleme terminali Program Files içinde, adında **Vantage** geçen klasörde arar.

## 2. Giriş yapın

- Terminali açın, Vantage hesabınızla giriş yapın ("şifreyi kaydet" işaretli olsun).
- Üst çubuktaki **Algo Trading** düğmesi yeşil olsun.
- Araçlar > Seçenekler > Uzman Danışmanlar: "Python API" geçen devre dışı bırakma kutusu işaretli olmasın.
- Terminal açık kalsın.

## 3. `.env` dosyasına üç satır ekleyin

Bot klasöründeki `.env` dosyasını Not Defteri ile açın, en alta şu üç satırı ekleyin, kaydedin:

```
VANTAGE_LOGIN=<hesap numaranız>
VANTAGE_PASSWORD=<işlem (ana) şifreniz, yatırımcı şifresi değil>
VANTAGE_SERVER=<sunucu adı, MT5 giriş penceresinde yazdığı gibi>
```

Bu satırları **siz** yazarsınız; sunucudaki Claude `.env` dosyasını açmaz, okumaz.

## 4. Güncelleyin

Her zamanki güncellemeyi yapın (`docs/SUNUCU_CLAUDE.md`). Güncelleme çıktısında şu satır görünmeli:

`vantage: Vantage MT5 hesabı eklendi ...`

Onun yerine **"Vantage eklenmedi: ..."** yazıyorsa nedeni o satırdadır (`.env` satırı eksik, terminal
bulunamadı...). Düzeltip güncellemeyi tekrar çalıştırın. Terminal başka bir klasördeyse:
`python guncelle.py --vantage-terminal "<terminal64.exe yolu>"`.

Bot başlayınca Telegram'daki "🚀 Kopya-trade motoru başladı" mesajının "Brokerlar" satırında `vantage` görünür
(bağlanamadıysa ayrı bir uyarı gelir). `/vantagekart` bu hesabın kartlarını gösterir.

## Bilmeniz gerekenler

- **Bağlanamazsa bot durmaz** (`optional: true`): size bir kez uyarı gelir, diğer hesaplar aynen çalışır, o arada
  gelen sinyaller o an Vantage'da açılmaz; bot arka planda yeniden bağlanmayı dener. Bağlanınca kaçan sinyal
  `/sonradan` ayarına göre ya şartlar uygunsa kendiliğinden açılır (`otomatik`) ya da size sorulur (`sor`);
  soruyu siz cevaplarsınız.
- Hesap **sizindir** (`friend_account: false`): karnenizde ve kartlarınızda görünür, arkadaşınız hiçbir yerde
  görmez.
- Sembol adları farklı olabilir (`EURUSD+`, `EURUSD.r`): bot bunları kendisi bulur. Bulamadığı bir sembol
  olursa o sinyal yalnızca Vantage'da atlanır ve size yazılır.
- `.env` satırlarını sonradan silerseniz bot yine başlar; Vantage terminalde açık olan Vantage hesabına bağlanır.
