# Telegram Kopya-Trade Botu

Seçtiğiniz Telegram grubunda trader'ın paylaştığı işlemleri okur ve **aynı işlemi** sizin kripto borsalarınızda ve MT4/MT5 hesaplarınızda otomatik açar. **Sadece emin olamadığında** size Telegram'dan yazar: "Şunu anladım, doğru mu?"

> ⚠️ Bu program **gerçek parayla** işlem yapar. Önce `dry_run: true` (deneme) modunda birkaç gün çalıştırın.

**İçindekiler**

1. [Ne yapar?](#ne-yapar)
2. [Örnek: "eth 1R long"](#örnek-eth-1r-long)
3. [Ne zaman size sorar?](#ne-zaman-size-sorar)
4. [Kurulum (11 adım)](#kurulum)
5. [7/24 çalıştırma](#724-çalıştırma)
6. [Bot komutları](#bot-komutları)
7. [Güvenlik ve riskler](#güvenlik-ve-riskler)
8. [MT4 / MT5 notları](#mt4--mt5-notları)
9. [data klasörü](#data-klasörü)
10. [Sorun giderme](#sorun-giderme)

---

## Ne yapar?

1. **Okur:** Kendi Telegram **kullanıcı** hesabınızla yalnızca ayarladığınız grubu dinler. Gruba hiçbir şey yazmaz, mesajları "okundu" yapmaz.
2. **Anlar:** Trader'ın metnini ve ekran görüntüsünü Claude (Anthropic) ile okur: varlık, yön, giriş, stop, hedef(ler), risk (R).
3. **Kontrol eder:** Stop doğru tarafta mı, giriş anlık fiyata yakın mı, R sınırın altında mı, sinyal tekrar mı…
4. **Açar:** Her şey netse tüm etkin hesaplarda aynı anda: giriş + stop + TP emirleri. Stop'suz işlem **asla** açılmaz.
5. **Yönetir:** Sonraki "stopu girişe çektim", "yarısını kapattım", "limit gelmedi iptal" gibi mesajları da uygular.
6. **Sorar:** Emin olamazsa işlem açmaz, bildirim botu size sorar.

"TP1 ✅", "stop oldu" gibi sonuç mesajlarında bir şey yapmaz; stop ve TP emirleri zaten borsada durur.

---

## Örnek: "eth 1R long"

Trader TradingView'da **Long Position** aracını çizip ekran görüntüsünü atar ve **"eth 1R long"** yazar. (Örnek görsel: `examples/eth_long_tradingview.jpg`)

Sağdaki fiyat etiketleri şöyle okunur:

- **2310.60** (gri, gri bölgenin üstü) → **TP** (hedef)
- **2232.18** (yeşil, iki bölgenin sınırı) → **Giriş**
- **2205.11** (kırmızı, kırmızı bölgenin altı) → **Stop**
- **2232.85** (siyah, altında `07:11` sayacı) → **anlık fiyat**, giriş DEĞİL

Anlık fiyat girişe %0.03 uzak (%0.5'ten yakın), bu yüzden **piyasa emri** ile girilir. Uzak olsaydı girişe **limit emir** konurdu (`risk.entry_mode: auto`).

> Bu belgede ondalık ayırıcı noktadır (bot mesajlarındaki gibi): 2232.18

### Miktar nasıl hesaplanır?

**1R = bakiyenin %2'si** (`risk.risk_per_r_pct: 2`). 0.5R = %1, 2R = %4. Stop olursa kaybedilen tutar budur.

```
miktar = (bakiye × %2 × R) ÷ |giriş − stop|
```

Örnek, **1000 USDT** bakiye:

- Risk: 1000 × %2 = **20 USDT**
- Stop mesafesi: 2232.18 − 2205.11 = **27.07**
- Miktar: 20 ÷ 27.07 ≈ 0.7388 → **0.738 ETH** (borsanın adımına *aşağı* yuvarlanır, risk asla aşılmaz)
- Pozisyon büyüklüğü: 0.738 × 2232.18 ≈ **1647 USDT** (~1650)
- Stop olursa: ≈ −20 USDT (−1R). TP olursa: 0.738 × 78.42 ≈ +57.9 USDT (≈ +2.9R)

**Kaldıraç otomatik seçilir:**

- `leverage` (varsayılan 10x) ile başlar → teminat ≈ 1647 ÷ 10 ≈ 165 USDT.
- Üst sınır, tasfiye (likidasyon) fiyatı stopun **ötesinde** kalacak şekilde hesaplanır: `1 ÷ (stop oranı × 1.5 + 0.005)`. Burada stop oranı 27.07 ÷ 2232.18 ≈ 0.0121 → en fazla **43x**.
- Serbest bakiye yetmezse kaldıraç gerektiği kadar artırılır ama bu sınırı ve `max_leverage`'ı (50) asla geçmez. Yine yetmezse işlem açılmaz, size bildirilir.

**Gerçekte küçük farklar olur:**

- **Komisyon dahil** (`include_fees_in_risk: true`): giriş + çıkış komisyonu da 1R'nin içine sayılır. Binance %0.05 komisyonla miktar ≈ **0.682 ETH** olur.
- **Piyasa emrinde** mesafe anlık fiyattan ölçülür (2232.85 − 2205.11 = 27.74), miktar biraz daha küçülür.
- **Bakiye** = hesap değeri (açık K/Z dahil, `use_equity: true`). Her hesap **kendi** bakiyesiyle ayrı hesaplanır.
- Hızlı piyasada **kayma** (slippage) kaybı 1R'nin biraz üstüne çıkarabilir.

**TP'ler** trader'ın yazdığı gibi kullanılır. Birden fazla TP varsa pozisyon eşit bölünür (`multi_tp_split: equal`); trader oran yazdıysa ("TP1'de yarısı") o oran kullanılır. TP sonradan değiştirilirse ("TP2'yi 2400 yaptım") işlemin oranları korunur; gerçekleşmiş hedefler tekrar konmaz, oranlar kalan hedeflere göre yeniden dağıtılır. Oranlar tutarsızsa size sorulur.

---

## Ne zaman size sorar?

Aşağıdakilerden biri olursa işlem **açılmaz**, bot size sorar.

**Düzeltmeden onaylanamayanlar** (sadece ✏️ Düzelt / ❌ İptal çıkar):

- Stop yok → "stopsuz işlem açılmaz"
- Varlık veya yön (long/short) anlaşılamadı
- Stop yanlış tarafta (long'da girişin üstünde, short'ta altında) veya TP yanlış tarafta
- Anlık fiyat stopu zaten geçmiş; piyasa girişinde ilk hedef zaten geçilmiş
- Yönetim mesajı hangi işleme ait belli değil (eşleşen yok ya da birden fazla)
- Sinyal **eksik** (ör. sadece görsel geldi, metin gelmedi) ve 3 dk içinde tamamlanmadı (`telegram.incomplete_timeout_sec`)
- Mesajın işlem talimatı olup olmadığından emin değil

**Onay isteyenler** (✅ Onayla da çıkar):

- Okuma güveni düşük (< %85, `llm.min_confidence`) veya Claude bir şüphe yazdı (ör. "son hane okunamadı")
- Giriş anlık fiyattan %10'dan uzak → yanlış okuma olabilir (`risk.max_entry_distance_pct`)
- R çok yüksek: 2R'den büyük (`risk.max_r_per_trade`). Onaylarsanız yazılan R ile açılır.
- R yazılmamış → normalde 1R kullanılır; `ask_if_r_missing: true` ise sorar
- TP verilmemiş, ya da anlık fiyat alınamadı
- Aynı sinyal tekrar: 15 dk içinde aynı varlık + yön + benzer giriş (`risk.duplicate_window_sec`)
- Aynı varlıkta ters yönde açık işlem var
- Sinyal mesajı **sonradan düzenlendi** (düzenlemeler asla otomatik uygulanmaz; yeni stop/TP önerilir). Zaten uygulanmış bir yönetim mesajı (ör. "yarısını kapattım") düzenlenirse: aynı talimat ise tekrar uygulanmaz (yalnızca kayda geçer); farklıysa ilk talimatın zaten uygulandığı belirtilerek sorulur.
- Belirsiz yönetim: stop uzaklaştırılıyor (risk artar), kapatma oranı yok, dolmuş işleme "iptal" dendi, varlık yazılmadı

**Sormadan atlar, sadece bildirir:** `/durdur` ile duraklatıldıysa, açık işlem sınırı (10) doluysa, o hesapta günlük zarar sınırı (%10) aşıldıysa, bakiye/minimum miktar/teminat yetmiyorsa, emir anında o hesapta fiyat stopu veya ilk hedefi geçmişse.

### Bot mesajı nasıl görünür?

```
🤔 Emin olamadım — şunu anladım, doğru mu?

ETH/USDT 📈 LONG (alış) · vadeli (perp)
➡️ Giriş: piyasa fiyatından (sinyaldeki giriş: 2232.18)
🛑 Stop: 2205.11 (girişten %1.21)
🎯 TP1: 2310.6 ≈2.9R — pozisyonun %100'ü
⚖️ Risk: 1R = bakiyenin %2'si
📝 ETH LONG (piyasa), giriş 2232.18, SL 2205.11,
   TP 2310.6, risk 1R (%2)

💹 Güncel fiyat: 2232.85
📊 Güven: %78

Dikkat:
⚠️ Güven düşük (%78).
⚠️ Stop etiketinin son hanesi tam okunamadı.
🔎 Okunan: Giriş = yeşil etiket 2232.18; SL = kırmızı
   etiket 2205.11; TP = gri etiket 2310.60 …
📨 Kaynak mesaj: #501, #502
⏳ 10 dk içinde yanıt gelmezse işlem açılmaz.

[ ✅ Onayla ]  [ ✏️ Düzelt ]  [ ❌ İptal ]
```

- **✅ Onayla:** İşlem açılır. Açmadan önce fiyat tekrar kontrol edilir; stop veya ilk hedef geçilmişse açılmaz.
- **✏️ Düzelt:** Doğrusunu yazın, ör. `SL 2200, TP 2320` veya `short olacak`. Bot yeniden okur ve **yeni** bir onay gönderir. Onay mesajını yanıtlayarak da yazabilirsiniz.
- **❌ İptal:** İşlem açılmaz.
- **10 dk** içinde yanıt yoksa işlem açılmaz (`notifier.confirm_timeout_sec`).

Varsayılan olarak başarılı işlemler **bildirilmez** (`notify_on_execution: false`); sadece sorular, uyarılar ve hatalar gelir. Bot açılınca bir "başladı" mesajı atar.

---

## Kurulum

**Gerekenler:** 7/24 açık bir bilgisayar veya sunucu (Linux VPS önerilir), Python 3.11+ **veya** Docker, Telegram hesabı, Anthropic API anahtarı (ücretli; her sinyal okuması bir Claude çağrısıdır), borsa API anahtarları.

### 1) Telegram API kimliği

1. https://my.telegram.org → telefonunuzla giriş
2. **API development tools** → bir uygulama oluşturun (ad önemli değil)
3. `api_id` ve `api_hash` değerlerini `.env` dosyasına yazın: `TG_API_ID`, `TG_API_HASH`

### 2) Bildirim botu ve kullanıcı ID'niz

1. Telegram'da **@BotFather** → `/newbot` → ad ve kullanıcı adı verin → gelen **token**'ı `TG_BOT_TOKEN` olarak yazın.
2. Yeni botunuzun sohbetini açıp **Start**'a basın. (Basmazsanız bot size yazamaz.)
3. **@userinfobot**'a bir mesaj atın → gösterdiği **ID** (sayı) sizin ID'nizdir → `TG_OWNER_ID`.

Bot yalnızca bu ID'den gelen komutları ve düğmeleri dinler.

### 3) Anthropic API anahtarı

1. https://console.anthropic.com → **API Keys** → **Create Key**
2. `ANTHROPIC_API_KEY` olarak yazın. Hesaba kredi yükleyin; harcama limiti koymanız önerilir.

### 4) Borsa API anahtarları

Her borsada:

- **Vadeli (futures) işlem izni: AÇIK**
- **Para çekme (withdraw) izni: KAPALI**
- **IP kısıtlaması:** sunucunuzun IP adresi (önerilir)
- USDT'yi **vadeli** cüzdana aktarın (bakiye oradan okunur).
- Pozisyon modu **One-way** (tek yönlü) olsun. Config'teki `position_mode` borsadaki ayarla **aynı** olmalı.

Borsaya göre:

- **Binance** (`binanceusdm`): API Management → "Enable Futures". → `BINANCE_API_KEY`, `BINANCE_API_SECRET`
- **Bybit** (Unified Trading hesabı): anahtarda Contract **Orders** ve **Positions** izinleri. → `BYBIT_API_KEY`, `BYBIT_API_SECRET`
- **OKX**: **Trade** izni; anahtarı oluştururken belirlediğiniz passphrase → `OKX_API_PASSPHRASE`

**Test ağı:** broker ayarında `sandbox: true` → Binance Futures testnet, Bybit testnet, OKX Demo Trading. Test ağı için **ayrı** anahtar oluşturmanız gerekir.

### 5) Programı kurun

**A) Python (venv)** — Linux/macOS:

```
cd telegram-copy-trader
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
cp .env.example .env
chmod 600 .env config.yaml
```

- Debian/Ubuntu'da önce: `sudo apt install python3-venv`
- Windows: `py -m venv .venv` ve `.venv\Scripts\activate`
- MT4/MT5 için MetaApi: `pip install metaapi-cloud-sdk`
- Windows + MT5 terminali: `pip install MetaTrader5`

Sonraki adımlardaki komutları venv açıkken bu klasörde çalıştırın.

**B) Docker:**

```
cd telegram-copy-trader
cp config.example.yaml config.yaml
cp .env.example .env
chmod 600 .env
mkdir -p data
sudo chown -R 1000:1000 data
docker compose build
```

Docker'da her komutun başına şunu ekleyin:

```
docker compose run --rm copytrader <komut>
```

Ör: `docker compose run --rm copytrader python -m copytrader check`

### 6) Telegram'a giriş

```
python -m copytrader login
```

Telefon numarası (+90…), Telegram'dan gelen kod ve varsa iki adımlı doğrulama şifresi istenir. Oturum `data/telegram_user.session` dosyasına kaydedilir; ekrana bir de **oturum metni** yazılır.

- Docker'da `login` komutu oturumu `data/` klasörüne yazar; bu yeterlidir.
- Oturum metnini başka bir makinede kullanmak isterseniz `.env`'de `TG_SESSION_STRING` olarak verin.
- ⚠️ Bu dosya ve metin Telegram hesabınıza **tam erişim** verir. Kimseyle paylaşmayın.

### 7) Sinyal grubunun ID'sini bulun

```
python -m copytrader chats
```

Listeden sinyal grubunun **SOHBET ID**'sini (ör. `-1001234567890`) `config.yaml` içine yazın:

```yaml
telegram:
  source_chats:
    - -1001234567890
```

İsteğe bağlı: sadece trader'ın mesajlarını dinlemek için `allowed_sender_ids: [trader_id]`. (Trader'ın bir mesajını @userinfobot'a iletirseniz ID'sini gösterebilir.)

### 8) config.yaml'ı doldurun

Her ayarın yanında Türkçe açıklama var. En önemlileri:

- `dry_run: true` — şimdilik böyle kalsın
- `telegram.source_chats` — 7. adımdaki ID
- `risk.risk_per_r_pct: 2` — 1R = bakiyenin %2'si
- `brokers` — kullanacağınız hesapta `enabled: true`. Anahtarlar `.env`'de.

Varsayılan olarak sadece **paper** (gerçek fiyatlarla çalışan 10000 USDT'lik sanal hesap) açıktır.

### 9) Kontrol

```
python -m copytrader check
```

Örnek çıktı:

```
Mod: DRY-RUN (gerçek emir gönderilmez)
Risk: 1R = bakiyenin %2'si · kaynak sohbet: [-100…]
Anthropic API anahtarı: ayarlı
Bot token: ayarlı
Sahip kullanıcı ID: 123456789
Telegram kullanıcı oturumu: var
✅ paper (paper): hesap değeri 10,000.00 USDT, …
✅ binance (dry-run:ccxt): hesap değeri 1,000.00 USDT, …

Sorun yok.
```

`check` hiçbir işlem açmaz. Borsa anahtarlarını bakiyeyi okuyarak dener. Telegram oturumu, bot token'ı ve Anthropic anahtarının yalnızca var olup olmadığına bakar; geçerli olup olmadıkları `parse` ve `run` sırasında görülür. Config'te bir şey değiştirdiğinizde tekrar çalıştırın.

### 10) Deneme okuma

```
python -m copytrader parse --text "eth 1R long" \
  --image examples/eth_long_tradingview.jpg
```

Ne anlaşıldığını, otomatik mi açılacağını yoksa size mi sorulacağını ve her hesapta kaç adet/lot açılacağını gösterir. **Emir göndermez.**

### 11) Çalıştırın

```
python -m copytrader run
```

(Docker: `docker compose up -d`, loglar: `docker compose logs -f`)

**Birkaç gün `dry_run: true` ile izleyin.** Gerçek hesap ekliyseniz bakiye ve fiyatlar gerçek hesaptan okunur, emirler **simüle** edilir. Her sinyalde bot doğru mu anladı, `/pozisyonlar` ile bakın.

**Canlıya geçiş:**

1. Botta `/hepsini_kapat` ile deneme işlemlerini kapatın.
2. Botu durdurun, `config.yaml`'da `dry_run: false` yapın. Gerçek hesap `enabled: true` olsun; `paper`'ı isterseniz `enabled: false` yapın.
3. Küçük başlayın: düşük bakiye veya `risk_per_r_pct: 0.5`.
4. `python -m copytrader check` → botu başlatın.
5. Başlangıç mesajında **🔴 CANLI** yazdığını görün.

---

## 7/24 çalıştırma

### Docker

```
docker compose up -d          # başlat
docker compose logs -f        # canlı log
docker compose restart        # yeniden başlat
docker compose down           # durdur
```

`restart: unless-stopped` sayesinde sunucu yeniden açılınca bot da açılır.

`config.yaml` veya `.env` değiştirdikten sonra `restart` yetmeyebilir; şunu kullanın:

```
docker compose up -d --force-recreate
```

> `login` ve `chats` komutlarını bot **çalışırken** aynı oturumla çalıştırmayın; önce `docker compose stop`. (`check` ve `parse` Telegram'a bağlanmaz, her zaman çalıştırılabilir.)

### systemd (venv kurulumu)

`deploy/copytrader.service` dosyası `/opt/telegram-copy-trader` klasörünü ve `copytrader` kullanıcısını varsayar. Proje klasöründeyken:

```
sudo useradd --system \
  --home-dir /opt/telegram-copy-trader \
  --shell /usr/sbin/nologin copytrader
sudo mkdir -p /opt/telegram-copy-trader
sudo cp -a ./. /opt/telegram-copy-trader/
sudo chown -R copytrader:copytrader \
  /opt/telegram-copy-trader
cd /opt/telegram-copy-trader
sudo -u copytrader python3 -m venv --clear .venv
sudo -u copytrader .venv/bin/pip install \
  -r requirements.txt
sudo -u copytrader .venv/bin/python \
  -m copytrader login
sudo -u copytrader .venv/bin/python \
  -m copytrader check
sudo cp deploy/copytrader.service \
  /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now copytrader
journalctl -u copytrader -f
```

- Daha önce `login` yaptıysanız oturum `data/` ile birlikte kopyalanır; tekrar `login` gerekmez. Eski klasörde botu artık çalıştırmayın.
- Ayar hatası veya Telegram girişi sorununda (çıkış kodu 2) servis kendini yeniden başlatmaz; sorunu düzeltip `sudo systemctl restart copytrader` yapın.

### Güncelleme

Botu durdurun → yeni dosyaları kopyalayın → `pip install -r requirements.txt` (Docker: `docker compose build`) → başlatın. `data/`, `.env` ve `config.yaml` korunur.

---

## Bot komutları

Bildirim botuna yazın:

| Komut | Ne yapar |
|---|---|
| `/durum` | Mod, hesaplar, aktif işlem ve bekleyen onay sayısı |
| `/pozisyonlar` | Açık işlemler: hesap bazında miktar, giriş, stop, ~K/Z |
| `/bakiye` | Her hesabın bakiyesi |
| `/durdur` | Yeni işlem açmayı durdurur (açık işlemler yönetilmeye devam eder) |
| `/devam` | Yeniden kopyalamaya başlar |
| `/hepsini_kapat` | Tüm pozisyonları kapatır, bekleyen emirleri iptal eder (2 dk içinde "Evet" ister) |
| `/test <metin>` | Metnin nasıl anlaşılacağını gösterir, işlem açmaz (görsel desteklemez) |
| `/yardim` | Komut listesi |

İngilizce karşılıkları da çalışır: `/status`, `/positions`, `/balance`, `/pause`, `/resume`, `/closeall`, `/help`.

`/durdur` durumu yeniden başlatmada da korunur.

---

## Güvenlik ve riskler

**Telegram oturumu = hesabınızın tamamı.** `data/telegram_user.session` dosyasına veya `TG_SESSION_STRING` metnine sahip olan biri tüm sohbetlerinizi okuyabilir ve sizin adınıza yazabilir.

- `data/` ve `.env`'i kimseyle paylaşmayın, git'e eklemeyin, ekran görüntüsü almayın.
- Dosya izinleri: `chmod 600 .env` ve `chmod 700 data`. venv kurulumunda `config.yaml` da 600 olabilir; Docker'da `config.yaml` konteyner tarafından okunabilmeli (içinde gizli bilgi yok).
- Telegram'da iki adımlı doğrulama açık olsun.
- Sızdığını düşünürseniz: Telegram → Ayarlar → Cihazlar → bu oturumu sonlandırın, sonra yeniden `login`.
- Program bu hesapla yalnızca kaynak grubu **okur**; hiçbir yere yazmaz. Yine de Telegram kullanıcı hesaplarındaki otomasyonu kısıtlayabilir; risk sıfır değildir.

**Para riskleri:**

- **Yanlış okuma:** Claude bir rakamı yanlış okuyabilir. Kontroller birçok hatayı yakalar ama hepsini yakalayamaz (ör. doğru tarafta kalan yanlış bir stop). Bu yüzden önce dry-run.
- **Kayma ve boşluk:** Stop, piyasa emriyle kapanır. Hızlı piyasada veya hafta sonu açılışında (CFD) kayıp 1R'yi aşabilir.
- **Borsa/broker kesintisi:** Bir hesapta emir başarısız olup diğerinde açılabilir. Hata mesajı gelir; o hesabı elle kontrol edin.
- **Limit girişler:** Binance'te ve birden fazla TP'li işlemlerde stop, limit giriş **dolduktan sonra** bot tarafından konur (normalde ~10 sn içinde). Bekleyen limit emir varken botu uzun süre kapalı tutmayın.
- **Bot kapalıyken:** Yeni sinyaller kaçar (açılışta sadece son 2 dk'daki mesajlar işlenir). Trader "kapattım" derse siz kapatmazsınız. Açık pozisyonların stop/TP emirleri borsada durmaya devam eder.
- **Elle işlem:** Tek yönlü (one-way) modda aynı sembolde elle işlem açmayın; pozisyonlar birleşir ve botun emirleri sizin pozisyonunuzu da kapatabilir.
- **Hesap kapatma:** Açık işlemi olan bir hesabı `enabled: false` yapmadan veya dry-run'dan canlıya geçmeden önce `/hepsini_kapat` kullanın. Aksi halde eski işlemler "açık" görünmeye devam eder ve sınırlara sayılır.
- **API anahtarları:** Para çekme kapalı, IP kısıtlı olsun.
- **Küçük başlayın:** Önce testnet/sandbox veya paper, sonra küçük bakiye ve düşük risk.

Bu program yatırım tavsiyesi değildir; açılan her işlemin sorumluluğu sizindir.

---

## MT4 / MT5 notları

İki yol var:

- **`type: mt5`** — MetaTrader 5 terminali ile, **sadece Windows**. `pip install MetaTrader5`, terminal kurulu olmalı, **Algo Trading** düğmesi açık olmalı, **ana (master) şifre** gerekir (yatırımcı şifresi işlem açamaz). Bir program kopyasında tek MT5 hesabı kullanılabilir.
- **`type: metaapi`** — MT4/MT5 hesabınız MetaApi bulutunda çalışır; **Linux ve Docker için** bu yol. metaapi.cloud'da hesabınızı ekleyin, **token** ve **account id**'yi `.env`'e yazın. MetaApi üçüncü taraf, ücretli olabilen bir hizmettir; MT şifreniz orada saklanır.

Her ikisi de `config.example.yaml` içinde **yorum** olarak duruyor: satır başlarındaki `# ` işaretlerini kaldırıp `.env`'i doldurun, sonra `check`.

Bilinmesi gerekenler:

- **Sembol:** Bot önce `ETHUSD`, `ETHUSDT` gibi adları ve `ETHUSD.m` gibi ekli adları dener. Altın sinyali (XAU) için brokerınız `GOLD` diyorsa: `symbol_map: {XAU: GOLD}`. Hangi sembolün seçildiğini `parse` ile görün.
- **Lot:** Brokerın kendi kâr/zarar hesabıyla, 1R hesap para biriminde olacak şekilde hesaplanır. Komisyon önceden bilinmez.
- **Birden fazla TP:** MT pozisyonunda tek TP olabildiği için her TP ayrı pozisyon olarak açılır, hepsinde aynı stop. Lot bölünemeyecek kadar küçükse seviyeler birleştirilir. **Netting** hesaplarda tek pozisyon açılır; ara TP'leri bot kısmi kapatma ile uygular (bot açık olmalı).
- **Fiyat farkı:** Brokerınızın ETHUSD fiyatı Binance ETHUSDT.P'den farklı olabilir; sinyaldeki seviyeler olduğu gibi kullanılır.
- **Piyasa saatleri:** CFD'ler hafta sonu kapalıdır; brokerın minimum stop mesafesi (stops level) çok yakın stopları reddeder.

---

## data klasörü

| Dosya | İçerik |
|---|---|
| `telegram_user.session` | Telegram hesabınızın oturumu (**en gizli**) |
| `bot.session` | Bildirim botunun oturumu |
| `copytrader.db` | İşlemler, onaylar, mesajlar |
| `media/` | İndirilen sinyal görselleri |
| `logs/copytrader.log` | Loglar (10 MB × 5 dosya) |
| `paper_<ad>.json` | Sanal hesap bakiyesi ve pozisyonları |
| `dryrun_<ad>.json` | Dry-run simülasyon durumu |

Yedek alırken `data/`, `.env` ve `config.yaml`'ı birlikte alın ve gizli saklayın. `media/` zamanla büyür; bir haftadan eski görselleri silebilirsiniz.

---

## Sorun giderme

- **`environment variable X is not set`**
  → `.env`'de `X` boş. Doldurun. (Sadece `${X:-}` yazan alanlar boş bırakılabilir.)
- **`Yapılandırma hatalı … Input should be a valid string`**
  → Şifre/passphrase sadece rakamdan oluşuyorsa olur. `.env`'de tek tırnak içinde çift tırnakla yazın: `MT5_PASSWORD='"12345678"'`. Aynısını `#` ile başlayan veya boşluk içeren değerler için de yapın.
- **Bot bana hiç yazmıyor**
  → Botun sohbetinde **Start**'a bastınız mı? `TG_OWNER_ID` sizin ID'niz mi (botun değil)? Logda `Cannot message the owner` arayın.
- **`Telegram kullanıcı oturumu yok veya geçersiz`**
  → `python -m copytrader login` ile tekrar giriş yapın. Telegram → Cihazlar'dan oturumu sonlandırdıysanız bu gerekir. Aynı oturumu aynı anda iki yerde çalıştırmayın; Telegram oturumu iptal edebilir.
- **`Kaynak sohbet bulunamadı`**
  → ID yanlış ya da hesabınız o grupta değil. `python -m copytrader chats` ile tekrar bakın.
- **`bot.session başka bir hesaba ait`**
  → Bot token'ını değiştirdiniz. `data/bot.session` dosyasını silip yeniden başlatın.
- **`… bağlanılamadı` (borsa)**
  → API anahtarı/izinleri, IP kısıtlaması, `sandbox` ayarı ve sunucu saatini (`timedatectl`) kontrol edin. Sonra `check`.
- **`… minimum emir miktarının altında`**
  → Bakiye küçük veya stop çok uzak, 1R minimum miktara yetmiyor. İşlem atlanır. `allow_min_qty_round_up: true` minimum miktarla açar (risk 2R'ye kadar çıkabilir).
- **`Yetersiz teminat`**
  → Serbest bakiye yetmiyor. Bakiyeyi, `max_leverage` ve `max_margin_usage_pct` ayarlarını kontrol edin.
- **`PermissionError: … 'data/logs'` (Docker)**
  → `data` klasörü konteyner kullanıcısına ait değil: `sudo chown -R 1000:1000 data`
- **`MetaTrader5 Python paketi yalnızca Windows'ta çalışır`**
  → Linux/Docker'da `type: metaapi` kullanın.
- **Bot kapalıyken gelen sinyal açılmadı**
  → Açılışta yalnızca son 2 dk içindeki mesajlar işlenir (`catchup_max_age_sec`). Daha eskiler bilerek açılmaz (fiyat kaçmış olabilir).
- **Paper bakiyesini sıfırlamak**
  → Botu durdurun, `data/paper_paper.json` dosyasını silin, başlatın.
- **Loglar nerede?**
  → `data/logs/copytrader.log` · Docker: `docker compose logs -f` · systemd: `journalctl -u copytrader -f`. Daha fazla ayrıntı için `log_level: DEBUG`.
