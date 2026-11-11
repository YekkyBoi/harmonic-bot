# Arkadaş Rehberi: Tickmill Hesabını MetaApi'ye Bağlama

Bu rehber **sadece senin yapacaklarını** anlatır.

- Telegram'a girmen gerekmez.
- Bot kurman gerekmez.
- Bilgisayarında program çalıştırman gerekmez.

Botu **hesabı veren kişi** kendi sunucusunda çalıştırır. Sen sadece Tickmill hesabını metaapi.cloud'a bağlarsın. Sonra iki bilgiyi ona gönderirsin.

## Önce riskleri oku

1. Bot senin hesabında **gerçek para** ile işlem açar.
2. Zarar edebilirsin. Hiçbir işlem garanti değildir.
3. Sadece kaybetmeyi göze aldığın parayla başla.
4. İşlemleri sen değil sinyal grubundaki kişi seçer. Bot onu kopyalar.
5. Emir, internet kesintisi veya servis arızası yüzünden gecikebilir veya hiç açılmayabilir.
6. MetaApi üçüncü taraf bir servistir. Tickmill işlem şifren onların sisteminde saklanır.
7. Küçük risk seç. Örnek: bir işlemde en fazla 5 dolar kaybet.

## Gerekenler

1. Bir **Tickmill MT5 hesabı**. Önce **demo hesapla** denemen önerilir.
2. Tickmill hesabının **giriş numarası** (login).
3. Tickmill hesabının **işlem şifresi**. Bu, "yatırımcı (investor) şifresi" **DEĞİLDİR**. Yatırımcı şifresiyle işlem açılamaz.
4. Tickmill **sunucu adı**. Örnek: `Tickmill-Demo` veya `Tickmill-Live`. Tickmill'in e-postasında yazar.
5. Bir e-posta adresi.

## Adımlar

### 1. MetaApi hesabı aç

1. Tarayıcıda **metaapi.cloud** adresine git.
2. Kayıt ol (Sign up).
3. E-postanı onayla.

### 2. Ücret ve limitleri kontrol et

1. Sitedeki **fiyat sayfasına** bak.
2. Ücretsiz planın limitlerini oku.
3. Güncel planı ve limitleri **onların sitesinden** kontrol et. Bu rehberde fiyat yazmıyoruz, çünkü değişebilir.
4. Gerekirse sana uygun planı seç.

### 3. Tickmill hesabını ekle

1. MetaApi panelinde **Add account** (hesap ekle) düğmesine bas.
2. Platform olarak **MetaTrader 5** seç.
3. **Login** alanına Tickmill giriş numaranı yaz.
4. **Password** alanına **işlem şifreni** yaz. Yatırımcı şifresini YAZMA.
5. **Server** alanına Tickmill sunucu adını yaz. Örnek: `Tickmill-Demo`.
6. Hesaba bir ad ver. Örnek: `tickmill`.
7. Kaydet.

### 4. Hesabı çalıştır (deploy)

1. Hesap listesinde yeni hesabı bul.
2. **Deploy** düğmesine bas.
3. Durum **DEPLOYED** olana kadar bekle.
4. Birkaç dakika sürebilir.
5. Durum **CONNECTED** da olmalı.
6. Hata yazarsa sunucu adını ve şifreyi tekrar kontrol et.

### 5. Hesap kimliğini (account id) kopyala

1. Hesabın detay sayfasını aç.
2. **Account ID** alanını bul.
3. Kopyala.

### 6. API token oluştur

1. MetaApi panelinde **API access** veya **Token** bölümünü aç.
2. Yeni bir token oluştur.
3. Token'ı kopyala.
4. Bu token hesabına erişim verir. Gizli tut.

### 7. İki bilgiyi hesabı verene gönder

1. Gönderilecek iki şey: **Account ID** ve **API token**.
2. Bunları **özel bir kanaldan** gönder. Örnek: birebir mesaj.
3. Herkese açık gruba **yazma**.
4. Ekran görüntüsünü herkese açık yere **koyma**.
5. Tickmill şifreni ona **GÖNDERME**. Ona gerek yok. Şifre sadece MetaApi'de kalır.

### 8. Sembol adlarını kontrol et

Tickmill'de kripto ve altın adları farklı olabilir. Örnek: `BTCUSD`, `ETHUSD`, `XAUUSD`.

1. MT5 programını aç ve Tickmill hesabına gir.
2. **Market Watch** (Piyasa Gözlemi) penceresini aç.
3. Sağ tıkla ve **Symbols** (Semboller) seç.
4. Kripto ve altın listesine bak.
5. İşlem görmek istediğin varlıkların **tam adını** not et.
6. Notu hesabı verene gönder.
7. Tickmill'de olmayan varlık işlem görmez. Bot onu atlar.

## Risk ayarı

- Başlangıç riski küçüktür. Örnek: 1 işlemde 5 dolar.
- Bu ayarı **hesabı veren kişi** yapar.
- Daha küçük veya büyük istersen ona söyle.
- Önce küçük başla. Birkaç işlem izle. Sonra karar ver.

## Durdurmak ve iptal etmek

Her an durdurabilirsin. Bunun iki yolu var.

**Yol 1: Token'ı sil.**

1. MetaApi panelinde token bölümünü aç.
2. Token'ı sil.
3. Bot artık hesabına erişemez. Yeni işlem açılmaz.

**Yol 2: Hesabı sil.**

1. MetaApi panelinde Tickmill hesabını sil (undeploy ve remove).
2. Tickmill şifreni değiştirmen de iyi olur.

Dikkat: Açık işlemler Tickmill'de açık kalabilir. MT5'ten kendin kapat. Stop emirleri hesapta durur.

## Sık sorunlar

- **Hesap CONNECTED olmuyor:** Sunucu adını ve işlem şifresini kontrol et. Yatırımcı şifresi çalışmaz.
- **İşlem açılmıyor:** Sembol adı farklı olabilir. Adım 8'i yap.
- **Token çalışmıyor:** Yeni token oluştur ve hesabı verene gönder.
