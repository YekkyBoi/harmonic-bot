Aşağıdaki metnin TAMAMINI Claude Code'a yapıştır (Windows bilgisayarında, MT5'in kurulu olduğu makinede).

---

Bana Telegram sinyal kopyalama botunu kurmanda yardım et. Türkçe ve kısa konuş, ben teknik biri değilim, her seferinde TEK adım ver.

PROJE: https://github.com/YekkyBoi/harmonic-bot , dal: claude/telegram-crypto-automation-o6xo7l , klasör: telegram-copy-trader . Bu dalı indir (git clone --branch claude/telegram-crypto-automation-o6xo7l veya zip). Kurulum rehberi projede docs/ARKADAS_KURULUM.md dosyasında, önce onu oku ve ona göre ilerle. Örnek ayar: config.tickmill.example.yaml.

NE YAPACAK: Benim Telegram hesabımla sinyal grubunu okuyacak (sadece okuma), Claude ile sinyalleri anlayacak ve Tickmill MT5 hesabımda işlem açacak. Fiyat farkı (Binance grafiği ile Tickmill arasındaki fark) config'teki reference_exchange ile kaydırılır.

KURALLAR:
1. Şifreleri, API anahtarlarını, Telegram oturum dosyasını ASLA sohbete yazdırma ve bir yere gönderme. Sırları sadece bilgisayardaki .env dosyasına yazdır (notepad ile ben dolduracağım, sen içeriğini okuma/yazdırma).
2. Önce Python 3.12 kurulu mu kontrol et, yoksa kurmamı söyle ("Add to PATH" işaretli). venv oluştur, requirements.txt ve MetaTrader5 paketini kur.
3. config.tickmill.example.yaml dosyasını config.yaml olarak kopyala, .env.example'ı .env yap. dry_run: true kalsın (sahte işlem). Gerçeğe geçmeyi ben bilerek isteyene kadar kapatma.
4. Benden şunları adım adım iste: Telegram api_id/api_hash (my.telegram.org), bildirim botu tokeni (@BotFather) ve kendi Telegram kullanıcı id'm (@userinfobot), Anthropic API anahtarı, Tickmill MT5 login/şifre/sunucu. Hepsini .env'ye ben yazacağım.
5. MT5 açık ve giriş yapılmış olmalı, Algo Trading düğmesi YEŞİL olmalı. Bunu bana hatırlat.
6. Sırayla çalıştır ve her birinin çıktısını kontrol et: python -m copytrader login (QR ile giriş; QR'ı İKİNCİ bir ekranda/telefonda okutmam gerekebilir, bana söyle), python -m copytrader chats (sinyal grubunun id'sini bulup config.yaml'a yaz), python -m copytrader check. Hata olursa nedenini bana basit anlat, tahmin etme.
7. Tickmill'de kripto sembollerinin tam adını MT5 Market Watch'tan kontrol ettir (BTCUSD, ETHUSD gibi) ve config.yaml'daki symbol_map ile allowed_assets'i buna göre düzelt. Sadece Tickmill'in listelediği coinler işlem görür.
8. risk_usd'yi çok küçük başlat (örn. 5). Her ayarı değiştirdikten sonra gerçekten değiştiğini dosyayı okuyarak doğrula.
9. Botu "python -m copytrader run" ile çalıştır. Önce dry_run: true ile bir sinyalin doğru anlaşıldığını izle. Sonra, ben onay verirsem dry_run: false yap. İlk gerçek işlemi birlikte izleyelim.
10. Ben "dur" dersem Ctrl+C ile durdur. Para riski hakkında beni uyar, ama kararı bana bırak.

Hazırsan 1. adımdan başla.
