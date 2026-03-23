from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    if not data:
        return jsonify({"error": "no data"}), 400
    
    symbol = data.get("symbol", "?")
    timeframe = data.get("timeframe", "?")
    pattern = data.get("pattern", "?")
    direction = data.get("direction", "?")
    prz_low = data.get("prz_low", "?")
    prz_high = data.get("prz_high", "?")
    confidence = data.get("confidence", "?")
    
    emoji_map = {"Gartley": "📐", "Butterfly": "🦋", "Bat": "🦇", "Crab": "🦀"}
    emoji = emoji_map.get(pattern, "📊")
    dir_emoji = "🟢 LONG" if direction == "bullish" else "🔴 SHORT"
    
    msg = f"""{emoji} *{pattern.upper()} PATTERNİ TESPİT EDİLDİ!*
━━━━━━━━━━━━━━━━━━━━
📌 *Parite:* {symbol}
⏱ *Zaman Dilimi:* {timeframe}
📈 *Yön:* {dir_emoji}
🎯 *PRZ Kutusu:* {prz_low} - {prz_high}
🔥 *Güven:* %{confidence}
━━━━━━━━━━━━━━━━━━━━
⚠️ _Finansal tavsiye değildir._"""
    
    send_telegram(msg)
    return jsonify({"status": "ok"}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"}), 200

if name == "__main__":
    app.run(host="0.0.0.0", port=5000)
