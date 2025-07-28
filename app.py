import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util  # MongoDB'nin BSON formatını JSON'a çevirmek için çok önemli!
from datetime import datetime, timedelta, timezone

# .env dosyasındaki ortam değişkenlerini yükle
load_dotenv()

# Flask uygulamasını başlat
app = Flask(__name__)
# CORS'u aktif et, bu API'ye dışarıdan erişim izni verir.
CORS(app)

# MongoDB bağlantısını kur
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise Exception("HATA: MONGO_URI ortam değişkeni bulunamadı!")

client = MongoClient(MONGO_URI)
db = client['GamesDB']  # Veritabanını seç
games_collection = db['games']
price_history_collection = db['price_history']


# --- API ENDPOINT'LERİ ---

@app.route("/api/games", methods=["GET"])
def get_all_games():
    """Tüm oyunları veritabanından çeker ve JSON olarak döndürür."""
    try:
        # Oyunları isme göre alfabetik sıralayarak bul
        games = list(games_collection.find().sort("name", 1))
        # BSON'u JSON formatına çevirip döndür. json_util kullanmak şart!
        return json_util.dumps(games), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/<string:game_id>/price", methods=["GET"])
def get_latest_price(game_id):
    """Belirli bir oyunun en son fiyat kaydını döndürür."""
    try:
        # Verilen game_id'ye ait kayıtları, tarihe göre tersten sırala ve ilkini al.
        latest_price_doc = price_history_collection.find_one(
            {"gameId": game_id},
            sort=[("snapshotDate", -1)]
        )

        if not latest_price_doc:
            return jsonify({"error": "Bu oyun için fiyat bilgisi bulunamadı."}), 404

        return json_util.dumps(latest_price_doc), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/discounted", methods=["GET"])
def get_discounted_games():
    """
    Fiyatı düşmüş ve indirimi son 7 gün içinde başlamış oyunları bulur.
    Ayrıca indirimin başlangıç tarihini ve kaç gündür devam ettiğini de döndürür.
    """
    try:
        # Son 7 günün başlangıç zamanını hesapla
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # Agregation Pipeline ile karmaşık sorgu
        pipeline = [
            # 1. Adım: Tüm fiyat kayıtlarını tarihe göre tersten sırala
            {"$sort": {"snapshotDate": -1}},
            # 2. Adım: Her oyunun fiyat geçmişini grupla
            {
                "$group": {
                    "_id": "$gameId",
                    "history": {"$push": "$$ROOT"}
                }
            },
            # 3. Adım: 'games' koleksiyonu ile birleştirerek oyun detaylarını al
            {
                "$lookup": {
                    "from": "games",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "game_details"
                }
            },
            # 4. Adım: Oyun detayı olmayanları (eşleşmeyenleri) filtrele
            {
                "$match": {"game_details": {"$ne": []}}
            },
            # 5. Adım: Gerekli alanları yeniden yapılandır
            {
                "$project": {
                    "gameId": "$_id",
                    "game_details": {"$arrayElemAt": ["$game_details", 0]},
                    "history": 1
                }
            }
        ]

        all_games_with_history = list(price_history_collection.aggregate(pipeline))

        discounted_games_with_info = []

        for game_data in all_games_with_history:
            history = game_data.get('history', [])

            # İndirimin ne zaman başladığını ve güncel fiyatı bul
            discount_start_date = None
            latest_price = None
            is_on_discount = False

            # Fiyat geçmişini eskiden yeniye doğru tara
            for i in range(len(history) - 1, 0, -1):
                current_record = history[i]
                next_record = history[i - 1]  # Daha yeni kayıt

                try:
                    current_price = float(current_record['editions'][0]['price'].replace(',', '.').strip())
                    next_price = float(next_record['editions'][0]['price'].replace(',', '.').strip())

                    # Fiyat düşüşü tespit edildi
                    if next_price < current_price:
                        # Eğer bu düşüş zaten devam eden bir indirimin parçası değilse
                        if not is_on_discount:
                            is_on_discount = True
                            # İndirimin başlangıç tarihi, fiyatın düştüğü YENİ kaydın tarihidir.
                            discount_start_date = datetime.fromisoformat(
                                next_record['snapshotDate'].replace('Z', '+00:00'))

                    # Fiyat tekrar yükseldiyse veya aynı kaldıysa, indirim bitti.
                    elif next_price >= current_price:
                        is_on_discount = False
                        discount_start_date = None

                except (ValueError, IndexError, KeyError):
                    continue

            # Eğer oyun hala indirimdeyse ve indirim son 7 gün içinde başladıysa
            if is_on_discount and discount_start_date and discount_start_date >= seven_days_ago:
                days_on_discount = (datetime.now(timezone.utc) - discount_start_date).days

                # API yanıtı için oyun objesini hazırla
                game_info = game_data['game_details']
                game_info['discountInfo'] = {
                    "discountStartDate": discount_start_date.isoformat(),
                    "daysOnDiscount": days_on_discount
                }
                discounted_games_with_info.append(game_info)

        return json_util.dumps(discounted_games_with_info), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


# Bu blok, kodu doğrudan 'python app.py' ile çalıştırdığımızda
# Flask'ın test sunucusunu başlatır.
if __name__ == "__main__":
    app.run(debug=True, port=5001)  # Farklı bir port belirttim
