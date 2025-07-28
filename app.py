import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util  # MongoDB'nin BSON formatını JSON'a çevirmek için çok önemli!

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

@app.route("/api/games/discounted", methods=["GET"])
def get_discounted_games():
    """
    Fiyatı son kayıtta bir öncekine göre düşmüş olan oyunları bulur.
    Bu işlem tüm veritabanını taradığı için biraz yavaş olabilir.
    """
    try:
        # Pipeline, MongoDB'de karmaşık sorgular ve veri işlemleri yapmak için kullanılır.
        # Bu pipeline, her bir oyunun son iki fiyat kaydını alıp karşılaştırır.
        pipeline = [
            # 1. Adım: Tüm fiyat kayıtlarını tarihe göre tersten sırala.
            {
                "$sort": {"snapshotDate": -1}
            },
            # 2. Adım: Her bir 'gameId' için fiyat kayıtlarını grupla.
            {
                "$group": {
                    "_id": "$gameId",  # Oyun ID'sine göre grupla
                    "price_history": {"$push": "$$ROOT"}  # O gruba ait tüm kayıtları bir diziye ekle
                }
            },
            # 3. Adım: Her gruptan sadece ilk iki (en yeni) kaydı al.
            {
                "$project": {
                    "price_history": {"$slice": ["$price_history", 2]}
                }
            },
            # 4. Adım: Sadece iki fiyat kaydı olanları filtrele.
            {
                "$match": {
                    "price_history.1": {"$exists": True}
                }
            }
        ]

        # Yukarıdaki pipeline'ı çalıştır ve sonuçları al
        potential_discounts = list(price_history_collection.aggregate(pipeline))

        discounted_game_ids = set()

        for item in potential_discounts:
            # item['price_history'][0] -> en son fiyat
            # item['price_history'][1] -> bir önceki fiyat
            latest_record = item['price_history'][0]
            previous_record = item['price_history'][1]

            # Fiyatları karşılaştırmak için hazırlık (ilk sürümü baz alıyoruz)
            try:
                # Fiyat metnini temizleyip sayıya dönüştür. 'Ücretsiz', 'Dahil' gibi metinleri atlar.
                latest_price = float(latest_record['editions'][0]['price'].replace(',', '.').strip())
                previous_price = float(previous_record['editions'][0]['price'].replace(',', '.').strip())

                # Eğer son fiyat, bir önceki fiyattan düşükse, bu bir indirimdir.
                if latest_price < previous_price:
                    discounted_game_ids.add(latest_record['gameId'])

            except (ValueError, IndexError, KeyError):
                # Fiyat sayıya çevrilemiyorsa, 'editions' listesi boşsa veya 'price' anahtarı yoksa devam et.
                continue

        # İndirimli olarak bulduğumuz ID'lere sahip oyunların tam bilgilerini 'games' koleksiyonundan çek.
        # '$in' operatörü, listedeki herhangi bir ID ile eşleşen tüm dokümanları bulur.
        discounted_games_docs = list(games_collection.find({"_id": {"$in": list(discounted_game_ids)}}))

        return json_util.dumps(discounted_games_docs), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


# Bu blok, kodu doğrudan 'python app.py' ile çalıştırdığımızda
# Flask'ın test sunucusunu başlatır.
if __name__ == "__main__":
    app.run(debug=True, port=5001)  # Farklı bir port belirttim
