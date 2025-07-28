import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util
from datetime import datetime, timedelta, timezone

# --- KURULUM ---
load_dotenv()
app = Flask(__name__)
CORS(app)

# --- VERİTABANI BAĞLANTISI ---
client = MongoClient(os.getenv('MONGO_URI'))
db = client['GamesDB']
games_collection = db['games']
price_history_collection = db['price_history']


# --- API ENDPOINT'LERİ ---

@app.route("/api/games", methods=["GET"])
def get_all_games():
    """Tüm oyunları veritabanından çeker ve JSON olarak döndürür."""
    try:
        games = list(games_collection.find().sort("name", 1))
        return json_util.dumps(games), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/games/<string:game_id>/price", methods=["GET"])
def get_latest_price(game_id):
    """Belirli bir oyunun en son fiyat kaydını döndürür."""
    try:
        latest_price_doc = price_history_collection.find_one(
            {"gameId": game_id}, sort=[("snapshotDate", -1)]
        )
        if not latest_price_doc:
            return jsonify({"error": "Fiyat bulunamadı."}), 404
        return json_util.dumps(latest_price_doc), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- YENİ ENDPOINT: Fiyat Grafiği İçin ---
@app.route("/api/games/<string:game_id>/price-history", methods=["GET"])
def get_price_history(game_id):
    """Belirli bir oyunun tüm fiyat geçmişini tarihe göre sıralı döndürür."""
    try:
        # Tüm geçmişi bul ve tarihe göre eskiden yeniye doğru sırala
        history = list(price_history_collection.find({"gameId": game_id}).sort("snapshotDate", 1))
        if not history:
            return jsonify({"error": "Fiyat geçmişi bulunamadı."}), 404
        return json_util.dumps(history), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- GÜNCELLENMİŞ ENDPOINT: İndirimli Oyunlar İçin ---
@app.route("/api/games/discounted", methods=["GET"])
def get_discounted_games():
    """Fiyatı son kayıtta bir öncekine göre düşmüş ve indirimi son 7 gün içinde başlamış oyunları bulur."""
    try:
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

        # Tüm oyunların ID'lerini al
        all_game_ids = [game['_id'] for game in games_collection.find({}, {'_id': 1})]

        discounted_games_with_info = []

        for game_id in all_game_ids:
            # Her oyunun son İKİ fiyat kaydını al
            price_records = list(price_history_collection.find(
                {"gameId": game_id},
                sort=[("snapshotDate", -1)]
            ).limit(2))

            if len(price_records) == 2:
                latest_record = price_records[0]
                previous_record = price_records[1]

                try:
                    # '2.499,00' -> 2499.00 dönüşümü
                    latest_price_str = latest_record['editions'][0]['price'].replace('.', '').replace(',', '.')
                    previous_price_str = previous_record['editions'][0]['price'].replace('.', '').replace(',', '.')

                    latest_price = float(latest_price_str)
                    previous_price = float(previous_price_str)

                    # Fiyat düşüşü varsa
                    if latest_price < previous_price:
                        # İndirimin başlangıç tarihini (son kaydın tarihi) kontrol et
                        discount_start_date = datetime.fromisoformat(
                            latest_record['snapshotDate'].replace('Z', '+00:00'))

                        if discount_start_date >= seven_days_ago:
                            days_on_discount = (datetime.now(timezone.utc) - discount_start_date).days

                            game_info = games_collection.find_one({"_id": game_id})
                            if game_info:
                                game_info['discountInfo'] = {
                                    "discountStartDate": discount_start_date.isoformat(),
                                    "daysOnDiscount": days_on_discount if days_on_discount >= 0 else 0
                                }
                                discounted_games_with_info.append(game_info)

                except (ValueError, IndexError, KeyError):
                    continue

        return json_util.dumps(discounted_games_with_info), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5001)
