# app.py - Geliştirilmiş İndirim Mantığı ile
import os
from flask import Flask, jsonify, request
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

# --- UYGULAMA KURULUMU ---
app = Flask(__name__)

# --- AYARLAR ---
# Render.com'da Environment Variables olarak ayarlanacak
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB"
# Kaç günlük geçmişe bakılacağını belirle
LOOKBACK_DAYS = 7 # Son 7 gün içindeki indirimleri bul

# --- VERİTABANI BAĞLANTISI ---
if not MONGO_URI:
    print("UYARI: MONGO_URI ortam değişkeni ayarlanmamış.")
    exit()

try:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    client.server_info()
    print(f"MongoDB Atlas'taki '{MONGO_DB_NAME}' veritabanına başarıyla bağlanıldı.")
except Exception as e:
    print(f"HATA: Veritabanı bağlantısı kurulamadı. Hata: {e}")
    exit()

games_collection = db["games"]
price_history_collection = db["price_history"]


# --- YARDIMCI FONKSİYONLAR (Script'inizden alındı) ---

def parse_price(price_str: Optional[str]) -> Optional[float]:
    """Fiyat metnini temizler ve sayısal bir değere dönüştürür."""
    if price_str is None: return None
    price_str = price_str.strip().lower()
    if any(tag in price_str for tag in ['ücretsiz', 'dahil', 'oyna', 'indir', 'n/a']):
        return 0.0
    try:
        # Önce binlik ayıracı olan '.' karakterini kaldır, sonra ondalık ayıracı olan ',' karakterini '.' yap
        cleaned_str = price_str.replace('.', '').replace(',', '.')
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None


# --- MEVCUT API ENDPOINT'LERİ (Değişiklik yok) ---

@app.route('/')
def index():
    return jsonify({"message": "PlayStation Games API'sine hoş geldiniz!", "status": "ok"})

@app.route('/games', methods=['GET'])
def get_all_games():
    try:
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        skip = (page - 1) * limit
        games_cursor = games_collection.find({}, {'_id': 1, 'name': 1, 'coverUrl': 1}).sort('name', ASCENDING).skip(skip).limit(limit)
        games_list = list(games_cursor)
        total_games = games_collection.count_documents({})
        return jsonify({"total_games": total_games, "page": page, "limit": limit, "data": games_list})
    except Exception as e:
        return jsonify({"error": "Oyunlar alınırken bir hata oluştu.", "details": str(e)}), 500

@app.route('/games/<string:game_id>', methods=['GET'])
def get_game_details(game_id):
    try:
        game = games_collection.find_one({'_id': game_id})
        if not game: return jsonify({"error": "Oyun bulunamadı."}), 404
        return jsonify(game)
    except Exception as e:
        return jsonify({"error": "Oyun detayı alınırken bir hata oluştu.", "details": str(e)}), 500

@app.route('/games/<string:game_id>/price-history', methods=['GET'])
def get_price_history(game_id):
    try:
        history = list(price_history_collection.find({'gameId': game_id}).sort('snapshotDate', DESCENDING))
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": "Fiyat geçmişi alınırken bir hata oluştu.", "details": str(e)}), 500


# --- YENİ VE GELİŞTİRİLMİŞ İNDİRİMLER ENDPOINT'İ ---

# Not: Eski `/games/most-price-drops` yerine bunu kullanacağız.
# Swift kodunuzda endpoint'i /games/recent-discounts olarak güncelleyebilirsiniz
# veya bu fonksiyon adını koruyup eski yolu kullanmaya devam edebilirsiniz.
@app.route('/games/most-price-drops', methods=['GET'])
def get_recent_discounts():
    """
    Son 'LOOKBACK_DAYS' gün içinde fiyatı düşen tüm ürünleri bulur ve döndürür.
    Bu fonksiyon, generate_discount_report.py'deki mantığı bir API olarak sunar.
    """
    try:
        # 1. Geriye dönük karşılaştırma için başlangıç tarihini belirle.
        # 7 günlük düşüşleri bulmak için en az 8 günlük veriye bakmak daha sağlıklıdır.
        reference_start_date = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS + 1)
        
        # 2. Veritabanından ilgili aralıktaki tüm veriyi, oyun ID'sine göre gruplayarak çek.
        # Bu, tüm veriyi tek seferde çekip Python'da gruplamaktan daha verimlidir.
        pipeline = [
            {'$match': {'snapshotDate': {'$gte': reference_start_date.isoformat()}}},
            {'$sort': {'snapshotDate': ASCENDING}},
            {'$group': {
                '_id': '$gameId',
                'history': {'$push': '$$ROOT'} # Bir oyuna ait tüm geçmişi bir diziye ekle
            }}
        ]
        all_game_histories = list(price_history_collection.aggregate(pipeline))

        if not all_game_histories:
            return jsonify([]) # Veri yoksa boş liste döndür

        # 3. Oyun bilgilerini (isim, kapak) tek seferde çekmek için bir harita oluştur
        game_ids = [game['_id'] for game in all_game_histories]
        game_info_map = {game['_id']: game for game in games_collection.find({'_id': {'$in': game_ids}})}

        recent_price_drops = {} # Son indirimleri saklamak için

        # 4. Her oyunun geçmişini Python içinde analiz et
        for game_group in all_game_histories:
            game_id = game_group['_id']
            history = game_group['history']

            if len(history) < 2: continue

            # Geçmişi eskiden yeniye doğru tara
            for i in range(1, len(history)):
                previous_doc = history[i - 1]
                current_doc = history[i]

                # Fiyat düşüşü olayının tarihi son 7 gün içinde mi kontrol et
                drop_date = datetime.fromisoformat(current_doc['snapshotDate'].replace('Z', '+00:00'))
                if (datetime.now(timezone.utc) - drop_date).days > LOOKBACK_DAYS:
                    continue

                prev_editions = {e['name']: e for e in previous_doc.get('editions', [])}

                for current_edition in current_doc.get('editions', []):
                    if current_edition['name'] in prev_editions:
                        prev_edition = prev_editions[current_edition['name']]

                        prev_price = parse_price(prev_edition.get('price'))
                        current_price = parse_price(current_edition.get('price'))

                        if prev_price is not None and current_price is not None and current_price < prev_price:
                            # BİR İNDİRİM OLAYI TESPİT EDİLDİ!
                            game_info = game_info_map.get(game_id, {})
                            drop_key = f"{game_id}-{current_edition['name']}"
                            
                            recent_price_drops[drop_key] = {
                                'gameId': game_id,
                                'name': game_info.get('name', 'Bilinmeyen Oyun'),
                                'coverUrl': game_info.get('coverUrl', ''),
                                'editionName': current_edition['name'],
                                'previousPrice': prev_price,
                                'currentPrice': current_price,
                                'priceDrop': prev_price - current_price
                            }

        # 5. Sonuçları fiyattaki düşüş miktarına göre sırala ve döndür
        final_drops_list = sorted(recent_price_drops.values(), key=lambda x: x['priceDrop'], reverse=True)
        return jsonify(final_drops_list)

    except Exception as e:
        return jsonify({"error": "İndirimler hesaplanırken bir hata oluştu.", "details": str(e)}), 500


# --- UYGULAMAYI ÇALIŞTIRMA ---
if __name__ == '__main__':
    # Render.com bu bloku kullanmaz, Gunicorn gibi bir WSGI sunucusu kullanır.
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5001)))
