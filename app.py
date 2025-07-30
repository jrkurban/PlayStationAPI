import os
from flask import Flask, jsonify, request
from pymongo import MongoClient, ASCENDING, DESCENDING

# --- UYGULAMA KURULUMU ---
app = Flask(__name__)

# --- VERİTABANI BAĞLANTISI ---
# Render.com'da Environment Variables olarak ayarlanacak
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "GamesDB" # Scraper'da kullandığımız veritabanı adı

if not MONGO_URI:
    print("UYARI: MONGO_URI ortam değişkeni ayarlanmamış. Lokal test için varsayılan bağlantı kullanılacak.")

try:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    # Bağlantıyı test etmek için sunucu bilgilerini al
    client.server_info() 
    print(f"MongoDB Atlas'taki '{MONGO_DB_NAME}' veritabanına başarıyla bağlanıldı.")
except Exception as e:
    print(f"HATA: Veritabanı bağlantısı kurulamadı. Hata: {e}")
    # Uygulama başlamadan hata vererek durmasını sağlayabiliriz.
    # Bu, Render loglarında sorunu hemen görmenizi sağlar.
    exit()

# Koleksiyonları daha kolay erişim için değişkenlere ata
games_collection = db["games"]
price_history_collection = db["price_history"]


# --- API ENDPOINT'LERİ (YOLLARI) ---

@app.route('/')
def index():
    """API'nin çalıştığını kontrol etmek için kök endpoint."""
    return jsonify({"message": "PlayStation Games API'sine hoş geldiniz!", "status": "ok"})


@app.route('/games', methods=['GET'])
def get_all_games():
    """
    Tüm oyunları sayfalamalı (paginated) olarak döndürür.
    Query Parametreleri:
    - page: Sayfa numarası (varsayılan: 1)
    - limit: Sayfa başına oyun sayısı (varsayılan: 20)
    """
    try:
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        skip = (page - 1) * limit

        # Veritabanından verileri çek ve isme göre sırala
        games_cursor = games_collection.find({}, {'_id': 1, 'name': 1, 'coverUrl': 1}).sort('name', ASCENDING).skip(skip).limit(limit)
        games_list = list(games_cursor)

        # Toplam oyun sayısını al (sayfalama için gerekli)
        total_games = games_collection.count_documents({})

        return jsonify({
            "total_games": total_games,
            "page": page,
            "limit": limit,
            "data": games_list
        })
    except Exception as e:
        return jsonify({"error": "Oyunlar alınırken bir hata oluştu.", "details": str(e)}), 500


@app.route('/games/<string:game_id>', methods=['GET'])
def get_game_details(game_id):
    """Belirli bir oyunun statik detaylarını döndürür."""
    try:
        game = games_collection.find_one({'_id': game_id})
        if not game:
            return jsonify({"error": "Oyun bulunamadı."}), 404
        return jsonify(game)
    except Exception as e:
        return jsonify({"error": "Oyun detayı alınırken bir hata oluştu.", "details": str(e)}), 500


@app.route('/games/<string:game_id>/price-history', methods=['GET'])
def get_price_history(game_id):
    """Belirli bir oyunun tüm fiyat geçmişini en yeniden eskiye doğru döndürür."""
    try:
        history = list(price_history_collection.find({'gameId': game_id}).sort('snapshotDate', DESCENDING))
        return jsonify(history)
    except Exception as e:
        return jsonify({"error": "Fiyat geçmişi alınırken bir hata oluştu.", "details": str(e)}), 500


@app.route('/games/most-price-drops', methods=['GET'])
def get_most_price_drops():
    """Son iki günün verisini karşılaştırarak en çok fiyatı düşen oyunları bulur."""
    try:
        # 1. Adım: Veritabanındaki son iki farklı "snapshotDate"i bul.
        distinct_dates = list(price_history_collection.distinct("snapshotDate"))
        if len(distinct_dates) < 2:
            return jsonify({"error": "Karşılaştırma için yeterli veri yok (en az 2 gün gerekir)."}), 404
        
        distinct_dates.sort(reverse=True)
        today_date_str = distinct_dates[0]
        yesterday_date_str = distinct_dates[1]

        # 2. Adım: MongoDB Aggregation Pipeline
        pipeline = [
            # Aşama 1: Sadece son iki günün verilerini filtrele
            {'$match': {'snapshotDate': {'$in': [today_date_str, yesterday_date_str]}}},
            # Aşama 2: "editions" dizisini açarak her sürüm için ayrı bir belge oluştur
            {'$unwind': '$editions'},
            # Aşama 3: Fiyat metnini sayıya çevir (virgül ve boşlukları temizleyerek)
            {'$addFields': {'numericPrice': {'$convert': {
                'input': {'$trim': {'input': {'$replaceAll': {'input': '$editions.price', 'find': ',', 'replacement': '.'}}}},
                'to': 'double', 'onError': 0.0, 'onNull': 0.0
            }}}},
            # Aşama 4: Oyun ID'si ve Sürüm Adına göre grupla
            {'$group': {
                '_id': {'gameId': '$gameId', 'editionName': '$editions.name'},
                'priceHistory': {'$push': {'date': '$snapshotDate', 'price': '$numericPrice'}}
            }},
            # Aşama 5: "Bugün" ve "Dün" fiyatlarını ayrı alanlar olarak ayıkla
            {'$addFields': {
                'currentPrice': {'$first': {'$filter': {'input': '$priceHistory', 'cond': {'$eq': ['$$this.date', today_date_str]}}}},
                'previousPrice': {'$first': {'$filter': {'input': '$priceHistory', 'cond': {'$eq': ['$$this.date', yesterday_date_str]}}}}
            }},
            {'$addFields': {'currentPrice': '$currentPrice.price', 'previousPrice': '$previousPrice.price'}},
            # Aşama 6: Fiyat düşüşünü hesapla
            {'$addFields': {'priceDrop': {'$subtract': ['$previousPrice', '$currentPrice']}}},
            # Aşama 7: Sadece gerçek bir fiyat düşüşü olanları ve önceki fiyatı sıfırdan büyük olanları tut
            {'$match': {'priceDrop': {'$gt': 0}, 'previousPrice': {'$gt': 0}}},
            # Aşama 8: Düşüş miktarına göre en yüksekten en düşüğe sırala
            {'$sort': {'priceDrop': DESCENDING}},
            # Aşama 9: Ana oyun bilgilerini (isim, kapak resmi vb.) 'games' koleksiyonundan al
            {'$lookup': {
                'from': 'games', 'localField': '_id.gameId', 'foreignField': '_id', 'as': 'gameInfo'
            }},
            # Aşama 10: Son çıktıyı temiz ve kullanışlı bir formata sok
            {'$project': {
                '_id': 0, 'gameId': '$_id.gameId', 'editionName': '$_id.editionName',
                'name': {'$arrayElemAt': ['$gameInfo.name', 0]},
                'coverUrl': {'$arrayElemAt': ['$gameInfo.coverUrl', 0]},
                'currentPrice': '$currentPrice', 'previousPrice': '$previousPrice', 'priceDrop': '$priceDrop'
            }},
            # Aşama 11: Sonuçları sınırla (isteğe bağlı, örn: en iyi 50)
            {'$limit': 50}
        ]

        results = list(price_history_collection.aggregate(pipeline))
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": "Fiyat düşüşleri hesaplanırken bir hata oluştu.", "details": str(e)}), 500

# --- UYGULAMAYI ÇALIŞTIRMA ---
if __name__ == '__main__':
    # Render.com bu kısmı kullanmaz, bunun yerine bir Gunicorn veya benzeri WSGI sunucusu kullanır.
    # Bu blok sadece lokalde test etmek içindir.
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5001)), debug=True)
