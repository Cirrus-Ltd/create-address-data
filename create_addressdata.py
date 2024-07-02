import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# キャッシュ用の辞書
cache = {}

# リトライ設定
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def get_lat_lon(postal_code):
    if postal_code in cache:
        return cache[postal_code]

    # 郵便番号をクリーンアップ（数字以外の文字を削除）
    postal_code = ''.join(filter(str.isdigit, postal_code))

    url = f"https://geoapi.heartrails.com/api/json?method=searchByPostal&postal={postal_code}"
    try:
        response = http.get(url)
        response.raise_for_status()  # HTTPエラーが発生した場合に例外を発生させる
        data = response.json()

        if 'location' in data['response']:
            location = data['response']['location'][0]
            lat_lon = (location['y'], location['x'])
            cache[postal_code] = lat_lon
            return lat_lon
        else:
            cache[postal_code] = (None, None)
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for postal code {postal_code}: {e}")
        return None, None

# CSVファイルを読み込む
df = pd.read_csv('utf_ken_all.csv', header=None, encoding='utf-8', dtype=str)

# 必要な列を抽出
df = df[[3, 7, 8, 9]]
df.columns = ['郵便番号', '都道府県', '市区町村', '町域']

# 緯度と経度を取得する関数を適用
def apply_get_lat_lon(row):
    lat, lon = get_lat_lon(row['郵便番号'])
    time.sleep(1)  # リクエスト間に遅延を追加
    return pd.Series({'緯度': lat, '経度': lon})

# 並列処理を使用して緯度と経度を取得
with ThreadPoolExecutor(max_workers=5) as executor:
    results = list(executor.map(apply_get_lat_lon, [row for _, row in df.iterrows()]))

# 結果をDataFrameに変換
lat_lon_df = pd.DataFrame(results)

# 元のDataFrameに緯度と経度を追加
df = pd.concat([df, lat_lon_df], axis=1)

# 結果をCSVファイルに保存
df.to_csv('output.csv', index=False, encoding='utf-8-sig')

print("処理が完了しました。")
