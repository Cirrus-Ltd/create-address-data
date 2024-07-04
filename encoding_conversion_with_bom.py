# encoding_conversion_with_bom.py
import chardet

# ファイルのエンコーディングを検出
with open('mst_load.csv', 'rb') as f:
    result = chardet.detect(f.read())

# 検出されたエンコーディングを表示
print(f"Detected encoding: {result['encoding']}")

# ファイルを適切なエンコーディングで読み込み、UTF-8で保存
with open('mst_load.csv', 'r', encoding=result['encoding']) as f:
    content = f.read()

# UTF-8エンコーディングでBOMを追加して保存
with open('mst_load_utf8_bom.csv', 'w', encoding='utf-8-sig') as f:
    f.write(content)

print("ファイルのエンコーディング変換とBOMの追加が完了しました。")
