import pandas as pd
import numpy as np
# CSVファイルを読み込む
df = pd.read_csv('utf_ken_all.csv', header=None, encoding='utf-8', dtype=str)

# 分割するファイル数
num_splits = 10  # 例えば6つのPCで処理する場合

# データを分割
split_dfs = np.array_split(df, num_splits)

# 分割されたデータを保存
for i, split_df in enumerate(split_dfs):
    split_df.to_csv(f'utf_ken_all_part_{i}.csv', index=False, header=False, encoding='utf-8-sig')
