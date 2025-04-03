# CSV to DB

CSVファイル検索とデータベース取り込みツール

## 概要

このツールは、指定されたフォルダ内でパターンに一致するCSVファイルを検索し、将来的にはデータベースに取り込む機能を提供します。
通常のファイルシステム内のCSVファイルだけでなく、ZIPファイル内のCSVファイルも検索対象となります。

## インストール

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/csv-to-db.git
cd csv-to-db

# 依存関係のインストール
uv pip install -e .
```

## 使用方法

1. `.env`ファイルを設定します（下記の設定項目を参照）
2. 以下のコマンドを実行します：

```bash
uv run main.py
```

## 設定項目

`.env`ファイルに以下の設定を記述します：

| 設定項目 | 説明 | 必須 | デフォルト値 |
|----------|------|------|------------|
| FOLDER | 検索対象のフォルダパス | ✅ | - |
| PATTERN | CSVファイル名の検索パターン（正規表現） | ✅ | - |
| DB | 使用するデータベースファイル | ❌ | sensor_data.duckdb |
| ENCODING | ファイルのエンコーディング | ❌ | utf-8 |
| PLANT | プラント名 | ❌ | - |
| MACHINE_ID | 機械ID | ❌ | - |
| DATA_LABEL | データラベル | ❌ | - |

設定例：
```
# データ取り込み用設定
FOLDER=data
PATTERN=(Cond|User|test)
DB=sensor_data.duckdb
ENCODING=utf-8
PLANT=AAA
MACHINE_ID=No.1
DATA_LABEL=2024
```

## ファイル構成

```
csv-to-db/
├── .env                # 環境変数設定ファイル
├── main.py             # エントリーポイント
├── pyproject.toml      # プロジェクト設定
├── README.md           # このファイル
├── create_test_zip.py  # テスト用ZIPファイル作成スクリプト
├── data/               # データフォルダ
└── src/                # ソースコード
    ├── __init__.py     # パッケージ初期化
    ├── main.py         # メイン処理
    ├── config.py       # 設定管理
    ├── file_finder.py  # ファイル検索
    ├── logger.py       # ロギング
    └── db.py           # データベース操作（将来拡張用）
```

## 開発情報

### 依存ライブラリ

- duckdb: データベースエンジン
- numpy: 数値計算ライブラリ
- pandas: データ分析ライブラリ
- polars: 高速データフレームライブラリ
- pyarrow: Apache Arrowデータフォーマット
- pytest: テストフレームワーク
- python-dotenv: .envファイルからの環境変数読み込み

### テスト

テスト用のZIPファイルを作成するには：

```bash
python create_test_zip.py
```

## ライセンス

[ライセンス情報をここに記載]
