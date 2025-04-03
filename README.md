# CSV to DB

CSVファイル検索とデータベース取り込みツール

## 概要

このツールは、指定されたフォルダ内でパターンに一致するCSVファイルを検索し、DuckDBデータベースに取り込む機能を提供します。
通常のファイルシステム内のCSVファイルだけでなく、ZIPファイル内のCSVファイルも検索対象となります。

## 主な機能

- 指定されたフォルダ内でパターンに一致するCSVファイルの検索
- ZIPファイル内のCSVファイルにも対応
- 特殊な形式のCSVファイルの読み込みと処理
- 横持ちデータから縦持ちデータへの変換
- 処理済みデータのParquetファイルへの保存
- DuckDBデータベースへのデータ取り込み
- 処理済みファイルの管理（重複処理の防止）

## クイックスタート

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/csv-to-db.git
cd csv-to-db

# 依存関係のインストール
uv pip install -e .

# .envファイルを設定
# 必要な設定項目: FOLDER, PATTERN

# ツールを実行
uv run main.py
```

## ドキュメント

詳細なドキュメントは以下のファイルを参照してください：

- [インストールガイド](docs/installation.md) - システム要件、依存関係、インストール手順
- [ユーザーマニュアル](docs/user_manual.md) - 基本的な使い方、設定ファイルの詳細
- [ファイル形式ガイド](docs/file_formats.md) - 対応しているCSV形式の詳細
- [技術リファレンス](docs/technical_reference.md) - アーキテクチャ、処理フロー
- [開発者ガイド](docs/developer_guide.md) - コード構造、拡張方法
- [トラブルシューティング](docs/troubleshooting.md) - 一般的な問題と解決方法

## ファイル構成

```
csv-to-db/
├── .env                # 環境変数設定ファイル
├── main.py             # エントリーポイント
├── pyproject.toml      # プロジェクト設定
├── README.md           # このファイル
├── create_test_zip.py  # テスト用ZIPファイル作成スクリプト
├── data/               # データフォルダ
├── docs/               # ドキュメントフォルダ
└── src/                # ソースコード
    ├── __init__.py     # パッケージ初期化
    ├── main.py         # メイン処理
    ├── config.py       # 設定管理
    ├── file_finder.py  # ファイル検索
    ├── logger.py       # ロギング
    ├── preprocessor.py # 前処理
    └── db.py           # データベース操作
```

## ライセンス

[ライセンス情報をここに記載]
