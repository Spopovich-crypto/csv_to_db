# インストールガイド

このドキュメントでは、CSV to DBツールのインストール方法について説明します。

## システム要件

- Python 3.8以上
- 対応OS: Windows、macOS、Linux

## 依存ライブラリ

CSV to DBツールは以下の主要なライブラリに依存しています：

- **duckdb**: 高速な組み込みSQLデータベースエンジン
- **numpy**: 数値計算ライブラリ
- **pandas**: データ分析ライブラリ
- **polars**: 高速データフレームライブラリ（主要なデータ処理に使用）
- **pyarrow**: Apache Arrowデータフォーマット（Parquetファイル処理に使用）
- **python-dotenv**: .envファイルからの環境変数読み込み

## インストール手順

### 1. リポジトリのクローン

```bash
git clone https://github.com/yourusername/csv-to-db.git
cd csv-to-db
```

### 2. 依存関係のインストール

このプロジェクトでは、[uv](https://github.com/astral-sh/uv)を使用して依存関係をインストールすることを推奨しています。uvがインストールされていない場合は、先にuvをインストールしてください。

```bash
# uvを使用して依存関係をインストール
uv pip install -e .
```

または、pipを使用する場合：

```bash
pip install -e .
```

## 初期設定

### 1. .envファイルの作成

プロジェクトのルートディレクトリに`.env`ファイルを作成し、必要な設定を記述します。最低限、以下の設定が必要です：

```
# 必須設定
FOLDER=data                # 検索対象のフォルダパス
PATTERN=(Cond|User|test)   # CSVファイル名の検索パターン（正規表現）

# オプション設定
DB=database/sensor_data.duckdb  # データベースファイルのパス
ENCODING=utf-8                  # ファイルのエンコーディング
PLANT=AAA                       # プラント名
MACHINE_ID=No.1                 # 機械ID
DATA_LABEL=2024                 # データラベル
```

### 2. データフォルダの準備

`.env`ファイルで指定した`FOLDER`パスにデータフォルダを作成します。このフォルダには、処理対象のCSVファイルやZIPファイルを配置します。

```bash
mkdir -p data
```

## インストール後の動作確認

インストールが正常に完了したことを確認するために、テスト用のZIPファイルを作成し、ツールを実行してみましょう。

### 1. テスト用ZIPファイルの作成

プロジェクトに含まれる`create_test_zip.py`スクリプトを使用して、テスト用のZIPファイルを作成します。

```bash
python create_test_zip.py
```

このスクリプトは、`data`フォルダ内にテスト用のCSVファイルを含むZIPファイルを作成します。

### 2. ツールの実行

```bash
uv run main.py
```

または、Pythonを直接使用する場合：

```bash
python main.py
```

正常に実行されると、以下のような出力が表示されます：

```
2025-04-03 10:00:00 - INFO - 以下のフォルダ内でパターン'(Cond|User|test)'に一致するCSVファイルを検索中...
検索フォルダ： data
2025-04-03 10:00:01 - INFO - 見つかったCSVファイル (X件):
1. test_data.csv
2. test_data.zip::Cond_data.csv (ZIPファイル内)
...
```

## トラブルシューティング

インストール中に問題が発生した場合は、[トラブルシューティングガイド](troubleshooting.md)を参照してください。

## 次のステップ

インストールが完了したら、[ユーザーマニュアル](user_manual.md)を参照して、ツールの基本的な使い方を学びましょう。
