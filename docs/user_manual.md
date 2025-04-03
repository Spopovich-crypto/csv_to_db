# ユーザーマニュアル

このドキュメントでは、CSV to DBツールの基本的な使い方について説明します。

## 基本概念

CSV to DBツールは、以下の主要な機能を提供します：

1. **CSVファイルの検索**: 指定されたフォルダ内でパターンに一致するCSVファイルを検索します
2. **データの前処理**: 特殊な形式のCSVファイルを読み込み、横持ちデータから縦持ちデータに変換します
3. **データベースへの取り込み**: 処理したデータをDuckDBデータベースに取り込みます
4. **処理済みファイルの管理**: 処理済みファイルを記録し、重複処理を防止します

## 設定ファイル（.env）

ツールの動作は`.env`ファイルで設定します。以下に各設定項目の詳細を説明します。

### 必須設定項目

| 設定項目 | 説明 | 例 |
|----------|------|-----|
| FOLDER | 検索対象のフォルダパス | `data` |
| PATTERN | CSVファイル名の検索パターン（正規表現） | `(Cond\|User\|test)` |

### オプション設定項目

| 設定項目 | 説明 | デフォルト値 | 例 |
|----------|------|------------|-----|
| DB | 使用するデータベースファイル | `sensor_data.duckdb` | `database/sensor_data.duckdb` |
| ENCODING | ファイルのエンコーディング | `utf-8` | `shift-jis` |
| PLANT | プラント名（縦持ちデータのPLANT列に設定） | `""` | `AAA` |
| MACHINE_ID | 機械ID（縦持ちデータのMACHINE_ID列に設定） | `""` | `No.1` |
| DATA_LABEL | データラベル（縦持ちデータのDATA_LABEL列に設定） | `""` | `2024` |
| PROCESSED_MARKER | 処理済みファイルの管理用ファイル | `processed_files.json` | `logs/processed.json` |
| OUTPUT_DIR | 処理結果の出力先ディレクトリ | `processed` | `output/data` |

### メモリ最適化設定項目（バージョン2.0.0以降）

| 設定項目 | 説明 | デフォルト値 | 例 |
|----------|------|------------|-----|
| BATCH_SIZE | 一度に処理するCSVファイル数 | `5` | `3` |
| CHUNK_SIZE | 一度に処理するデータ行数 | `10000` | `5000` |

### 設定例

```
# データ取り込み用設定
FOLDER=data
PATTERN=(Cond|User|test)
DB=database/sensor_data.duckdb
ENCODING=utf-8
PLANT=AAA
MACHINE_ID=No.1
DATA_LABEL=2024

# メモリ最適化設定
BATCH_SIZE=5
CHUNK_SIZE=10000
```

## 基本的な使用方法

### 1. 設定ファイルの準備

プロジェクトのルートディレクトリに`.env`ファイルを作成し、必要な設定を記述します。

### 2. データファイルの配置

`.env`ファイルで指定した`FOLDER`パスに処理対象のCSVファイルやZIPファイルを配置します。

### 3. ツールの実行

コマンドラインから以下のコマンドを実行します：

```bash
uv run main.py
```

または、Pythonを直接使用する場合：

```bash
python main.py
```

### コマンドライン引数の使用

バージョン2.1.0以降では、`.env`ファイルの設定値をコマンドライン引数で上書きすることができます。コマンドライン引数で指定された値は、`.env`ファイルの値よりも優先されます。

```bash
python main.py --folder data/new_folder --pattern "Test.*" --db custom_db.duckdb
```

#### 利用可能なコマンドライン引数

| 引数 | 説明 | 対応する環境変数 |
|------|------|----------------|
| `--folder` | 処理対象のフォルダパス | `FOLDER` |
| `--pattern` | CSVファイル検索パターン | `PATTERN` |
| `--db` | データベースファイルのパス | `DB` |
| `--encoding` | CSVファイルのエンコーディング | `ENCODING` |
| `--plant` | プラント名 | `PLANT` |
| `--machine-id` | 機器ID | `MACHINE_ID` |
| `--data-label` | データラベル | `DATA_LABEL` |
| `--batch-size` | バッチサイズ | `BATCH_SIZE` |
| `--chunk-size` | チャンクサイズ | `CHUNK_SIZE` |
| `--max-temp-directory-size` | DuckDBの一時ディレクトリの最大サイズ | `MAX_TEMP_DIRECTORY_SIZE` |

コマンドライン引数のヘルプを表示するには：

```bash
python main.py --help
```

#### 使用例

1. 異なるフォルダのデータを処理する：
   ```bash
   python main.py --folder backup/data
   ```

2. 異なるパターンでファイルを検索する：
   ```bash
   python main.py --pattern "Sensor.*"
   ```

3. 複数の設定を変更する：
   ```bash
   python main.py --folder test_data --pattern ".*\.csv" --plant "BBB" --machine-id "No.2"
   ```

4. メモリ最適化設定を変更する：
   ```bash
   python main.py --batch-size 1 --chunk-size 5000
   ```

コマンドライン引数で指定されなかった設定値は、`.env`ファイルから読み込まれます。

### 4. 実行結果の確認

ツールの実行結果は、コンソールに表示されるログメッセージで確認できます。また、以下の場所に処理結果が保存されます：

- **処理済みデータ**: `.env`ファイルの`OUTPUT_DIR`で指定したディレクトリ（デフォルト: `processed`）にParquetファイルとして保存されます
- **データベース**: `.env`ファイルの`DB`で指定したパス（デフォルト: `sensor_data.duckdb`）にDuckDBデータベースファイルとして保存されます
- **処理済みファイルリスト**: `.env`ファイルの`PROCESSED_MARKER`で指定したファイル（デフォルト: `processed_files.json`）に保存されます

## 実行結果の見方

### ログメッセージ

ツールの実行中には、以下のようなログメッセージが表示されます：

```
2025-04-03 10:00:00 - INFO - 設定情報:
2025-04-03 10:00:00 - INFO - FOLDER: data
2025-04-03 10:00:00 - INFO - PATTERN: (Cond|User|test)
...
2025-04-03 10:00:00 - INFO - 以下のフォルダ内でパターン'(Cond|User|test)'に一致するCSVファイルを検索中...
検索フォルダ： data
2025-04-03 10:00:01 - INFO - 見つかったCSVファイル (2件):
1. test_data.csv
2. test_data.zip::Cond_data.csv (ZIPファイル内)
...
2025-04-03 10:00:02 - INFO - データベースに接続しました: database/sensor_data.duckdb
2025-04-03 10:00:02 - INFO - CSVファイルの前処理と直接DBへの投入を開始します...
...
2025-04-03 10:00:03 - INFO - ファイルの前処理が完了しました: data/test_data.csv -> processed/processed_test_data.csv
...
2025-04-03 10:00:04 - INFO - テーブル sensor_data_integrated の構造:
  TIME (TIMESTAMP)
  VALUE (DOUBLE)
  PLANT (VARCHAR)
  MACHINE_ID (VARCHAR)
  DATA_LABEL (VARCHAR)
  SENSOR_ID (VARCHAR)
  SENSOR_NAME (VARCHAR)
  SENSOR_UNIT (VARCHAR)
  file_name (VARCHAR)
2025-04-03 10:00:04 - INFO - インポートされたレコード数: 1000
2025-04-03 10:00:04 - INFO - データベース接続を閉じました
2025-04-03 10:00:04 - INFO - データベースへの直接取り込みが完了しました。
```

### 処理済みデータ（Parquetファイル）

処理結果は、`OUTPUT_DIR`で指定したディレクトリに以下のようなParquetファイルとして保存されます：

- **個別ファイルの処理結果**: `processed_<元ファイル名>.parquet`
- **統合データ**: `integrated_data.parquet`

これらのファイルは、[Apache Parquet](https://parquet.apache.org/)形式で保存されており、PandasやPolarsなどのデータ分析ライブラリで読み込むことができます。

```python
import polars as pl

# Parquetファイルの読み込み
df = pl.read_parquet("processed/integrated_data.parquet")
print(df)
```

### データベース（DuckDB）

処理したデータは、`DB`で指定したDuckDBデータベースファイルに`sensor_data_integrated`テーブルとして保存されます。DuckDBは標準的なSQLクエリをサポートしているため、以下のようにしてデータにアクセスできます：

```python
import duckdb

# データベースに接続
conn = duckdb.connect("database/sensor_data.duckdb")

# データの取得
result = conn.execute("SELECT * FROM sensor_data_integrated LIMIT 10").fetchall()
print(result)

# 接続を閉じる
conn.close()
```

## 処理済みファイルの管理

ツールは、処理済みのファイルを`PROCESSED_MARKER`で指定したJSONファイル（デフォルト: `processed_files.json`）に記録します。このファイルには、処理済みファイルのパスとハッシュ値が保存されており、同じファイルが再度処理されることを防止します。

処理済みファイルを再度処理したい場合は、以下のいずれかの方法を使用します：

1. `processed_files.json`ファイルを削除する
2. `processed_files.json`ファイルから該当するファイルのエントリを削除する
3. 元のCSVファイルを変更する（ハッシュ値が変わるため、新しいファイルとして認識される）

## よくある使用シナリオ

### シナリオ1: 新しいCSVファイルの処理

1. 新しいCSVファイルを`FOLDER`で指定したディレクトリに配置する
2. ツールを実行する
3. 新しいファイルのみが処理され、データベースに追加される

### シナリオ2: 設定の変更

1. `.env`ファイルの設定を変更する（例: `PLANT`や`MACHINE_ID`の値を変更）
2. `processed_files.json`ファイルを削除する（すべてのファイルを再処理するため）
3. ツールを実行する
4. すべてのファイルが新しい設定で再処理される

### シナリオ3: 特定のパターンに一致するファイルのみを処理

1. `.env`ファイルの`PATTERN`設定を変更する（例: `User`のみに一致するパターンに変更）
2. ツールを実行する
3. 新しいパターンに一致するファイルのみが処理される

### シナリオ4: 大量のデータを処理する場合のメモリ最適化

1. `.env`ファイルのメモリ最適化設定を調整する
   ```
   # メモリ最適化設定
   BATCH_SIZE=1     # 非常に大きなファイルの場合は1に設定
   CHUNK_SIZE=5000  # 行数を小さくして一度に処理するデータ量を減らす
   ```
2. ツールを実行する
3. メモリ使用量が削減され、「Out of Memory Error」を回避できる

メモリ使用量と処理速度はトレードオフの関係にあります。以下に、異なるデータサイズに対する推奨設定を示します：

| データサイズ | BATCH_SIZE | CHUNK_SIZE | 備考 |
|------------|------------|------------|------|
| 小（数MB以下） | 10 | 20000 | 高速処理優先 |
| 中（数十MB） | 5 | 10000 | バランス重視 |
| 大（数百MB以上） | 1 | 5000 | メモリ使用量優先 |

## コマンドライン引数

バージョン2.1.0以降では、`.env`ファイルの設定値をコマンドライン引数で上書きすることができます。コマンドライン引数で指定された値は、`.env`ファイルの値よりも優先されます。

```bash
python main.py --folder data/new_folder --pattern "Test.*" --db custom_db.duckdb
```

### 利用可能なコマンドライン引数

| 引数 | 説明 | 対応する環境変数 |
|------|------|----------------|
| `--folder` | 処理対象のフォルダパス | `FOLDER` |
| `--pattern` | CSVファイル検索パターン | `PATTERN` |
| `--db` | データベースファイルのパス | `DB` |
| `--encoding` | CSVファイルのエンコーディング | `ENCODING` |
| `--plant` | プラント名 | `PLANT` |
| `--machine-id` | 機器ID | `MACHINE_ID` |
| `--data-label` | データラベル | `DATA_LABEL` |
| `--batch-size` | バッチサイズ | `BATCH_SIZE` |
| `--chunk-size` | チャンクサイズ | `CHUNK_SIZE` |
| `--max-temp-directory-size` | DuckDBの一時ディレクトリの最大サイズ | `MAX_TEMP_DIRECTORY_SIZE` |

コマンドライン引数のヘルプを表示するには：

```bash
python main.py --help
```

### 使用例

1. 異なるフォルダのデータを処理する：
   ```bash
   python main.py --folder backup/data
   ```

2. 異なるパターンでファイルを検索する：
   ```bash
   python main.py --pattern "Sensor.*"
   ```

3. 複数の設定を変更する：
   ```bash
   python main.py --folder test_data --pattern ".*\.csv" --plant "BBB" --machine-id "No.2"
   ```

4. メモリ最適化設定を変更する：
   ```bash
   python main.py --batch-size 1 --chunk-size 5000
   ```

コマンドライン引数で指定されなかった設定値は、`.env`ファイルから読み込まれます。

## 次のステップ

- [ファイル形式ガイド](file_formats.md)を参照して、対応しているCSV形式の詳細を確認する
- [技術リファレンス](technical_reference.md)を参照して、ツールの内部動作について理解する
- [トラブルシューティング](troubleshooting.md)を参照して、一般的な問題と解決方法を確認する
