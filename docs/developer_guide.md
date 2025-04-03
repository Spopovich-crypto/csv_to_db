# 開発者ガイド

このドキュメントでは、CSV to DBツールの開発者向けの情報を提供します。ツールの拡張方法やカスタマイズ方法について説明します。

## コード構造

CSV to DBツールのコードは、以下のような構造になっています：

```
csv-to-db/
├── main.py             # エントリーポイント
├── src/                # ソースコード
│   ├── __init__.py     # パッケージ初期化
│   ├── main.py         # メイン処理
│   ├── config.py       # 設定管理
│   ├── file_finder.py  # ファイル検索
│   ├── logger.py       # ロギング
│   ├── preprocessor.py # 前処理
│   └── db.py           # データベース操作
├── tests/              # テストコード（将来的に追加予定）
├── data/               # データフォルダ
├── processed/          # 処理結果フォルダ
└── database/           # データベースフォルダ
```

### 主要クラスとモジュール

#### `main.py`

プロジェクトのエントリーポイントです。`src/main.py`の`main`関数を呼び出します。

#### `src/main.py`

メイン処理を行うモジュールです。以下の処理を行います：

1. ロガーの設定
2. 設定の読み込みと検証
3. CSVファイルの検索
4. データベースへの接続
5. CSVファイルの前処理とデータベースへの投入
6. 処理結果の表示

#### `src/config.py`

設定を管理するモジュールです。以下のクラスを定義しています：

- `Config`: アプリケーション設定を管理するクラス

#### `src/file_finder.py`

ファイル検索を行うモジュールです。以下の関数を定義しています：

- `find_csv_files`: 指定されたフォルダ内でパターンに一致するCSVファイルを検索する関数
- `_process_zip_file`: ZIPファイル内のCSVファイルを処理する関数（内部関数）

#### `src/logger.py`

ロギングを管理するモジュールです。以下の関数を定義しています：

- `setup_logger`: アプリケーションのロガーを設定する関数
- `log_csv_files`: 見つかったCSVファイルの情報をログに出力する関数
- `log_search_start`: 検索開始情報をログに出力する関数

#### `src/preprocessor.py`

前処理を行うモジュールです。以下のクラスを定義しています：

- `PreprocessorConfig`: 前処理の設定を管理するクラス
- `CsvPreprocessor`: CSVファイルの前処理を行うクラス

#### `src/db.py`

データベース操作を行うモジュールです。以下のクラスを定義しています：

- `DatabaseManager`: データベース操作を管理するクラス

## 拡張方法

CSV to DBツールは、以下の方法で拡張することができます：

### 1. 新しいCSV形式のサポート

新しいCSV形式をサポートするには、`src/preprocessor.py`の`CsvPreprocessor`クラスを拡張します。

#### 例: 異なるヘッダー構造を持つCSVファイルのサポート

```python
def _read_new_csv_format(self, file_path):
    """新しい形式のCSVファイルを読み込む

    Args:
        file_path (str): CSVファイルのパス

    Returns:
        dict: 読み込んだデータ（ヘッダー情報とデータフレーム）
    """
    try:
        # ファイルの内容を行ごとに読み込む
        with open(file_path, "r", encoding=self.config.encoding) as f:
            lines = f.readlines()

        # 新しい形式に合わせたヘッダー処理
        # ...

        # データフレームを作成
        # ...

        return {"headers": headers, "data": df, "format": "new_format"}

    except Exception as e:
        logging.error(f"新形式CSVファイル読み込みエラー {file_path}: {str(e)}")
        return None
```

そして、`process_file`メソッド内で新しい形式を検出して処理するように変更します：

```python
def process_file(self, file_info):
    # ...
    
    # CSVファイルの読み込み（形式に応じて処理）
    if file_type == "file":
        # ファイル名やパスから形式を判断
        if file_name.startswith("new_format_"):
            data = self._read_new_csv_format(file_path)
        else:
            data = self._read_special_csv(file_path)
    # ...
```

### 2. 新しいデータベースのサポート

新しいデータベースをサポートするには、`src/db.py`に新しいデータベース管理クラスを追加します。

#### 例: SQLiteデータベースのサポート

```python
import sqlite3

class SqliteDatabaseManager:
    """SQLiteデータベース操作を管理するクラス"""

    def __init__(self, db_path, encoding="utf-8"):
        """SQLiteデータベースマネージャーを初期化する

        Args:
            db_path (str): データベースファイルのパス
            encoding (str, optional): 使用するエンコーディング. デフォルトは "utf-8".
        """
        self.db_path = Path(db_path)
        self.encoding = encoding
        self.connection = None

        # データベースディレクトリが存在しない場合は作成
        db_dir = self.db_path.parent
        if not db_dir.exists():
            db_dir.mkdir(parents=True, exist_ok=True)

    def connect(self):
        """データベースに接続する

        Returns:
            bool: 接続に成功したかどうか
        """
        try:
            self.connection = sqlite3.connect(str(self.db_path))
            logging.info(f"SQLiteデータベースに接続しました: {self.db_path}")
            return True
        except Exception as e:
            logging.error(f"SQLiteデータベース接続エラー: {str(e)}")
            return False

    # 他のメソッドも同様に実装
    # ...
```

そして、`src/main.py`内でデータベースマネージャーを選択するように変更します：

```python
# データベースへの接続
logging.info("データベースへの接続を開始します...")
if config.db.endswith(".sqlite"):
    db_manager = SqliteDatabaseManager(config.db, config.encoding)
else:
    db_manager = DatabaseManager(config.db, config.encoding)

if not db_manager.connect():
    logging.error("データベースへの接続に失敗しました。")
    return
```

### 3. 新しいファイル形式のサポート

新しいファイル形式をサポートするには、`src/file_finder.py`の`find_csv_files`関数を拡張します。

#### 例: Excelファイルのサポート

```python
import pandas as pd

def find_data_files(base_folder, pattern_str):
    """指定されたフォルダ内でパターンに一致するデータファイルを検索する

    Args:
        base_folder (str): 検索対象のベースフォルダパス
        pattern_str (str): ファイル名の検索パターン（正規表現）

    Returns:
        list: 見つかったデータファイルの情報リスト
    """
    pattern = re.compile(pattern_str)
    data_files = []
    base_path = Path(base_folder)

    # 通常のファイルシステム内を検索（再帰的に全てのファイルを取得）
    for file_path in base_path.glob("**/*"):
        if file_path.is_file():
            # CSVファイルの検索
            if file_path.suffix.lower() == ".csv" and pattern.search(file_path.name):
                data_files.append({"path": str(file_path), "type": "csv"})

            # Excelファイルの検索
            elif file_path.suffix.lower() in [".xlsx", ".xls"] and pattern.search(file_path.name):
                data_files.append({"path": str(file_path), "type": "excel"})

            # ZIPファイルの検索と処理
            elif file_path.suffix.lower() == ".zip":
                data_files.extend(_process_zip_file(file_path, pattern))

    return data_files
```

そして、`src/preprocessor.py`に新しいファイル形式を処理するメソッドを追加します：

```python
def _read_excel_file(self, file_path):
    """Excelファイルを読み込む

    Args:
        file_path (str): Excelファイルのパス

    Returns:
        dict: 読み込んだデータ（ヘッダー情報とデータフレーム）
    """
    try:
        # pandasを使用してExcelファイルを読み込む
        excel_df = pd.read_excel(file_path)
        
        # 必要な処理を行い、Polarsデータフレームに変換
        # ...

        return {"headers": headers, "data": df, "format": "excel"}

    except Exception as e:
        logging.error(f"Excelファイル読み込みエラー {file_path}: {str(e)}")
        return None
```

### 4. 新しい前処理の追加

新しい前処理を追加するには、`src/preprocessor.py`の`CsvPreprocessor`クラスに新しいメソッドを追加します。

#### 例: データのフィルタリング

```python
def _filter_data(self, df, filter_conditions):
    """データをフィルタリングする

    Args:
        df (pl.DataFrame): フィルタリング対象のデータフレーム
        filter_conditions (dict): フィルタリング条件

    Returns:
        pl.DataFrame: フィルタリング後のデータフレーム
    """
    filtered_df = df

    # 各フィルタリング条件を適用
    for column, condition in filter_conditions.items():
        if column in df.columns:
            if isinstance(condition, tuple) and len(condition) == 2:
                # 範囲フィルタリング（例: (">=", 10)）
                op, value = condition
                if op == ">=":
                    filtered_df = filtered_df.filter(pl.col(column) >= value)
                elif op == ">":
                    filtered_df = filtered_df.filter(pl.col(column) > value)
                elif op == "<=":
                    filtered_df = filtered_df.filter(pl.col(column) <= value)
                elif op == "<":
                    filtered_df = filtered_df.filter(pl.col(column) < value)
                elif op == "==":
                    filtered_df = filtered_df.filter(pl.col(column) == value)
                elif op == "!=":
                    filtered_df = filtered_df.filter(pl.col(column) != value)
            else:
                # 値一致フィルタリング
                filtered_df = filtered_df.filter(pl.col(column) == condition)

    return filtered_df
```

そして、`process_file`メソッド内でフィルタリングを適用するように変更します：

```python
def process_file(self, file_info, filter_conditions=None):
    # ...
    
    # 縦持ちデータに変換
    vertical_data = self._transform_to_vertical(data, file_name)
    
    # フィルタリング条件が指定されている場合は適用
    if filter_conditions:
        vertical_data = self._filter_data(vertical_data, filter_conditions)
    
    # ...
```

## テスト方法

CSV to DBツールのテストは、以下の方法で行うことができます：

### 1. テスト用データの作成

テスト用のCSVファイルとZIPファイルを作成するには、`create_test_zip.py`スクリプトを使用します：

```bash
python create_test_zip.py
```

このスクリプトは、`data`フォルダ内にテスト用のCSVファイルとZIPファイルを作成します。

### 2. 単体テストの作成

将来的には、`tests`フォルダ内に単体テストを追加することを推奨します。以下は、`pytest`を使用した単体テストの例です：

```python
# tests/test_file_finder.py
import os
import tempfile
from pathlib import Path
import zipfile

import pytest

from src.file_finder import find_csv_files

def test_find_csv_files():
    # テスト用の一時ディレクトリを作成
    with tempfile.TemporaryDirectory() as temp_dir:
        # テスト用のCSVファイルを作成
        csv_path = Path(temp_dir) / "test_data.csv"
        with open(csv_path, "w") as f:
            f.write("test,data\n1,2\n")
        
        # テスト用のZIPファイルを作成
        zip_path = Path(temp_dir) / "test_archive.zip"
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("Cond_data.csv", "test,data\n1,2\n")
        
        # ファイル検索を実行
        csv_files = find_csv_files(temp_dir, "(test|Cond)")
        
        # 結果を検証
        assert len(csv_files) == 2
        assert csv_files[0]["path"] == str(csv_path)
        assert csv_files[0]["type"] == "file"
        assert csv_files[1]["type"] == "zip"
        assert "test_archive.zip::Cond_data.csv" in csv_files[1]["path"]
```

### 3. 統合テストの実行

ツール全体の動作をテストするには、以下のコマンドを実行します：

```bash
python main.py
```

テスト実行前に、`.env`ファイルに適切な設定を記述してください：

```
FOLDER=data
PATTERN=(Cond|User|test)
DB=database/test_db.duckdb
ENCODING=utf-8
PLANT=TEST
MACHINE_ID=TEST01
DATA_LABEL=TEST
```

## コーディング規約

CSV to DBツールの開発には、以下のコーディング規約を推奨します：

1. **PEP 8**: Pythonの標準的なコーディングスタイルガイドに従う
2. **型ヒント**: 関数やメソッドの引数と戻り値に型ヒントを追加する
3. **ドキュメンテーション**: 関数やクラスにはdocstringを追加する
4. **エラー処理**: 適切な例外処理を行い、エラーメッセージをログに出力する
5. **テスト**: 新しい機能には単体テストを追加する

## 貢献方法

CSV to DBツールへの貢献を歓迎します。以下の手順で貢献することができます：

1. リポジトリをフォークする
2. 新しいブランチを作成する（`git checkout -b feature/new-feature`）
3. 変更を加える
4. テストを実行して変更が正常に動作することを確認する
5. 変更をコミットする（`git commit -m "Add new feature"`）
6. フォークしたリポジトリにプッシュする（`git push origin feature/new-feature`）
7. プルリクエストを作成する

## 次のステップ

- [技術リファレンス](technical_reference.md)に戻って、ツールの内部動作について理解する
- [トラブルシューティング](troubleshooting.md)を参照して、一般的な問題と解決方法を確認する
