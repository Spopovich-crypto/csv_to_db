import logging
import os
from pathlib import Path

import duckdb


class DatabaseManager:
    """データベース操作を管理するクラス"""

    def __init__(self, db_path, encoding="utf-8"):
        """データベースマネージャーを初期化する

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
            self.connection = duckdb.connect(str(self.db_path))
            logging.info(f"データベースに接続しました: {self.db_path}")
            return True
        except Exception as e:
            logging.error(f"データベース接続エラー: {str(e)}")
            return False

    def close(self):
        """データベース接続を閉じる"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logging.info("データベース接続を閉じました")

    def execute_query(self, query, params=None):
        """SQLクエリを実行する

        Args:
            query (str): 実行するSQLクエリ
            params (tuple, optional): クエリパラメータ. デフォルトは None.

        Returns:
            duckdb.DuckDBPyRelation: クエリ結果
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return None

        try:
            if params:
                result = self.connection.execute(query, params)
            else:
                result = self.connection.execute(query)
            return result
        except Exception as e:
            logging.error(f"クエリ実行エラー: {str(e)}")
            logging.error(f"実行クエリ: {query}")
            return None

    def create_table_if_not_exists(self, table_name, schema):
        """テーブルが存在しない場合に作成する

        Args:
            table_name (str): テーブル名
            schema (str): テーブルスキーマ（CREATE TABLE文のカラム定義部分）

        Returns:
            bool: テーブル作成に成功したかどうか
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return False

        try:
            # テーブルが存在するか確認
            check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            result = self.connection.execute(check_query).fetchall()

            if not result:
                # テーブルが存在しない場合は作成
                create_query = f"CREATE TABLE {table_name} ({schema})"
                self.connection.execute(create_query)
                logging.info(f"テーブルを作成しました: {table_name}")
            else:
                logging.info(f"テーブルは既に存在します: {table_name}")

            return True
        except Exception as e:
            logging.error(f"テーブル作成エラー: {str(e)}")
            return False

    def import_csv(self, csv_path, table_name, metadata=None):
        """CSVファイルからデータをインポートする

        Args:
            csv_path (str): CSVファイルのパス
            table_name (str): インポート先のテーブル名
            metadata (dict, optional): 追加のメタデータ. デフォルトは None.

        Returns:
            bool: インポートに成功したかどうか
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return False

        try:
            # CSVファイルからデータをインポート
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
            self.connection.execute(sql)
            logging.info(f"CSVファイルをインポートしました: {csv_path} -> {table_name}")
            return True
        except Exception as e:
            logging.error(f"CSVインポートエラー: {str(e)}")
            return False

    def import_parquet(self, parquet_path, table_name, metadata=None):
        """Parquetファイルからデータをインポートする

        Args:
            parquet_path (str): Parquetファイルのパス
            table_name (str): インポート先のテーブル名
            metadata (dict, optional): 追加のメタデータ. デフォルトは None.

        Returns:
            bool: インポートに成功したかどうか
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return False

        try:
            # Parquetファイルからデータをインポート
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
            self.connection.execute(sql)
            logging.info(
                f"Parquetファイルをインポートしました: {parquet_path} -> {table_name}"
            )
            return True
        except Exception as e:
            logging.error(f"Parquetインポートエラー: {str(e)}")
            return False

    def get_table_info(self, table_name):
        """テーブル情報を取得する

        Args:
            table_name (str): テーブル名

        Returns:
            list: テーブル情報
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return None

        try:
            # テーブル情報を取得
            query = f"PRAGMA table_info({table_name})"
            result = self.connection.execute(query).fetchall()
            return result
        except Exception as e:
            logging.error(f"テーブル情報取得エラー: {str(e)}")
            return None
