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

    def import_dataframe(self, df, table_name, if_exists="append"):
        """Polarsデータフレームを直接データベースにインポートする

        Args:
            df (pl.DataFrame): インポートするデータフレーム
            table_name (str): インポート先のテーブル名
            if_exists (str, optional): テーブルが存在する場合の動作。"append"または"replace"。デフォルトは"append"。

        Returns:
            bool: インポートに成功したかどうか
        """
        if not self.connection:
            logging.error("データベースに接続されていません")
            return False

        try:
            # テーブルが存在するか確認
            check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            result = self.connection.execute(check_query).fetchall()

            if result and if_exists == "replace":
                # テーブルを削除
                self.connection.execute(f"DROP TABLE {table_name}")

            # データフレームをDuckDBに直接インポート
            # DuckDBはPolarsと互換性があるため、直接インポートが可能
            if not result or if_exists == "replace":
                # テーブルが存在しない場合、または置き換える場合は新しいテーブルを作成
                # TIMEカラムがdatetime型の場合はTIMESTAMP型として保存
                schema_info = df.schema
                time_col_type = schema_info.get("TIME")

                if time_col_type and str(time_col_type).lower() == "datetime":
                    # TIMEカラムがdatetime型の場合、明示的にTIMESTAMP型として保存
                    # 一時テーブルを作成してから適切な型でメインテーブルを作成
                    self.connection.execute(
                        f"CREATE TEMP TABLE temp_df AS SELECT * FROM df"
                    )

                    # カラム定義を取得
                    columns_info = self.connection.execute(
                        "PRAGMA table_info(temp_df)"
                    ).fetchall()
                    column_defs = []

                    for col_info in columns_info:
                        col_name = col_info[1]
                        col_type = col_info[2]

                        if col_name == "TIME":
                            col_type = "TIMESTAMP"

                        column_defs.append(f'"{col_name}" {col_type}')

                    # 適切な型でテーブルを作成
                    columns_str = ", ".join(column_defs)
                    self.connection.execute(
                        f"CREATE TABLE {table_name} ({columns_str})"
                    )

                    # データを挿入
                    self.connection.execute(
                        f"INSERT INTO {table_name} SELECT * FROM temp_df"
                    )

                    # 一時テーブルを削除
                    self.connection.execute("DROP TABLE temp_df")
                else:
                    # 通常の方法でテーブルを作成
                    self.connection.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM df"
                    )
            else:
                # テーブルが存在する場合は追加
                # TIMEカラムがdatetime型の場合、既存のテーブルのTIMEカラムの型を確認
                schema_info = df.schema
                time_col_type = schema_info.get("TIME")

                if time_col_type and str(time_col_type).lower() == "datetime":
                    # テーブル情報を取得
                    table_info = self.get_table_info(table_name)
                    time_col_info = next(
                        (col for col in table_info if col[1] == "TIME"), None
                    )

                    if time_col_info and time_col_info[2] != "TIMESTAMP":
                        # TIMEカラムの型がTIMESTAMPでない場合、テーブルを再作成
                        logging.info(
                            f"テーブル {table_name} のTIMEカラムをTIMESTAMP型に変更します"
                        )

                        # 既存のデータを一時テーブルに保存
                        self.connection.execute(
                            f"CREATE TEMP TABLE temp_data AS SELECT * FROM {table_name}"
                        )

                        # 既存のテーブルを削除
                        self.connection.execute(f"DROP TABLE {table_name}")

                        # 新しいテーブルを作成（TIMEカラムをTIMESTAMP型に）
                        columns_info = self.connection.execute(
                            "PRAGMA table_info(temp_data)"
                        ).fetchall()
                        column_defs = []

                        for col_info in columns_info:
                            col_name = col_info[1]
                            col_type = col_info[2]

                            if col_name == "TIME":
                                col_type = "TIMESTAMP"

                            column_defs.append(f'"{col_name}" {col_type}')

                        # 適切な型でテーブルを作成
                        columns_str = ", ".join(column_defs)
                        self.connection.execute(
                            f"CREATE TABLE {table_name} ({columns_str})"
                        )

                        # 既存のデータを挿入（TIMEカラムを適切に変換）
                        self.connection.execute(f"""
                            INSERT INTO {table_name}
                            SELECT * FROM temp_data
                        """)

                        # 一時テーブルを削除
                        self.connection.execute("DROP TABLE temp_data")

                # データを挿入
                self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM df")

            logging.info(
                f"データフレームをインポートしました: {len(df)}行 -> {table_name}"
            )
            return True
        except Exception as e:
            logging.error(f"データフレームインポートエラー: {str(e)}")
            return False
