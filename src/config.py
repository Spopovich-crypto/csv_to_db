import logging
import os
from pathlib import Path

from dotenv import load_dotenv


class Config:
    """アプリケーション設定を管理するクラス"""

    def __init__(self, cli_args=None):
        """環境変数とコマンドライン引数から設定を読み込む

        Args:
            cli_args: コマンドライン引数（Namespaceオブジェクト）
        """
        # .envファイルを強制的に再読み込み
        load_dotenv(override=True)

        # 必須の環境変数（コマンドライン引数を優先）
        self.folder = getattr(cli_args, "folder", None) or os.getenv("FOLDER")
        self.pattern = getattr(cli_args, "pattern", None) or os.getenv("PATTERN")

        # オプションの環境変数（デフォルト値付き、コマンドライン引数を優先）
        self.db = getattr(cli_args, "db", None) or os.getenv("DB", "sensor_data.duckdb")
        self.encoding = getattr(cli_args, "encoding", None) or os.getenv(
            "ENCODING", "utf-8"
        )
        self.plant = getattr(cli_args, "plant", None) or os.getenv("PLANT", "")
        self.machine_id = getattr(cli_args, "machine_id", None) or os.getenv(
            "MACHINE_ID", ""
        )
        self.data_label = getattr(cli_args, "data_label", None) or os.getenv(
            "DATA_LABEL", ""
        )

        # メモリ最適化設定（コマンドライン引数を優先）
        batch_size_str = getattr(cli_args, "batch_size", None) or os.getenv(
            "BATCH_SIZE", "5"
        )
        chunk_size_str = getattr(cli_args, "chunk_size", None) or os.getenv(
            "CHUNK_SIZE", "10000"
        )
        self.batch_size = int(batch_size_str)
        self.chunk_size = int(chunk_size_str)

        # DuckDB設定（コマンドライン引数を優先）
        self.max_temp_directory_size = getattr(
            cli_args, "max_temp_directory_size", None
        ) or os.getenv("MAX_TEMP_DIRECTORY_SIZE", "20GB")

    def validate(self):
        """設定値を検証する

        Returns:
            bool: 設定が有効かどうか
        """
        if not self.folder or not self.pattern:
            logging.error(".envファイルにFOLDERまたはPATTERNが設定されていません。")
            return False

        # フォルダの存在確認
        folder_path = Path(self.folder)
        if not folder_path.exists():
            logging.error(f"指定されたフォルダが存在しません: {self.folder}")
            return False

        return True

    def log_settings(self):
        """設定情報をログに出力する"""
        logging.debug("設定情報:")
        logging.debug(f"FOLDER: {self.folder}")
        logging.debug(f"PATTERN: {self.pattern}")
        logging.debug(f"DB: {self.db}")
        logging.debug(f"ENCODING: {self.encoding}")
        logging.debug(f"PLANT: {self.plant}")
        logging.debug(f"MACHINE_ID: {self.machine_id}")
        logging.debug(f"DATA_LABEL: {self.data_label}")
        logging.debug(f"MAX_TEMP_DIRECTORY_SIZE: {self.max_temp_directory_size}")
