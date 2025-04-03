import logging
import os
from pathlib import Path

from dotenv import load_dotenv


class Config:
    """アプリケーション設定を管理するクラス"""

    def __init__(self):
        """環境変数から設定を読み込む"""
        # .envファイルを読み込む
        load_dotenv()

        # 必須の環境変数
        self.folder = os.getenv("FOLDER")
        self.pattern = os.getenv("PATTERN")

        # オプションの環境変数（デフォルト値付き）
        self.db = os.getenv("DB", "sensor_data.duckdb")
        self.encoding = os.getenv("ENCODING", "utf-8")
        self.plant = os.getenv("PLANT", "")
        self.machine_id = os.getenv("MACHINE_ID", "")
        self.data_label = os.getenv("DATA_LABEL", "")

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
