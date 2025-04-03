"""CSVファイル検索とデータベース取り込みのメインモジュール"""

import logging

from src.config import Config
from src.file_finder import find_csv_files
from src.logger import log_csv_files, log_search_start, setup_logger


def main():
    """メイン実行関数"""
    # ロガーの設定
    setup_logger()

    # 設定の読み込みと検証
    config = Config()
    config.log_settings()

    if not config.validate():
        return

    # 検索開始のログ出力
    log_search_start(config.folder, config.pattern)

    # 指定されたパターンに一致するCSVファイルを検索
    csv_files = find_csv_files(config.folder, config.pattern)

    # 結果の表示
    log_csv_files(csv_files)

    # 将来的な拡張: データベースへの取り込み処理
    # from src.db import DatabaseManager
    # db_manager = DatabaseManager(config.db, config.encoding)
    # db_manager.connect()
    # for file_info in csv_files:
    #     # ファイルからデータを取り込む処理
    #     pass
    # db_manager.close()


if __name__ == "__main__":
    main()
