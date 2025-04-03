"""CSVファイル検索、前処理、データベース取り込みのメインモジュール"""

import logging
from pathlib import Path

from src.config import Config
from src.db import DatabaseManager
from src.file_finder import find_csv_files
from src.logger import log_csv_files, log_search_start, setup_logger
from src.preprocessor import CsvPreprocessor, PreprocessorConfig


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
    log_csv_files(csv_files, config.folder)

    if not csv_files:
        logging.info("処理対象のCSVファイルが見つかりませんでした。")
        return

    # 前処理の実行
    logging.info("CSVファイルの前処理を開始します...")
    preprocessor_config = PreprocessorConfig()
    preprocessor = CsvPreprocessor(preprocessor_config)

    # 複数のCSVファイルを処理し、統合する
    integrated_data = preprocessor.process_all_files(csv_files)

    if integrated_data is None:
        logging.info("前処理が必要なCSVファイルはありませんでした。")
        return

    # データベースへの取り込み処理
    logging.info("データベースへの取り込みを開始します...")
    db_manager = DatabaseManager(config.db, config.encoding)
    if db_manager.connect():
        # 将来的な実装: 統合データをデータベースに取り込む
        # integrated_path = Path(preprocessor_config.output_dir) / "integrated_data.parquet"
        # table_name = "sensor_data_integrated"
        # db_manager.import_parquet(str(integrated_path), table_name)

        db_manager.close()
        logging.info("データベースへの取り込みが完了しました。")
    else:
        logging.error("データベースへの接続に失敗しました。")


if __name__ == "__main__":
    main()
