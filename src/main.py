"""CSVファイル検索、前処理、データベース取り込みのメインモジュール"""

import logging
import time
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

    # 処理時間の計測開始
    start_time = time.time()

    # 複数のCSVファイルを処理し、統合する
    integrated_data = preprocessor.process_all_files(csv_files)

    # 処理時間の計測終了
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"前処理の実行時間: {processing_time:.3f}秒")

    if integrated_data is None:
        logging.info("前処理が必要なCSVファイルはありませんでした。")
        return

    # データベースへの取り込み処理
    logging.info("データベースへの取り込みを開始します...")
    db_manager = DatabaseManager(config.db, config.encoding)
    if db_manager.connect():
        # 統合データをデータベースに取り込む
        integrated_path = (
            Path(preprocessor_config.output_dir) / "integrated_data.parquet"
        )
        table_name = "sensor_data_integrated"

        # テーブルスキーマの定義（必要に応じて）
        # schema = """
        #     TIME TEXT,
        #     PLANT TEXT,
        #     MACHINE_ID TEXT,
        #     DATA_LABEL TEXT,
        #     SENSOR_ID TEXT,
        #     SENSOR_NAME TEXT,
        #     SENSOR_UNIT TEXT,
        #     VALUE TEXT,
        #     file_name TEXT
        # """
        # db_manager.create_table_if_not_exists(table_name, schema)

        # Parquetファイルからデータをインポート
        if db_manager.import_parquet(str(integrated_path), table_name):
            # テーブル情報を表示
            table_info = db_manager.get_table_info(table_name)
            if table_info:
                logging.info(f"テーブル {table_name} の構造:")
                for col in table_info:
                    logging.info(f"  {col[1]} ({col[2]})")

                # レコード数を取得
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                result = db_manager.execute_query(count_query)
                if result:
                    count = result.fetchone()[0]
                    logging.info(f"インポートされたレコード数: {count}")

        db_manager.close()
        logging.info("データベースへの取り込みが完了しました。")
    else:
        logging.error("データベースへの接続に失敗しました。")


if __name__ == "__main__":
    main()
