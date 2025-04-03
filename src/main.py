"""CSVファイル検索、前処理、データベース取り込みのメインモジュール"""

import argparse
import logging
import time
from pathlib import Path

from src.config import Config
from src.db import DatabaseManager
from src.file_finder import find_csv_files
from src.logger import log_csv_files, log_search_start, setup_logger
from src.preprocessor import CsvPreprocessor, PreprocessorConfig


def parse_args():
    """コマンドライン引数を解析する

    Returns:
        argparse.Namespace: 解析されたコマンドライン引数
    """
    parser = argparse.ArgumentParser(
        description="CSVファイルをデータベースに取り込むツール"
    )

    # データ取り込み用設定
    parser.add_argument("--folder", help="処理対象のフォルダパス")
    parser.add_argument("--pattern", help="CSVファイル検索パターン")
    parser.add_argument("--db", help="データベースファイルのパス")
    parser.add_argument("--encoding", help="CSVファイルのエンコーディング")
    parser.add_argument("--plant", help="プラント名")
    parser.add_argument("--machine-id", dest="machine_id", help="機器ID")
    parser.add_argument("--data-label", dest="data_label", help="データラベル")

    # メモリ最適化設定
    parser.add_argument(
        "--batch-size", dest="batch_size", type=int, help="バッチサイズ"
    )
    parser.add_argument(
        "--chunk-size", dest="chunk_size", type=int, help="チャンクサイズ"
    )

    # DuckDB設定
    parser.add_argument(
        "--max-temp-directory-size",
        dest="max_temp_directory_size",
        help="DuckDBの一時ディレクトリの最大サイズ",
    )

    return parser.parse_args()


def main():
    """メイン実行関数"""
    # ロガーの設定
    setup_logger()

    # コマンドライン引数の解析
    args = parse_args()

    # 設定の読み込みと検証（コマンドライン引数を渡す）
    config = Config(args)
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

    # データベースへの接続
    logging.info("データベースへの接続を開始します...")
    db_manager = DatabaseManager(config.db, config, config.encoding)
    if not db_manager.connect():
        logging.error("データベースへの接続に失敗しました。")
        return

    # 前処理と直接DBへの投入を実行
    logging.info("CSVファイルの前処理と直接DBへの投入を開始します...")
    preprocessor_config = PreprocessorConfig()
    preprocessor = CsvPreprocessor(preprocessor_config)

    # 処理時間の計測開始
    start_time = time.time()

    # テーブル名の設定
    table_name = "sensor_data_integrated"

    # CSVファイルを処理して直接DBに投入
    total_records = preprocessor.process_all_files_to_db(
        csv_files, db_manager, table_name, batch_size=config.batch_size
    )

    # 処理時間の計測終了
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"前処理と直接DBへの投入の実行時間: {processing_time:.3f}秒")

    if total_records > 0:
        # テーブル情報を表示
        table_info = db_manager.get_table_info(table_name)
        if table_info:
            logging.info(f"テーブル {table_name} の構造:")
            for col in table_info:
                logging.info(f"  {col[1]} ({col[2]})")

            logging.info(f"インポートされたレコード数: {total_records}")
    else:
        logging.info("処理対象のCSVファイルはありませんでした。")

    db_manager.close()
    logging.info("データベースへの直接取り込みが完了しました。")


if __name__ == "__main__":
    main()
