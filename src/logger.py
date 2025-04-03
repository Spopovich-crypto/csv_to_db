import logging


def setup_logger():
    """アプリケーションのロガーを設定する"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def log_csv_files(csv_files):
    """見つかったCSVファイルの情報をログに出力する

    Args:
        csv_files (list): CSVファイル情報のリスト
    """
    if csv_files:
        logging.info(f"見つかったCSVファイル ({len(csv_files)}件):")
        for i, file_info in enumerate(csv_files, 1):
            if file_info["type"] == "file":
                logging.info(f"{i}. {file_info['path']}")
            else:  # zip
                logging.info(
                    f"{i}. {file_info['zip_path']}::{file_info['file_in_zip']} (ZIPファイル内)"
                )
    else:
        logging.info("条件に一致するCSVファイルは見つかりませんでした。")


def log_search_start(folder, pattern):
    """検索開始情報をログに出力する

    Args:
        folder (str): 検索対象フォルダ
        pattern (str): 検索パターン
    """
    logging.info(
        f"以下のフォルダ内でパターン'{pattern}'に一致するCSVファイルを検索中...\n"
        f"検索フォルダ： {folder}"
    )
