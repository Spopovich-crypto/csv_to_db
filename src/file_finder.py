import logging
import re
import zipfile
from pathlib import Path


def find_csv_files(base_folder, pattern_str):
    """指定されたフォルダ内でパターンに一致するCSVファイルを検索する

    Args:
        base_folder (str): 検索対象のベースフォルダパス
        pattern_str (str): ファイル名の検索パターン（正規表現）

    Returns:
        list: 見つかったCSVファイルの情報リスト
    """
    pattern = re.compile(pattern_str)
    csv_files = []
    base_path = Path(base_folder)

    # 通常のファイルシステム内を検索（再帰的に全てのファイルを取得）
    for file_path in base_path.glob("**/*"):
        if file_path.is_file():
            # CSVファイルの検索
            if file_path.suffix.lower() == ".csv" and pattern.search(file_path.name):
                csv_files.append({"path": str(file_path), "type": "file"})

            # ZIPファイルの検索と処理
            elif file_path.suffix.lower() == ".zip":
                csv_files.extend(_process_zip_file(file_path, pattern))

    return csv_files


def _process_zip_file(file_path, pattern):
    """ZIPファイル内のCSVファイルを処理する

    Args:
        file_path (Path): ZIPファイルのパス
        pattern (re.Pattern): ファイル名の検索パターン

    Returns:
        list: ZIPファイル内で見つかったCSVファイルの情報リスト
    """
    csv_files = []
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            for zip_info in zip_ref.infolist():
                zip_file_name = Path(zip_info.filename).name
                if zip_info.filename.endswith(".csv") and pattern.search(zip_file_name):
                    csv_files.append(
                        {
                            "path": f"{file_path}::{zip_info.filename}",
                            "type": "zip",
                            "zip_path": str(file_path),
                            "file_in_zip": zip_info.filename,
                        }
                    )
    except zipfile.BadZipFile:
        logging.warning(f"不正なZIPファイル: {file_path}")
    except Exception as e:
        logging.error(f"ZIPファイル処理中のエラー {file_path}: {str(e)}")

    return csv_files
