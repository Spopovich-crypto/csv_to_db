import os
import re
import zipfile
from pathlib import Path

from dotenv import load_dotenv


def find_csv_files(base_folder, pattern_str):
    """指定されたフォルダ内でパターンに一致するCSVファイルを検索する"""
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
                try:
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        for zip_info in zip_ref.infolist():
                            zip_file_name = Path(zip_info.filename).name
                            if zip_info.filename.endswith(".csv") and pattern.search(
                                zip_file_name
                            ):
                                csv_files.append(
                                    {
                                        "path": f"{file_path}::{zip_info.filename}",
                                        "type": "zip",
                                        "zip_path": str(file_path),
                                        "file_in_zip": zip_info.filename,
                                    }
                                )
                except zipfile.BadZipFile:
                    print(f"警告: 不正なZIPファイル: {file_path}")
                except Exception as e:
                    print(f"エラー: ZIPファイル処理中のエラー {file_path}: {str(e)}")

    return csv_files


def main():
    # .envファイルを読み込む
    load_dotenv()

    # 環境変数を取得
    folder = os.getenv("FOLDER")
    pattern = os.getenv("PATTERN")
    db = os.getenv("DB")
    encoding = os.getenv("ENCODING")
    plant = os.getenv("PLANT")
    machine_id = os.getenv("MACHINE_ID")
    data_label = os.getenv("DATA_LABEL")

    # 環境変数の値を表示
    print(f"設定情報:")
    print(f"FOLDER: {folder}")
    print(f"PATTERN: {pattern}")
    print(f"DB: {db}")
    print(f"ENCODING: {encoding}")
    print(f"PLANT: {plant}")
    print(f"MACHINE_ID: {machine_id}")
    print(f"DATA_LABEL: {data_label}")

    # 指定されたパターンに一致するCSVファイルを検索
    if folder and pattern:
        base_folder = Path(folder)
        if not base_folder.exists():
            print(f"エラー: 指定されたフォルダが存在しません: {folder}")
            return

        print(
            f"\n以下のフォルダ内でパターン'{pattern}'に一致するCSVファイルを検索中...\n"
            f"検索フォルダ： {folder}"
        )
        csv_files = find_csv_files(str(base_folder), pattern)

        # 結果の表示
        if csv_files:
            print(f"\n見つかったCSVファイル ({len(csv_files)}件):")
            for i, file_info in enumerate(csv_files, 1):
                if file_info["type"] == "file":
                    print(f"{i}. {Path(file_info['path']).name}")
                else:  # zip
                    print(f"{i}. {Path(file_info['path']).name} (ZIPファイル内)")
        else:
            print("\n条件に一致するCSVファイルは見つかりませんでした。")
    else:
        print("エラー: .envファイルにFOLDERまたはPATTERNが設定されていません。")
