"""CSVファイルの前処理を行うモジュール"""

import hashlib
import io
import json
import logging
import os
import zipfile
from pathlib import Path

import pandas as pd


class PreprocessorConfig:
    """前処理の設定を管理するクラス"""

    def __init__(self):
        """環境変数から設定を読み込む"""
        # 処理済みファイルの管理用設定
        self.processed_marker = os.getenv("PROCESSED_MARKER", "processed_files.json")
        self.output_dir = os.getenv("OUTPUT_DIR", "processed")

        # .envから取得する縦持ちデータ用の情報
        self.plant = os.getenv("PLANT", "")
        self.machine_id = os.getenv("MACHINE_ID", "")
        self.data_label = os.getenv("DATA_LABEL", "")

        # CSVファイルの読み込み用エンコーディング設定
        self.encoding = os.getenv("ENCODING", "utf-8")

        # 出力先ディレクトリの作成
        output_path = Path(self.output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True)


class CsvPreprocessor:
    """CSVファイルの前処理を行うクラス"""

    def __init__(self, config):
        """前処理クラスを初期化する

        Args:
            config: 設定情報を持つオブジェクト
        """
        self.config = config
        self.processed_files = self._load_processed_files()

    def process_file(self, file_info):
        """CSVファイルを前処理する

        Args:
            file_info (dict): CSVファイル情報

        Returns:
            dict: 処理結果（成功した場合は縦持ちデータを含む辞書、失敗した場合はNone）
        """
        try:
            file_path = file_info["path"]
            file_type = file_info["type"]

            # ファイル名の取得
            if file_type == "file":
                file_name = Path(file_path).name
            else:  # zip
                file_name = Path(file_info["file_in_zip"]).name

            # 処理済みかどうかを確認
            if self._is_processed(file_path):
                logging.info(f"ファイルは既に処理済みです: {file_path}")
                return None

            # CSVファイルの読み込み（すべて特殊形式として処理）
            if file_type == "file":
                data = self._read_special_csv(file_path)
            else:  # zip
                # ZIPファイル内のCSVを読み込む処理
                zip_path = file_info["zip_path"]
                file_in_zip = file_info["file_in_zip"]

                try:
                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        with zip_ref.open(file_in_zip) as f:
                            # CSVデータを読み込む（.envで設定されたエンコーディングを使用）
                            content = io.TextIOWrapper(
                                f, encoding=self.config.encoding
                            ).read()

                            # 一時的にメモリ上で処理
                            lines = content.splitlines()

                            if len(lines) < 4:  # ヘッダー3行 + データ行1行以上
                                logging.error(
                                    f"ZIPファイル内のCSVファイルの行数が不足しています: {file_path}"
                                )
                                return None

                            # ヘッダー行の処理（余分なカンマを削除）
                            sensor_ids = [
                                col.strip() for col in lines[0].strip().split(",")
                            ]
                            sensor_names = [
                                col.strip() for col in lines[1].strip().split(",")
                            ]
                            sensor_units = [
                                col.strip() for col in lines[2].strip().split(",")
                            ]

                            # データ行の処理（末尾のカンマを削除）
                            data_lines = []
                            for line in lines[3:]:
                                # 末尾のカンマを削除
                                if line.strip().endswith(","):
                                    line = line.strip()[:-1]
                                data_lines.append(line)

                            # データフレームを作成するための準備
                            # 1列目は日時、残りの列はセンサーID
                            columns = ["TIME"] + sensor_ids[
                                1:
                            ]  # 最初の空欄をTIMEに置き換え

                            # データを解析
                            data_rows = []
                            for line in data_lines:
                                values = [val.strip() for val in line.split(",")]
                                if len(values) >= len(columns):  # 列数が一致するか確認
                                    row_data = {}
                                    for i, col in enumerate(columns):
                                        row_data[col] = values[i]
                                    data_rows.append(row_data)

                            # DataFrameに変換
                            df = pd.DataFrame(data_rows)

                            # ヘッダー情報を辞書として保存
                            headers = {
                                "sensor_ids": sensor_ids[1:],  # 最初の空欄を除外
                                "sensor_names": sensor_names[1:],  # 最初の空欄を除外
                                "sensor_units": sensor_units[1:],  # 最初の空欄を除外
                            }

                            data = {"headers": headers, "data": df, "format": "special"}
                except Exception as e:
                    logging.error(f"ZIPファイル内のCSV処理エラー {file_path}: {str(e)}")
                    return None

            if data is None:
                return None

            # 縦持ちデータに変換
            vertical_data = self._transform_to_vertical(data, file_name)

            # 個別ファイルの処理結果を保存（オプション）
            output_path = Path(self.config.output_dir) / f"processed_{file_name}"
            self._save_processed_data(vertical_data, output_path)

            # 処理済みとしてマーク
            self._mark_as_processed(file_path)

            logging.info(
                f"ファイルの前処理が完了しました: {file_path} -> {output_path}"
            )

            # 処理結果を返す（統合処理のため）
            return {
                "data": vertical_data,
                "file_name": file_name,
                "file_path": file_path,
            }

        except Exception as e:
            logging.error(f"ファイル処理中にエラーが発生しました {file_path}: {str(e)}")
            return None

    def _load_processed_files(self):
        """処理済みファイルのリストを読み込む

        Returns:
            dict: 処理済みファイルの情報
        """
        marker_path = Path(self.config.processed_marker)
        if marker_path.exists():
            try:
                with open(marker_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"処理済みファイルリストの読み込みエラー: {str(e)}")

        return {}

    def _save_processed_files(self):
        """処理済みファイルのリストを保存する"""
        marker_path = Path(self.config.processed_marker)
        try:
            with open(marker_path, "w", encoding="utf-8") as f:
                json.dump(self.processed_files, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"処理済みファイルリストの保存エラー: {str(e)}")

    def _is_processed(self, file_path):
        """ファイルが処理済みかどうかを確認する

        Args:
            file_path (str): ファイルパス

        Returns:
            bool: 処理済みかどうか
        """
        # ファイルのハッシュ値を計算
        file_hash = self._calculate_file_hash(file_path)

        # 処理済みリストに存在し、ハッシュ値が一致するか確認
        return (
            file_path in self.processed_files
            and self.processed_files[file_path]["hash"] == file_hash
        )

    def _mark_as_processed(self, file_path):
        """ファイルを処理済みとしてマークする

        Args:
            file_path (str): ファイルパス
        """
        # ファイルのハッシュ値を計算
        file_hash = self._calculate_file_hash(file_path)

        # 処理済みリストに追加
        self.processed_files[file_path] = {
            "hash": file_hash,
            "timestamp": pd.Timestamp.now().isoformat(),
        }

        # 処理済みリストを保存
        self._save_processed_files()

    def _calculate_file_hash(self, file_path):
        """ファイルのハッシュ値を計算する

        Args:
            file_path (str): ファイルパス（通常のファイルまたはZIPファイル内のパス）

        Returns:
            str: ファイルのハッシュ値
        """
        try:
            # ZIPファイル内のファイルかどうかを確認
            if "::" in file_path:
                zip_path, file_in_zip = file_path.split("::", 1)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    with zip_ref.open(file_in_zip) as f:
                        file_hash = hashlib.md5(f.read()).hexdigest()
            else:
                # 通常のファイル
                with open(file_path, "rb") as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()
            return file_hash
        except Exception as e:
            logging.error(f"ファイルハッシュ計算エラー {file_path}: {str(e)}")
            return ""

    def _read_special_csv(self, file_path):
        """特殊な形式のCSVファイルを読み込む

        Args:
            file_path (str): CSVファイルのパス

        Returns:
            dict: 読み込んだデータ（ヘッダー情報とデータフレーム）
        """
        try:
            # ファイルの内容を行ごとに読み込む（.envで設定されたエンコーディングを使用）
            with open(file_path, "r", encoding=self.config.encoding) as f:
                lines = f.readlines()

            if len(lines) < 4:  # ヘッダー3行 + データ行1行以上
                logging.error(f"CSVファイルの行数が不足しています: {file_path}")
                return None

            # ヘッダー行の処理（余分なカンマを削除）
            sensor_ids = [col.strip() for col in lines[0].strip().split(",")]
            sensor_names = [col.strip() for col in lines[1].strip().split(",")]
            sensor_units = [col.strip() for col in lines[2].strip().split(",")]

            # データ行の処理（末尾のカンマを削除）
            data_lines = []
            for line in lines[3:]:
                # 末尾のカンマを削除
                if line.strip().endswith(","):
                    line = line.strip()[:-1]
                data_lines.append(line)

            # データフレームを作成するための準備
            # 1列目は日時、残りの列はセンサーID
            columns = ["TIME"] + sensor_ids[1:]  # 最初の空欄をTIMEに置き換え

            # データを解析
            data = []
            for line in data_lines:
                values = [val.strip() for val in line.split(",")]
                if len(values) >= len(columns):  # 列数が一致するか確認
                    row_data = {}
                    for i, col in enumerate(columns):
                        row_data[col] = values[i]
                    data.append(row_data)

            # DataFrameに変換
            df = pd.DataFrame(data)

            # ヘッダー情報を辞書として保存
            headers = {
                "sensor_ids": sensor_ids[1:],  # 最初の空欄を除外
                "sensor_names": sensor_names[1:],  # 最初の空欄を除外
                "sensor_units": sensor_units[1:],  # 最初の空欄を除外
            }

            return {"headers": headers, "data": df, "format": "special"}

        except Exception as e:
            logging.error(f"特殊CSVファイル読み込みエラー {file_path}: {str(e)}")
            return None

    def process_all_files(self, csv_files):
        """複数のCSVファイルを処理し、統合する

        Args:
            csv_files (list): CSVファイル情報のリスト

        Returns:
            pd.DataFrame: 統合された縦持ちデータ
        """
        # 各ファイルの処理結果を格納するリスト
        processed_results = []

        # 各ファイルを処理
        for file_info in csv_files:
            result = self.process_file(file_info)
            if result is not None:
                processed_results.append(result)

        if not processed_results:
            logging.info("処理対象のCSVファイルがありませんでした。")
            return None

        # すべてのデータを統合
        all_data = pd.concat(
            [result["data"] for result in processed_results], ignore_index=True
        )

        # 重複を削除（TIME, SENSOR_IDが同じデータを重複とみなす）
        deduplicated_data = all_data.drop_duplicates(
            subset=["TIME", "SENSOR_ID"], keep="first"
        )

        # 重複削除の結果をログ出力
        duplicate_count = len(all_data) - len(deduplicated_data)
        if duplicate_count > 0:
            logging.info(f"重複データを {duplicate_count} 件削除しました。")

        # 統合データを保存
        integrated_path = Path(self.config.output_dir) / "integrated_data.csv"
        self._save_processed_data(deduplicated_data, integrated_path)
        logging.info(f"統合データを保存しました: {integrated_path}")

        return deduplicated_data

    def _transform_to_vertical(self, data, file_name):
        """横持ちデータを縦持ちデータに変換する

        Args:
            data (dict): 読み込んだデータ（ヘッダー情報とデータフレーム）
            file_name (str): ファイル名

        Returns:
            pd.DataFrame: 縦持ちデータ
        """
        if data is None:
            return None

        headers = data["headers"]
        df = data["data"]

        # 結果を格納するリスト
        rows = []

        # 各行、各センサーについて縦持ちデータを作成
        for _, row in df.iterrows():
            time_value = row["TIME"]

            for i, sensor_id in enumerate(headers["sensor_ids"]):
                # センサー名と単位の取得
                sensor_name = headers["sensor_names"][i]
                sensor_unit = headers["sensor_units"][i]

                # センサーの値を取得
                try:
                    value = row[sensor_id]
                except:
                    # キーが見つからない場合は列インデックスで取得を試みる
                    try:
                        value = row.iloc[i + 1]  # TIME列の次から
                    except:
                        logging.warning(f"センサー {sensor_id} の値が見つかりません")
                        continue

                # 縦持ちデータの1行を作成
                rows.append(
                    {
                        "PLANT": self.config.plant,
                        "MACHINE_ID": self.config.machine_id,
                        "DATA_LABEL": self.config.data_label,
                        "TIME": time_value,
                        "SENSOR_ID": sensor_id,
                        "SENSOR_NAME": sensor_name,
                        "SENSOR_UNIT": sensor_unit,
                        "VALUE": value,
                        "file_name": file_name,
                    }
                )

        # DataFrameに変換
        vertical_df = pd.DataFrame(rows)
        return vertical_df

    def _save_processed_data(self, data, output_path):
        """処理済みデータを保存する

        Args:
            data (pd.DataFrame): 処理済みデータ
            output_path (Path): 出力先パス

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            # CSVファイルとして保存
            data.to_csv(output_path, index=False, encoding="utf-8")
            return True
        except Exception as e:
            logging.error(f"データ保存エラー {output_path}: {str(e)}")
            return False
