"""CSVファイルの前処理を行うモジュール"""

import hashlib
import io
import json
import logging
import os
import zipfile
from datetime import datetime
from pathlib import Path

import polars as pl
from polars import col, lit


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

                            # データを解析してPolarsのDataFrameに変換
                            data_rows = []
                            for line in data_lines:
                                values = [val.strip() for val in line.split(",")]
                                if len(values) >= len(columns):  # 列数が一致するか確認
                                    row_data = {}
                                    for i, col in enumerate(columns):
                                        row_data[col] = values[i]
                                    data_rows.append(row_data)

                            # Polarsのデータフレームに変換
                            df = pl.DataFrame(data_rows)

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
            "timestamp": datetime.now().isoformat(),
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

            # Polarsのデータフレームに変換
            df = pl.DataFrame(data)

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
            pl.DataFrame: 統合された縦持ちデータ
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
        all_data = pl.concat([result["data"] for result in processed_results])

        # 重複を削除（TIME, SENSOR_IDが同じデータを重複とみなす）
        deduplicated_data = all_data.unique(subset=["TIME", "SENSOR_ID"], keep="first")

        # 重複削除の結果をログ出力
        duplicate_count = len(all_data) - len(deduplicated_data)
        if duplicate_count > 0:
            logging.info(f"重複データを {duplicate_count} 件削除しました。")

        # 統合データを保存
        integrated_path = Path(self.config.output_dir) / "integrated_data"
        self._save_processed_data(deduplicated_data, integrated_path)
        logging.info(
            f"統合データを保存しました: {integrated_path.with_suffix('.parquet')}"
        )

        return deduplicated_data

    def process_all_files_to_db(self, csv_files, db_manager, table_name, batch_size=5):
        """複数のCSVファイルをバッチで処理し、直接データベースに投入する

        Args:
            csv_files (list): CSVファイル情報のリスト
            db_manager (DatabaseManager): データベース管理オブジェクト
            table_name (str): インポート先のテーブル名
            batch_size (int): 一度に処理するファイル数。デフォルトは5。

        Returns:
            int: 処理されたレコード数
        """
        total_records = 0
        total_batches = (len(csv_files) - 1) // batch_size + 1

        # バッチ処理
        for i in range(0, len(csv_files), batch_size):
            batch = csv_files[i : i + batch_size]
            current_batch = i // batch_size + 1
            logging.info(
                f"バッチ処理開始: {current_batch}/{total_batches} (ファイル {i + 1}～{min(i + batch_size, len(csv_files))})"
            )

            batch_records = 0
            # 各ファイルを処理
            for file_info in batch:
                result = self.process_file(file_info)
                if result is not None:
                    # 処理したデータを直接DBに投入
                    if db_manager.import_dataframe(result["data"], table_name):
                        batch_records += len(result["data"])
                        total_records += len(result["data"])

                    # メモリ解放のために明示的に削除
                    del result["data"]
                    result.clear()

            logging.info(
                f"バッチ処理完了: {current_batch}/{total_batches} - {batch_records}レコード処理"
            )

            # 各バッチ処理後にガベージコレクションを実行
            import gc

            gc.collect()

        if total_records == 0:
            logging.info("処理対象のCSVファイルがありませんでした。")
            return 0

        # 重複を削除（TIME, SENSOR_IDが同じデータを重複とみなす）
        if total_records > 0:
            try:
                # データベース上で重複を削除するクエリを実行（日付ごとに分割処理）
                logging.info("重複データの削除を開始します（日付ごとの分割処理）...")
                try:
                    # まず空の一時テーブルを作成
                    db_manager.execute_query(
                        f"CREATE TABLE {table_name}_temp AS SELECT * FROM {table_name} WHERE 1=0"
                    )

                    # 日付の一覧を取得
                    date_query = f"SELECT DISTINCT DATE_TRUNC('day', TIME) as date_day FROM {table_name} ORDER BY date_day"
                    date_result = db_manager.execute_query(date_query)

                    if date_result:
                        dates = date_result.fetchall()
                        total_dates = len(dates)
                        logging.info(f"処理対象の日付数: {total_dates}日")

                        for i, date in enumerate(dates):
                            date_str = date[0]
                            logging.info(
                                f"日付 {i + 1}/{total_dates} の処理中: {date_str}"
                            )

                            # 1日分のデータから重複を除外して一時テーブルに挿入
                            # テーブルのカラム名を取得
                            columns_info = db_manager.get_table_info(
                                f"{table_name}_temp"
                            )
                            if not columns_info:
                                logging.error(
                                    f"テーブル {table_name}_temp の情報を取得できませんでした"
                                )
                                continue

                            # カラム名のリストを作成
                            column_names = [col[1] for col in columns_info]
                            columns_str = ", ".join(
                                [f"t.{col}" for col in column_names]
                            )

                            batch_query = f"""
                            INSERT INTO {table_name}_temp 
                            SELECT {columns_str} FROM (
                                SELECT *, ROW_NUMBER() OVER (PARTITION BY TIME, SENSOR_ID ORDER BY TIME) as rn 
                                FROM {table_name}
                                WHERE DATE_TRUNC('day', TIME) = '{date_str}'
                            ) t WHERE t.rn = 1
                            """
                            db_manager.execute_query(batch_query)

                            # 処理済みの日付のデータを削除して一時ディレクトリの使用量を減らす
                            db_manager.execute_query(
                                f"DELETE FROM {table_name} WHERE DATE_TRUNC('day', TIME) = '{date_str}'"
                            )

                            # 各バッチ後にガベージコレクションを実行
                            db_manager.execute_query("PRAGMA force_checkpoint")

                            # メモリ解放
                            import gc

                            gc.collect()

                        # 元のテーブルを削除して一時テーブルの名前を変更
                        db_manager.execute_query(f"DROP TABLE {table_name}")
                        db_manager.execute_query(
                            f"ALTER TABLE {table_name}_temp RENAME TO {table_name}"
                        )

                        # 処理後のレコード数を取得
                        count_query = f"SELECT COUNT(*) FROM {table_name}"
                        result = db_manager.execute_query(count_query)
                        if result:
                            final_count = result.fetchone()[0]
                            duplicate_count = total_records - final_count
                            if duplicate_count > 0:
                                logging.info(
                                    f"重複データを {duplicate_count} 件削除しました。"
                                )
                            total_records = final_count
                    else:
                        logging.warning(
                            "日付データの取得に失敗しました。重複削除をスキップします。"
                        )
                except Exception as e:
                    logging.error(f"重複削除中にエラーが発生しました: {str(e)}")
                    logging.error(f"エラー詳細: {type(e).__name__}")

                # 重複削除後のレコード数を取得
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                result = db_manager.execute_query(count_query)
                if result:
                    final_count = result.fetchone()[0]
                    duplicate_count = total_records - final_count
                    if duplicate_count > 0:
                        logging.info(f"重複データを {duplicate_count} 件削除しました。")
                    total_records = final_count
            except Exception as e:
                logging.error(f"重複削除中にエラーが発生しました: {str(e)}")
                logging.error(f"エラー詳細: {type(e).__name__}")

        return total_records

    def _transform_to_vertical(self, data, file_name):
        """横持ちデータを縦持ちデータに変換する（メモリ最適化版）

        Args:
            data (dict): 読み込んだデータ（ヘッダー情報とデータフレーム）
            file_name (str): ファイル名

        Returns:
            pl.DataFrame: 縦持ちデータ
        """
        if data is None:
            return None

        headers = data["headers"]
        df = data["data"]

        # 横持ちデータを縦持ちに変換するための準備
        sensor_ids = headers["sensor_ids"]

        # TIMEカラムをdatetime型に変換
        try:
            time_col = df.select("TIME").with_columns(
                pl.col("TIME").str.to_datetime("%Y/%m/%d %H:%M:%S").alias("TIME")
            )
        except Exception as e:
            # 日付形式が異なる場合は別の形式を試す
            try:
                time_col = df.select("TIME").with_columns(
                    pl.col("TIME").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("TIME")
                )
            except Exception as e2:
                logging.warning(
                    f"日時の変換に失敗しました: {str(e2)}。文字列のまま処理を続行します。"
                )
                time_col = df.select("TIME")

        # 結果を格納するデータフレームのリスト
        vertical_dfs = []

        # センサーごとに縦持ちデータを作成（メモリ使用量を削減するためにバッチ処理）
        sensor_batch_size = 10  # センサーのバッチサイズ

        for i in range(0, len(sensor_ids), sensor_batch_size):
            batch_sensor_ids = sensor_ids[i : i + sensor_batch_size]
            batch_dfs = []

            for j, sensor_id in enumerate(batch_sensor_ids):
                # センサー名と単位の取得
                sensor_idx = i + j
                if sensor_idx < len(headers["sensor_names"]) and sensor_idx < len(
                    headers["sensor_units"]
                ):
                    sensor_name = headers["sensor_names"][sensor_idx]
                    sensor_unit = headers["sensor_units"][sensor_idx]
                else:
                    # インデックスが範囲外の場合はスキップ
                    logging.warning(
                        f"センサー {sensor_id} のメタデータが見つかりません"
                    )
                    continue

                # センサーの値を含む列を抽出
                if sensor_id in df.columns:
                    sensor_values = df.select(sensor_id).rename({sensor_id: "VALUE"})
                else:
                    # 列名が見つからない場合はスキップ
                    logging.warning(f"センサー {sensor_id} の列が見つかりません")
                    continue

                # 時間列とセンサー値を結合
                sensor_df = pl.concat([time_col, sensor_values], how="horizontal")

                # 固定値の列を追加
                sensor_df = sensor_df.with_columns(
                    [
                        lit(self.config.plant).alias("PLANT"),
                        lit(self.config.machine_id).alias("MACHINE_ID"),
                        lit(self.config.data_label).alias("DATA_LABEL"),
                        lit(sensor_id).alias("SENSOR_ID"),
                        lit(sensor_name).alias("SENSOR_NAME"),
                        lit(sensor_unit).alias("SENSOR_UNIT"),
                        lit(file_name).alias("file_name"),
                    ]
                )

                batch_dfs.append(sensor_df)

                # メモリ解放
                del sensor_values

            # バッチ内のデータフレームを結合
            if batch_dfs:
                batch_df = pl.concat(batch_dfs)
                vertical_dfs.append(batch_df)

                # メモリ解放
                del batch_dfs

            # バッチごとにガベージコレクション
            import gc

            gc.collect()

        # すべてのセンサーデータを結合
        if not vertical_dfs:
            return pl.DataFrame()

        vertical_df = pl.concat(vertical_dfs)

        # メモリ解放
        del vertical_dfs

        return vertical_df

    def _save_processed_data(self, data, output_path):
        """処理済みデータを保存する

        Args:
            data (pl.DataFrame): 処理済みデータ
            output_path (Path): 出力先パス

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            # 出力パスの拡張子を.parquetに変更
            parquet_path = output_path.with_suffix(".parquet")

            # Parquetファイルとして保存（圧縮オプションも指定）
            data.write_parquet(parquet_path, compression="snappy")
            logging.info(f"Parquetファイルとして保存しました: {parquet_path}")
            return True
        except Exception as e:
            logging.error(f"データ保存エラー {output_path}: {str(e)}")
            return False
