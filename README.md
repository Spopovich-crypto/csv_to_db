# CSV to DB

CSVファイル検索とデータベース取り込みツール

## 概要

このツールは、指定されたフォルダ内でパターンに一致するCSVファイルを検索し、DuckDBデータベースに取り込む機能を提供します。
通常のファイルシステム内のCSVファイルだけでなく、ZIPファイル内のCSVファイルも検索対象となります。

このツールはexe化して実行可能ファイルとして配布することも、Tauriで構築するデスクトップアプリケーションからRustを使って呼び出すこともできます。

## 主な機能

- 指定されたフォルダ内でパターンに一致するCSVファイルの検索
- ZIPファイル内のCSVファイルにも対応
- 特殊な形式のCSVファイルの読み込みと処理
- 横持ちデータから縦持ちデータへの変換
- 処理済みデータのParquetファイルへの保存
- DuckDBデータベースへのデータ取り込み
- 処理済みファイルの管理（重複処理の防止）
- メモリ最適化機能（バッチ処理とチャンク処理による大量データの効率的な処理）

## クイックスタート

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/csv-to-db.git
cd csv-to-db

# 依存関係のインストール
uv pip install -e .

# .envファイルを設定
# 必要な設定項目: FOLDER, PATTERN

# ツールを実行
uv run main.py
```

## ドキュメント

詳細なドキュメントは以下のファイルを参照してください：

- [インストールガイド](docs/installation.md) - システム要件、依存関係、インストール手順
- [ユーザーマニュアル](docs/user_manual.md) - 基本的な使い方、設定ファイルの詳細
- [ファイル形式ガイド](docs/file_formats.md) - 対応しているCSV形式の詳細
- [技術リファレンス](docs/technical_reference.md) - アーキテクチャ、処理フロー
- [開発者ガイド](docs/developer_guide.md) - コード構造、拡張方法
- [トラブルシューティング](docs/troubleshooting.md) - 一般的な問題と解決方法

## ファイル構成

```
csv-to-db/
├── .env                        # 環境変数設定ファイル
├── main.py                     # エントリーポイント
├── pyproject.toml              # プロジェクト設定
├── README.md                   # このファイル
├── create_test_zip.py          # テスト用ZIPファイル作成スクリプト
├── csv_to_db.spec              # PyInstaller設定ファイル
├── build_exe.py                # exe化ビルドスクリプト
├── tauri_integration_example.rs # Tauriアプリケーション連携サンプル
├── data/                       # データフォルダ
├── docs/                       # ドキュメントフォルダ
└── src/                        # ソースコード
    ├── __init__.py             # パッケージ初期化
    ├── main.py                 # メイン処理
    ├── config.py               # 設定管理
    ├── file_finder.py          # ファイル検索
    ├── logger.py               # ロギング
    ├── preprocessor.py         # 前処理
    └── db.py                   # データベース操作
```

## exe化の手順

このプロジェクトはPyInstallerを使用して単一の実行ファイル（.exe）にビルドできます。

```bash
# ビルドスクリプトを実行
python build_exe.py
```

ビルドが成功すると、`dist`フォルダに`csv_to_db.exe`が生成されます。

### exe実行時のオプション

生成されたexeファイルは、元のPythonスクリプトと同じコマンドライン引数をサポートしています。

```bash
# 基本的な使用方法
csv_to_db.exe --folder C:\path\to\csv_files --pattern "*.csv"

# すべてのオプションを指定
csv_to_db.exe --folder C:\path\to\csv_files --pattern "*.csv" --db output.duckdb --encoding shift-jis --plant AAA --machine-id No.1 --data-label 2024
```

## Tauriアプリケーションからの呼び出し

このツールは、Tauriで構築するデスクトップアプリケーションからRustを使って呼び出すことができます。

### Rustからの呼び出し（基本）

```rust
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // csv_to_db.exeを実行
    let output = Command::new("path/to/csv_to_db.exe")
        .arg("--folder")
        .arg("C:/path/to/csv_files")
        .arg("--pattern")
        .arg("*.csv")
        .arg("--db")
        .arg("output.duckdb")
        .output()?;
    
    // 実行結果を取得
    if output.status.success() {
        println!("成功: {}", String::from_utf8_lossy(&output.stdout));
    } else {
        eprintln!("エラー: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    Ok(())
}
```

### Tauriアプリケーションでの統合

より詳細な統合方法については、`tauri_integration_example.rs`ファイルを参照してください。このファイルには以下の内容が含まれています：

- Tauriコマンドとしての実装
- 引数の渡し方
- 非同期実行の方法
- フロントエンドからの呼び出し例（JavaScript/TypeScript）

## ライセンス

[CHINAMI]
