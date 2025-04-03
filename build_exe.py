"""
csv_to_dbプロジェクトをexe化するためのビルドスクリプト
"""

import os
import subprocess
import sys


def build_exe():
    """PyInstallerを使用してexeをビルドする"""
    print("csv_to_dbプロジェクトのexe化を開始します...")

    # PyInstallerコマンドを実行
    cmd = [
        "pyinstaller",
        "--clean",  # キャッシュをクリーン
        "csv_to_db.spec",  # 設定ファイル
    ]

    try:
        subprocess.run(cmd, check=True)
        print("exe化が完了しました。")
        print(f"実行ファイル: {os.path.join('dist', 'csv_to_db.exe')}")
    except subprocess.CalledProcessError as e:
        print(f"エラー: exe化に失敗しました。 {e}", file=sys.stderr)
        return False

    return True


def main():
    """メイン関数"""
    if not build_exe():
        sys.exit(1)

    print("\n使用方法:")
    print("csv_to_db.exe [オプション]")
    print("\nオプション例:")
    print("  --folder C:\\path\\to\\csv_files")
    print('  --pattern "*.csv"')
    print("  --db output.duckdb")
    print("  --encoding shift-jis")

    print("\nTauriアプリケーションからの呼び出し例（Rust）:")
    print("""
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
""")


if __name__ == "__main__":
    main()
