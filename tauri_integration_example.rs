//! Tauriアプリケーションからcsv_to_db.exeを呼び出すサンプルコード
//!
//! このファイルは、Tauriプロジェクトの`src-tauri/src/`ディレクトリに配置し、
//! `main.rs`から`mod csv_to_db;`のように読み込んで使用します。

use std::path::PathBuf;
use std::process::Command;
use tauri::{command, State};

/// csv_to_db.exeの実行結果を格納する構造体
#[derive(serde::Serialize)]
pub struct CsvToDbResult {
    success: bool,
    message: String,
    records_imported: Option<i32>,
}

/// csv_to_db.exeのパスを保持する状態
pub struct CsvToDbState {
    exe_path: PathBuf,
}

impl CsvToDbState {
    /// 新しい状態を作成
    pub fn new(exe_path: PathBuf) -> Self {
        Self { exe_path }
    }
}

/// csv_to_db.exeを実行するTauriコマンド
#[command]
pub async fn run_csv_to_db(
    state: State<'_, CsvToDbState>,
    folder: String,
    pattern: String,
    db_path: Option<String>,
    encoding: Option<String>,
    plant: Option<String>,
    machine_id: Option<String>,
    data_label: Option<String>,
) -> Result<CsvToDbResult, String> {
    // コマンドを構築
    let mut cmd = Command::new(state.exe_path.clone());
    
    // 必須引数
    cmd.arg("--folder").arg(folder);
    cmd.arg("--pattern").arg(pattern);
    
    // オプション引数
    if let Some(db) = db_path {
        cmd.arg("--db").arg(db);
    }
    
    if let Some(enc) = encoding {
        cmd.arg("--encoding").arg(enc);
    }
    
    if let Some(p) = plant {
        cmd.arg("--plant").arg(p);
    }
    
    if let Some(mid) = machine_id {
        cmd.arg("--machine-id").arg(mid);
    }
    
    if let Some(label) = data_label {
        cmd.arg("--data-label").arg(label);
    }
    
    // 非同期でコマンドを実行
    let output = tokio::task::spawn_blocking(move || cmd.output())
        .await
        .map_err(|e| format!("タスク実行エラー: {}", e))?
        .map_err(|e| format!("コマンド実行エラー: {}", e))?;
    
    // 実行結果を解析
    let success = output.status.success();
    let message = if success {
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        String::from_utf8_lossy(&output.stderr).to_string()
    };
    
    // インポートされたレコード数を抽出（成功時のみ）
    let records_imported = if success {
        // 出力からインポートされたレコード数を抽出
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout
            .lines()
            .find(|line| line.contains("インポートされたレコード数:"))
            .and_then(|line| {
                line.split(':')
                    .nth(1)
                    .and_then(|s| s.trim().parse::<i32>().ok())
            })
    } else {
        None
    };
    
    Ok(CsvToDbResult {
        success,
        message,
        records_imported,
    })
}

/// Tauriアプリケーションのmain.rsでの使用例
pub fn setup_csv_to_db(app: &mut tauri::App) -> Result<(), Box<dyn std::error::Error>> {
    // exe_pathは実際の環境に合わせて設定
    let exe_path = PathBuf::from("path/to/csv_to_db.exe");
    
    // 状態を管理
    app.manage(CsvToDbState::new(exe_path));
    
    Ok(())
}

/// フロントエンドからの呼び出し例（JavaScript/TypeScript）
///
/// ```typescript
/// // Tauriアプリケーションのフロントエンド（JavaScript/TypeScript）
/// import { invoke } from '@tauri-apps/api/tauri';
///
/// async function runCsvToDb() {
///   try {
///     const result = await invoke('run_csv_to_db', {
///       folder: 'C:/path/to/csv_files',
///       pattern: '*.csv',
///       dbPath: 'output.duckdb',
///       encoding: 'utf-8',
///       plant: 'AAA',
///       machineId: 'No.1',
///       dataLabel: '2024'
///     });
///     
///     if (result.success) {
///       console.log(`成功: ${result.records_imported} レコードがインポートされました`);
///     } else {
///       console.error(`エラー: ${result.message}`);
///     }
///   } catch (error) {
///     console.error(`呼び出しエラー: ${error}`);
///   }
/// }
