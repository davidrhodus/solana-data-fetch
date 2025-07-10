// Integration tests for epoch download functionality
// Run with: cargo test --test epoch_integration_tests

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_epoch_data_command_basic() {
    // Test that epoch-data command starts successfully (we'll cancel it quickly)
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0"]) // Use epoch 0 (small/empty)
        .timeout(std::time::Duration::from_secs(5)) // Cancel after 5 seconds
        .assert()
        .interrupted() // Should be interrupted by timeout
        .stdout(predicate::str::contains(
            "Downloading all slots for epoch 0",
        ))
        .stdout(predicate::str::contains("Created directory: epoch_0"));

    // Verify directory was created
    let epoch_dir = temp_dir.path().join("epoch_0");
    assert!(epoch_dir.exists(), "Epoch directory should be created");
}

#[test]
fn test_epoch_data_with_no_rate_limit() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--no-rate-limit"])
        .timeout(std::time::Duration::from_secs(5))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains(
            "Rate limiting disabled for maximum performance",
        ))
        .stdout(predicate::str::contains(
            "Downloading all slots for epoch 0",
        ));
}

#[test]
fn test_epoch_data_with_concurrency_option() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--concurrent", "50"])
        .timeout(std::time::Duration::from_secs(5))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("50 concurrent connections"));
}

#[test]
fn test_epoch_data_with_no_compression() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--no-compression"])
        .timeout(std::time::Duration::from_secs(5))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("Compression: Disabled"));
}

#[test]
fn test_epoch_data_progress_display() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--no-rate-limit"])
        .timeout(std::time::Duration::from_secs(10)) // Longer timeout to see progress
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("⬇️  Starting download"))
        .stdout(predicate::str::contains("concurrent connections"))
        .stdout(predicate::str::contains("💡 Many slots may not exist"));
}

#[test]
fn test_epoch_data_invalid_epoch() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["epoch-data", "not-a-number"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid value"));
}

#[test]
fn test_epoch_data_disk_space_check() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "800"]) // Recent epoch
        .timeout(std::time::Duration::from_secs(3))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("Estimated disk space needed"));
}

#[test]
fn test_epoch_data_compression_settings() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Test with compression (default)
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0"])
        .timeout(std::time::Duration::from_secs(3))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("Compression: Enabled"))
        .stdout(predicate::str::contains("zstd format"));
}

#[test]
fn test_epoch_data_creates_correct_directory_structure() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Run epoch download briefly
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "1", "--no-rate-limit"])
        .timeout(std::time::Duration::from_secs(8))
        .assert()
        .interrupted();

    // Check that directory structure is correct
    let epoch_dir = temp_dir.path().join("epoch_1");
    assert!(epoch_dir.exists(), "Epoch directory should exist");
    assert!(epoch_dir.is_dir(), "Epoch path should be a directory");

    // Check if any slot files were created (might be none for early epochs)
    let files: Vec<_> = fs::read_dir(&epoch_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .collect();

    println!("Created {} files in epoch directory", files.len());

    // If files were created, check they have correct naming
    for file in files {
        let filename = file.file_name();
        let filename_str = filename.to_string_lossy();

        // Should be slot_*.json.zst format (with compression)
        assert!(
            filename_str.starts_with("slot_")
                && (filename_str.ends_with(".json.zst") || filename_str.ends_with(".json")),
            "Invalid file name format: {}",
            filename_str
        );
    }
}

#[test]
fn test_epoch_data_adaptive_concurrency_display() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--no-rate-limit"])
        .timeout(std::time::Duration::from_secs(8))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains("⚡")); // Should show adaptive concurrency indicator
}

#[test]
fn test_epoch_data_optimizations_enabled() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Test that optimization indicators are present
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.current_dir(temp_path)
        .args(["epoch-data", "0", "--no-rate-limit"])
        .timeout(std::time::Duration::from_secs(5))
        .assert()
        .interrupted()
        .stdout(predicate::str::contains(
            "CPU cores for parallel compression",
        ))
        .stdout(predicate::str::contains("concurrent connections"));
}
