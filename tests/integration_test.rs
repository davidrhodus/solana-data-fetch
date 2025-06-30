use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_help_command() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "A high-performance Rust CLI client",
        ))
        .stdout(predicates::str::contains("slot"))
        .stdout(predicates::str::contains("slot-data"))
        .stdout(predicates::str::contains("epoch-data"));
}

#[test]
fn test_search_token_help() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["search-token", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Search for tokens by name or symbol",
        ))
        .stdout(predicate::str::contains("--query"))
        .stdout(predicate::str::contains("--limit"));
}

#[test]
fn test_epoch_data_help() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["epoch-data", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Download all slots for an epoch"))
        .stdout(predicate::str::contains("--concurrent"))
        .stdout(predicate::str::contains("--skip-existing"));
}

#[test]
fn test_missing_required_args() {
    // Test search-token without query
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("search-token")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));

    // Test token-info without mint
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("token-info")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));

    // Test epoch-data without epoch number
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("epoch-data")
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));
}

#[test]
fn test_invalid_subcommand() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("invalid-command")
        .assert()
        .failure()
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn test_slot_command_with_invalid_number() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["slot", "not-a-number"]).assert().failure();
}

// Note: These tests would require a mock server or would hit the real API
// For demonstration, they're commented out but show the structure

/*
#[test]
fn test_health_command_real() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("health")
        .assert()
        .success()
        .stdout(predicate::str::contains("Checking API health"))
        .stdout(predicate::str::contains("API Status"))
        .stdout(predicate::str::contains("Database"));
}

#[test]
fn test_stats_command_real() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.arg("stats")
        .assert()
        .success()
        .stdout(predicate::str::contains("Blockchain Overview"))
        .stdout(predicate::str::contains("Total Slots"))
        .stdout(predicate::str::contains("Indexing Rate"));
}
*/

// Version flag is not automatically added by clap
// #[test]
// fn test_version_output() {
//     let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
//     cmd.arg("--version")
//         .assert()
//         .success()
//         .stdout(predicate::str::contains("solana-data-fetch"));
// }

#[test]
fn test_swaps_with_all_filters() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    // Test that all arguments are accepted
    cmd.args([
        "swaps",
        "--token",
        "So11111111111111111111111111111111111111112",
        "--start-slot",
        "100000",
        "--end-slot",
        "200000",
        "--limit",
        "50",
    ])
    .assert()
    .success() // Command runs successfully but may return "No swaps found"
    .stdout(predicate::str::contains("Fetching token swaps"));
}

#[test]
fn test_account_subcommands() {
    // Test account activity help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["account", "activity", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Get account activity"));

    // Test account balances help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["account", "balances", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Get token balances"));
}

#[test]
fn test_volume_command_args() {
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["volume", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Token mint address"))
        .stdout(predicate::str::contains("--days"));
}

#[test]
fn test_auth_commands_help() {
    // Test auth help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Authentication operations"))
        .stdout(predicate::str::contains("register"))
        .stdout(predicate::str::contains("login"))
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("logout"));

    // Test auth register help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "register", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Register a new account"))
        .stdout(predicate::str::contains("--email"))
        .stdout(predicate::str::contains("--password"));

    // Test auth login help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "login", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Login to existing account"))
        .stdout(predicate::str::contains("--email"))
        .stdout(predicate::str::contains("--password"));

    // Test auth status help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "status", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Show current authentication status",
        ));

    // Test auth logout help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "logout", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Logout and clear saved credentials",
        ));
}

#[test]
fn test_api_keys_commands_help() {
    // Test api-keys help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("API key management"))
        .stdout(predicate::str::contains("create"))
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("delete"));

    // Test api-keys create help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "create", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Create a new API key"))
        .stdout(predicate::str::contains("--name"))
        .stdout(predicate::str::contains("--token"));

    // Test api-keys list help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "list", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("List all your API keys"))
        .stdout(predicate::str::contains("--token"));

    // Test api-keys delete help
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "delete", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Delete an API key"))
        .stdout(predicate::str::contains("<KEY_ID>"));
}

#[test]
fn test_auth_missing_required_args() {
    // Test register without email
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "register"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));

    // Test login without email
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "login"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));
}

#[test]
fn test_api_keys_missing_required_args() {
    // Test create without name
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "create"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));

    // Test delete without key_id
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["api-keys", "delete"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));
}

#[test]
fn test_auth_status_runs() {
    // Test that auth status command runs (even without credentials)
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Checking authentication status"))
        .stdout(predicate::str::contains("Config file:"));
}

#[test]
fn test_auth_logout_runs() {
    // Test that auth logout command runs (even without credentials)
    let mut cmd = Command::cargo_bin("solana-data-fetch").unwrap();
    cmd.args(["auth", "logout"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Logging out"));
}
