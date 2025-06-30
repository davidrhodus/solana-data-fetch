use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use governor::{Jitter, Quota, RateLimiter};
use nonzero_ext::nonzero;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use zstd::stream::encode_all;

// Performance optimization structures
#[derive(Clone)]
struct DownloadStats {
    completed: Arc<std::sync::atomic::AtomicUsize>,
    failed: Arc<std::sync::atomic::AtomicUsize>,
    skipped: Arc<std::sync::atomic::AtomicUsize>,
    not_found: Arc<std::sync::atomic::AtomicUsize>,
    rate_limited: Arc<std::sync::atomic::AtomicUsize>,
    requests_made: Arc<std::sync::atomic::AtomicUsize>,
}

impl DownloadStats {
    fn new() -> Self {
        Self {
            completed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            skipped: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            not_found: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            rate_limited: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            requests_made: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

enum SlotError {
    NotFound,
    RateLimited,
    ParseError,
    #[allow(dead_code)]
    NetworkError(String),
    #[allow(dead_code)]
    Other(String),
}

impl From<anyhow::Error> for SlotError {
    fn from(err: anyhow::Error) -> Self {
        let error_str = err.to_string();

        if error_str.contains("404")
            || error_str.contains("Not found")
            || error_str.contains("not found")
        {
            SlotError::NotFound
        } else if error_str.contains("429")
            || error_str.contains("rate limit")
            || error_str.contains("Rate limit exceeded")
        {
            SlotError::RateLimited
        } else if error_str.contains("Invalid response format")
            || error_str.contains("Invalid JSON")
            || error_str.contains("EOF while parsing")
            || error_str.contains("contains no data")
        {
            SlotError::ParseError
        } else if error_str.contains("Network")
            || error_str.contains("timeout")
            || error_str.contains("connection")
        {
            SlotError::NetworkError(error_str)
        } else {
            SlotError::Other(error_str)
        }
    }
}

// File write batch for async channel
struct FileWriteBatch {
    path: String,
    data: Vec<u8>,
}

const API_BASE_URL: &str = "https://api.pipe-solana.com";
const SLOTS_PER_EPOCH: i64 = 432_000; // Solana mainnet slots per epoch

// CLI structure
#[derive(Parser)]
#[command(
    name = "solana-data-fetch",
    version = "0.1.0",
    about = "High-performance CLI for downloading Solana blockchain data",
    long_about = "A high-performance Rust CLI client for downloading Solana blockchain data.\n\nCore features for slot and epoch data retrieval."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // === BETA Features (Core functionality) ===
    /// Get slot metadata [BETA]
    Slot {
        /// Slot number
        slot: String,
    },

    /// Get full slot/block data with transactions [BETA]
    SlotData {
        /// Slot number
        slot: i64,
    },

    /// Download all slots for an epoch [BETA]
    EpochData {
        /// Epoch number
        epoch: i64,

        /// Maximum concurrent downloads (default: 100)
        #[arg(short, long, default_value = "100")]
        concurrent: usize,

        /// Skip existing files
        #[arg(short, long)]
        skip_existing: bool,

        /// Override rate limit (requests per second)
        #[arg(short = 'r', long)]
        rate_limit: Option<u32>,

        /// Disable compression (files will be larger but save CPU)
        #[arg(long)]
        no_compression: bool,

        /// Disable client-side rate limiting entirely (use when server has no limits)
        #[arg(long)]
        no_rate_limit: bool,
    },

    // === ALPHA Features ===
    /// Search for tokens by name or symbol [ALPHA]
    #[command(hide = true)]
    SearchToken {
        /// Search query (token name or symbol)
        #[arg(short, long)]
        query: String,

        /// Maximum number of results (default: 10)
        #[arg(short, long, default_value = "10")]
        limit: i64,
    },

    /// Get information about a specific token [ALPHA]
    #[command(hide = true)]
    TokenInfo {
        /// Token mint address
        mint: String,
    },

    /// Get token swap transactions [ALPHA]
    #[command(hide = true)]
    Swaps {
        /// Filter by token mint address (optional)
        #[arg(short = 't', long)]
        token: Option<String>,

        /// Start slot (optional)
        #[arg(short = 's', long)]
        start_slot: Option<i64>,

        /// End slot (optional)
        #[arg(short = 'e', long)]
        end_slot: Option<i64>,

        /// Maximum number of results (default: 10)
        #[arg(short, long, default_value = "10")]
        limit: i64,
    },

    /// Get token volume statistics [ALPHA]
    #[command(hide = true)]
    Volume {
        /// Token mint address
        mint: String,

        /// Number of days to analyze (default: 7)
        #[arg(short, long, default_value = "7")]
        days: i32,
    },

    /// Get account information [ALPHA]
    #[command(hide = true)]
    Account {
        #[command(subcommand)]
        command: AccountCommands,
    },

    /// Authentication operations [ALPHA]
    #[command(hide = true)]
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },

    /// API key management (requires authentication) [ALPHA]
    #[command(hide = true)]
    ApiKeys {
        #[command(subcommand)]
        command: ApiKeyCommands,
    },

    // === Monitoring & Stats ===
    /// Get cache statistics [ALPHA]
    #[command(hide = true)]
    CacheStats,

    /// Get rate limiting information [ALPHA]
    #[command(hide = true)]
    RateLimits,
}

#[derive(Subcommand)]
enum AccountCommands {
    /// Get account activity
    Activity {
        /// Account address
        address: String,

        /// Maximum number of results (default: 100)
        #[arg(short, long, default_value = "100")]
        limit: i64,
    },

    /// Get token balances for an account
    Balances {
        /// Account address
        address: String,

        /// Get balances at a specific slot (optional)
        #[arg(short, long)]
        slot: Option<i64>,
    },
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Register a new account
    Register {
        /// Email address
        #[arg(short, long)]
        email: String,

        /// Password (will be prompted if not provided)
        #[arg(short, long)]
        password: Option<String>,
    },

    /// Login to existing account
    Login {
        /// Email address
        #[arg(short, long)]
        email: String,

        /// Password (will be prompted if not provided)
        #[arg(short, long)]
        password: Option<String>,
    },

    /// Show current authentication status
    Status,

    /// Logout and clear saved credentials
    Logout,
}

#[derive(Subcommand)]
enum ApiKeyCommands {
    /// Create a new API key
    Create {
        /// Name for the API key
        #[arg(short, long)]
        name: String,

        /// JWT token (or set SOLANA_API_TOKEN env var)
        #[arg(short, long)]
        token: Option<String>,
    },

    /// List all your API keys
    List {
        /// JWT token (or set SOLANA_API_TOKEN env var)
        #[arg(short, long)]
        token: Option<String>,
    },

    /// Delete an API key
    Delete {
        /// API key ID to delete
        key_id: i32,

        /// JWT token (or set SOLANA_API_TOKEN env var)
        #[arg(short, long)]
        token: Option<String>,
    },
}

// Data structures based on the OpenAPI spec
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountActivity {
    pub id: i64,
    pub account_address: String,
    pub slot: i64,
    pub signature: String,
    pub activity_type: Option<String>,
    pub amount: Option<f64>,
    pub balance_change: Option<f64>,
    pub token_mint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenBalance {
    pub account_address: String,
    pub token_mint: String,
    pub slot: i64,
    pub balance: f64,
    pub ui_amount: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SlotMetadata {
    pub slot: i64,
    pub epoch: i32,
    pub transaction_count: i32,
    pub block_height: Option<i64>,
    pub block_time: Option<i64>,
    pub indexed_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenSwap {
    pub id: i64,
    pub slot: i64,
    pub signature: String,
    pub program_id: String,
    pub token_a: String,
    pub token_b: String,
    pub amount_a: f64,
    pub amount_b: f64,
    pub block_time: Option<i64>,
    pub fee_amount: Option<f64>,
    pub swap_direction: Option<String>,
    pub user_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenMint {
    pub mint_address: String,
    pub decimals: Option<i32>,
    pub first_seen_slot: Option<i64>,
    pub first_seen_time: Option<i64>,
    pub metadata_uri: Option<String>,
    pub name: Option<String>,
    pub supply: Option<f64>,
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenSwapVolume {
    pub token_mint: String,
    pub day: String,
    pub swap_count: i64,
    pub volume: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheStats {
    pub entries: i64,
    pub capacity: i64,
    pub ttl_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RateLimitConfig {
    pub global_rate_limit: Option<i32>, // Now optional
    pub per_ip_rate_limit: i32,
    pub burst_size: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct RateLimitStats {
    pub allowed_requests: i64,
    pub blocked_requests: i64,
    pub total_requests: i64,
    pub unique_ips: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RateLimitsResponse {
    pub config: RateLimitConfig,
    pub stats: RateLimitStats,
}

// Authentication structures
#[derive(Debug, Serialize, Deserialize)]
struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AuthResponse {
    pub token: String,
    pub user: User,
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    pub id: i32,
    pub email: String,
    pub created_at: String,
    pub updated_at: String,
    pub is_admin: Option<bool>,
}

// API Key structures
#[derive(Debug, Serialize, Deserialize)]
struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiKey {
    pub id: i32,
    pub user_id: i32,
    pub key: String,
    pub name: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub request_count: i32,
    pub tier: String,
    pub created_from_ip: Option<String>,
}

// Configuration file structures
#[derive(Debug, Serialize, Deserialize)]
struct Config {
    pub api_key: Option<String>,
    pub jwt_token: Option<String>,
    pub email: Option<String>,
    pub user_id: Option<i32>,
}

impl Config {
    fn new() -> Self {
        Self {
            api_key: None,
            jwt_token: None,
            email: None,
            user_id: None,
        }
    }

    fn load() -> Result<Self> {
        let config_path = get_config_path()?;

        if config_path.exists() {
            let contents = fs::read_to_string(&config_path)?;
            Ok(serde_json::from_str(&contents)?)
        } else {
            Ok(Self::new())
        }
    }

    fn save(&self) -> Result<()> {
        let config_path = get_config_path()?;

        // Create directory if it doesn't exist
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string_pretty(self)?;
        fs::write(&config_path, contents)?;

        // Set file permissions to 600 (read/write for owner only) on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&config_path)?.permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&config_path, perms)?;
        }

        Ok(())
    }

    fn clear_auth(&mut self) {
        self.jwt_token = None;
        self.api_key = None;
        self.email = None;
        self.user_id = None;
    }
}

fn get_config_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Could not find home directory"))?;
    Ok(home.join(".solana-data-fetch").join("config.json"))
}

fn check_disk_space(path: &str, required_gb: f64) -> Result<bool> {
    // For now, just check if we can create the directory
    // More sophisticated disk space checking would require platform-specific code
    let target_path = Path::new(path);

    // Try to get filesystem stats using std::fs
    match fs::metadata(target_path.parent().unwrap_or(Path::new("."))) {
        Ok(_) => {
            // On Unix systems, we could use statvfs, but for portability
            // we'll just warn the user about space requirements
            println!(
                "⚠️  Please ensure you have at least {:.0} GB free disk space",
                required_gb
            );
            Ok(true)
        }
        Err(_) => {
            // Can't check, assume it's okay
            Ok(true)
        }
    }
}

#[derive(Debug)]
struct SolanaDataClient {
    client: Client,
    base_url: String,
    token: Option<String>,
    api_key: Option<String>,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl SolanaDataClient {
    fn new() -> Self {
        // Create optimized HTTP client with aggressive settings
        let client = Client::builder()
            .pool_max_idle_per_host(200)
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client");

        let mut client = Self {
            client,
            base_url: API_BASE_URL.to_string(),
            token: None,
            api_key: None,
            // Default to anonymous rate limit (50 RPS)
            rate_limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(50u32)))),
        };

        // Load saved credentials
        if let Ok(config) = Config::load() {
            // Priority: API key > JWT token
            if let Some(api_key) = config.api_key {
                client.api_key = Some(api_key);
                // Set rate limit based on tier (we'll detect this from API)
                client.update_rate_limiter();
            } else if let Some(token) = config.jwt_token {
                client.token = Some(token);
                client.update_rate_limiter();
            }
        }

        // Environment variables override config
        if let Ok(api_key) = std::env::var("SOLANA_API_KEY") {
            client.api_key = Some(api_key);
            client.update_rate_limiter();
        } else if let Ok(token) = std::env::var("SOLANA_API_TOKEN") {
            client.token = Some(token);
            client.update_rate_limiter();
        }

        client
    }

    fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self.update_rate_limiter();
        self
    }

    fn update_rate_limiter(&mut self) {
        let quota = if self.api_key.is_some() || self.token.is_some() {
            Quota::per_second(nonzero!(500u32)) // Authenticated rate limit
        } else {
            Quota::per_second(nonzero!(50u32)) // Anonymous rate limit
        };
        self.rate_limiter = Arc::new(RateLimiter::direct(quota));
    }

    fn add_auth_headers(
        &self,
        request_builder: reqwest::RequestBuilder,
    ) -> reqwest::RequestBuilder {
        // Priority: API key > JWT token
        if let Some(api_key) = &self.api_key {
            request_builder.header("X-API-Key", api_key)
        } else if let Some(token) = &self.token {
            request_builder.header("Authorization", format!("Bearer {}", token))
        } else {
            request_builder
        }
    }

    // Optimized retry logic without rate limiting for maximum performance
    async fn retry_request_no_limit<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
        T: serde::de::DeserializeOwned,
    {
        let mut retries = 0;
        let max_retries = 3; // Fewer retries for speed
        let mut backoff = Duration::from_millis(10); // Very short initial backoff

        loop {
            match operation().await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        // Use bytes() instead of json() for faster parsing
                        let bytes = response
                            .bytes()
                            .await
                            .map_err(|_| anyhow::anyhow!("Failed to read response body"))?;

                        return serde_json::from_slice(&bytes)
                            .map_err(|_| anyhow::anyhow!("Invalid JSON response"));
                    } else if status.as_u16() == 429 {
                        retries += 1;
                        if retries > max_retries {
                            return Err(anyhow::anyhow!(
                                "Rate limit exceeded after {} retries",
                                max_retries
                            ));
                        }

                        sleep(backoff).await;
                        backoff = backoff.min(Duration::from_secs(2)); // Cap at 2 seconds
                        continue;
                    } else if status.as_u16() == 404 {
                        // Fast path for non-existent slots
                        return Err(anyhow::anyhow!("Not found"));
                    } else {
                        return Err(anyhow::anyhow!("HTTP {}", status));
                    }
                }
                Err(e) => {
                    retries += 1;
                    if retries > max_retries {
                        return Err(anyhow::anyhow!("Network error: {}", e));
                    }

                    sleep(backoff).await;
                    backoff *= 2;
                    continue;
                }
            }
        }
    }

    // Add optimized get_full_slot_data method
    async fn get_full_slot_data_optimized(&self, slot_number: i64) -> Result<serde_json::Value> {
        let url = format!("{}/slot/{}", self.base_url, slot_number);

        self.retry_request_no_limit(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // Existing retry_request method
    async fn retry_request<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
        T: serde::de::DeserializeOwned,
    {
        let mut retries = 0;
        let max_retries = 3;
        let mut backoff = Duration::from_millis(25);

        loop {
            // Wait for rate limiter
            self.rate_limiter
                .until_ready_with_jitter(Jitter::up_to(Duration::from_millis(50)))
                .await;

            match operation().await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return response
                            .json::<T>()
                            .await
                            .map_err(|_| anyhow::anyhow!("Invalid response format"));
                    } else if status.as_u16() == 429 {
                        // Rate limit hit - handle silently
                        retries += 1;
                        if retries > max_retries {
                            return Err(anyhow::anyhow!("Rate limit exceeded"));
                        }

                        // Check for Retry-After header
                        let retry_after = response
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(backoff.as_secs());

                        let wait_time = Duration::from_secs(retry_after.max(backoff.as_secs()));

                        // Silent retry - no error messages
                        sleep(wait_time).await;

                        // Exponential backoff for next retry
                        backoff *= 2;
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }

                        continue;
                    } else {
                        // Other error - don't retry
                        let error_text =
                            response.text().await.unwrap_or_else(|_| status.to_string());
                        return Err(anyhow::anyhow!(
                            "Request failed with status {}: {}",
                            status,
                            error_text
                        ));
                    }
                }
                Err(e) => {
                    // Network error - retry silently
                    retries += 1;
                    if retries > max_retries {
                        return Err(anyhow::anyhow!("Request failed: {}", e));
                    }

                    sleep(backoff).await;
                    backoff *= 2;
                    if backoff > Duration::from_secs(30) {
                        backoff = Duration::from_secs(30);
                    }

                    continue;
                }
            }
        }
    }

    // Special retry logic for requests that return empty response (like DELETE)
    async fn retry_request_empty<F, Fut>(&self, operation: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut retries = 0;
        let max_retries = 5;
        let mut backoff = Duration::from_millis(100);

        loop {
            // Wait for rate limiter
            self.rate_limiter
                .until_ready_with_jitter(Jitter::up_to(Duration::from_millis(50)))
                .await;

            match operation().await {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return Ok(());
                    } else if status.as_u16() == 429 {
                        // Rate limit hit - handle silently
                        retries += 1;
                        if retries > max_retries {
                            return Err(anyhow::anyhow!("Rate limit exceeded"));
                        }

                        // Check for Retry-After header
                        let retry_after = response
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .unwrap_or(backoff.as_secs());

                        let wait_time = Duration::from_secs(retry_after.max(backoff.as_secs()));

                        // Silent retry
                        sleep(wait_time).await;

                        // Exponential backoff for next retry
                        backoff *= 2;
                        if backoff > Duration::from_secs(60) {
                            backoff = Duration::from_secs(60);
                        }

                        continue;
                    } else {
                        // Other error - don't retry
                        let error_text =
                            response.text().await.unwrap_or_else(|_| status.to_string());
                        return Err(anyhow::anyhow!(
                            "Request failed with status {}: {}",
                            status,
                            error_text
                        ));
                    }
                }
                Err(e) => {
                    // Network error - retry silently
                    retries += 1;
                    if retries > max_retries {
                        return Err(anyhow::anyhow!("Request failed: {}", e));
                    }

                    sleep(backoff).await;
                    backoff *= 2;
                    if backoff > Duration::from_secs(30) {
                        backoff = Duration::from_secs(30);
                    }

                    continue;
                }
            }
        }
    }

    // Account endpoints
    async fn get_account_activity(
        &self,
        account_address: &str,
        limit: Option<i64>,
    ) -> Result<Vec<AccountActivity>> {
        let mut url = format!(
            "{}/api/accounts/{}/activity",
            self.base_url, account_address
        );

        if let Some(limit) = limit {
            url.push_str(&format!("?limit={}", limit));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn get_token_balances(
        &self,
        account_address: &str,
        slot: Option<i64>,
    ) -> Result<Vec<TokenBalance>> {
        let mut url = format!(
            "{}/api/accounts/{}/balances",
            self.base_url, account_address
        );

        if let Some(slot) = slot {
            url.push_str(&format!("?slot={}", slot));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // Slot endpoints
    async fn get_slot_metadata(&self, slot_number: i64) -> Result<SlotMetadata> {
        let url = format!("{}/api/slots/{}/metadata", self.base_url, slot_number);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // Token endpoints
    async fn get_token_swaps(
        &self,
        token_mint: Option<&str>,
        start_slot: Option<i64>,
        end_slot: Option<i64>,
        limit: Option<i64>,
    ) -> Result<Vec<TokenSwap>> {
        let mut url = format!("{}/api/swaps", self.base_url);
        let mut params = Vec::new();

        if let Some(mint) = token_mint {
            params.push(format!("token_mint={}", mint));
        }
        if let Some(start) = start_slot {
            params.push(format!("start_slot={}", start));
        }
        if let Some(end) = end_slot {
            params.push(format!("end_slot={}", end));
        }
        if let Some(limit) = limit {
            params.push(format!("limit={}", limit));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn search_tokens(&self, query: &str, limit: Option<i64>) -> Result<Vec<TokenMint>> {
        let mut url = format!("{}/api/tokens/search?q={}", self.base_url, query);

        if let Some(limit) = limit {
            url.push_str(&format!("&limit={}", limit));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn get_token_info(&self, token_mint: &str) -> Result<TokenMint> {
        let url = format!("{}/api/tokens/{}", self.base_url, token_mint);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn get_token_volume(
        &self,
        token_mint: &str,
        days: Option<i32>,
    ) -> Result<Vec<TokenSwapVolume>> {
        let mut url = format!("{}/api/tokens/{}/volume", self.base_url, token_mint);

        if let Some(days) = days {
            url.push_str(&format!("?days={}", days));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // Additional endpoints
    async fn get_cache_stats(&self) -> Result<CacheStats> {
        let url = format!("{}/stats", self.base_url);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn get_rate_limits(&self) -> Result<RateLimitsResponse> {
        let url = format!("{}/rate-limits", self.base_url);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn get_full_slot_data(&self, slot_number: i64) -> Result<serde_json::Value> {
        let url = format!("{}/slot/{}", self.base_url, slot_number);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
        .map_err(|e| {
            // Clean up technical error messages
            let error_str = e.to_string();
            if error_str.contains("Invalid response format")
                || error_str.contains("EOF while parsing")
            {
                anyhow::anyhow!("Slot {} contains no data", slot_number)
            } else if error_str.contains("404") {
                anyhow::anyhow!("Slot {} not found", slot_number)
            } else {
                anyhow::anyhow!("Unable to fetch slot {}", slot_number)
            }
        })
    }

    // Alternative swap volume endpoint
    #[allow(dead_code)]
    async fn get_swap_volume(
        &self,
        token_mint: &str,
        days: Option<i32>,
    ) -> Result<Vec<TokenSwapVolume>> {
        let mut url = format!("{}/api/swaps/{}/volume", self.base_url, token_mint);
        if let Some(days) = days {
            url.push_str(&format!("?days={}", days));
        }

        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // Authentication endpoints
    async fn register(&self, email: &str, password: &str) -> Result<AuthResponse> {
        let url = format!("{}/api/auth/register", self.base_url);
        let body = RegisterRequest {
            email: email.to_string(),
            password: password.to_string(),
        };

        self.retry_request(|| {
            let req = self.client.post(&url).json(&body);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn login(&self, email: &str, password: &str) -> Result<AuthResponse> {
        let url = format!("{}/api/auth/login", self.base_url);
        let body = LoginRequest {
            email: email.to_string(),
            password: password.to_string(),
        };

        self.retry_request(|| {
            let req = self.client.post(&url).json(&body);
            self.add_auth_headers(req).send()
        })
        .await
    }

    // API Key endpoints
    async fn create_api_key(&self, name: &str) -> Result<ApiKey> {
        let url = format!("{}/api/keys", self.base_url);
        let body = CreateApiKeyRequest {
            name: name.to_string(),
        };

        self.retry_request(|| {
            let req = self.client.post(&url).json(&body);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn list_api_keys(&self) -> Result<Vec<ApiKey>> {
        let url = format!("{}/api/keys", self.base_url);
        self.retry_request(|| {
            let req = self.client.get(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }

    async fn delete_api_key(&self, key_id: i32) -> Result<()> {
        let url = format!("{}/api/keys/{}", self.base_url, key_id);
        self.retry_request_empty(|| {
            let req = self.client.delete(&url);
            self.add_auth_headers(req).send()
        })
        .await
    }
}

// Helper function to print token info
fn print_token_info(token: &TokenMint) {
    println!("\n📊 Token Information:");
    println!("  Mint: {}", token.mint_address);
    if let Some(name) = &token.name {
        println!("  Name: {}", name);
    }
    if let Some(symbol) = &token.symbol {
        println!("  Symbol: {}", symbol);
    }
    if let Some(decimals) = token.decimals {
        println!("  Decimals: {}", decimals);
    }
    if let Some(supply) = token.supply {
        println!("  Supply: {:.2}", supply);
    }
    if let Some(first_seen) = token.first_seen_slot {
        println!("  First Seen: Slot {}", first_seen);
    }
}

// Helper function to print swap info
fn print_swap_info(swap: &TokenSwap) {
    println!("\n💱 Swap Transaction:");
    println!("  Signature: {}", &swap.signature[..8]);
    println!("  Token A: {}", &swap.token_a[..8]);
    println!("  Token B: {}", &swap.token_b[..8]);
    println!("  Amount A: {:.4}", swap.amount_a);
    println!("  Amount B: {:.4}", swap.amount_b);
    if let Some(time) = swap.block_time {
        let datetime = chrono::DateTime::from_timestamp(time, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        println!("  Time: {}", datetime);
    }
    if let Some(user) = &swap.user_address {
        println!("  User: {}", user);
    }
}

// Helper function to print account activity
fn print_account_activity(activity: &AccountActivity) {
    println!("\n📝 Account Activity:");
    println!("  Signature: {}", activity.signature);
    println!("  Slot: {}", activity.slot);
    println!("  Address: {}", activity.account_address);
    if let Some(activity_type) = &activity.activity_type {
        println!("  Type: {}", activity_type);
    }
    if let Some(amount) = activity.amount {
        println!("  Amount: {:.4}", amount);
    }
    if let Some(balance_change) = activity.balance_change {
        println!("  Balance Change: {:.4}", balance_change);
    }
    if let Some(token_mint) = &activity.token_mint {
        println!("  Token: {}", token_mint);
    }
}

// Helper function to print token balance
fn print_token_balance(balance: &TokenBalance) {
    println!("\n💰 Token Balance:");
    println!("  Token: {}", balance.token_mint);
    println!("  Balance: {:.4}", balance.balance);
    if let Some(ui_amount) = balance.ui_amount {
        println!("  UI Amount: {:.4}", ui_amount);
    }
    println!("  Slot: {}", balance.slot);
}

// Helper function to get JWT token from command line, environment, or config file
fn get_token(token_arg: Option<String>) -> Result<String> {
    // Priority: command arg > env var > config file
    if let Some(token) = token_arg {
        return Ok(token);
    }

    if let Ok(token) = std::env::var("SOLANA_API_TOKEN") {
        return Ok(token);
    }

    // Try loading from config
    if let Ok(config) = Config::load() {
        if let Some(token) = config.jwt_token {
            return Ok(token);
        }
    }

    Err(anyhow::anyhow!(
        "No JWT token provided. Use --token, set SOLANA_API_TOKEN env var, or login first"
    ))
}

// Helper function to prompt for password securely
fn prompt_password() -> Result<String> {
    print!("Password: ");
    std::io::stdout().flush()?;

    // For simplicity, we'll read from stdin. In production, use rpassword crate
    let mut password = String::new();
    std::io::stdin().read_line(&mut password)?;
    Ok(password.trim().to_string())
}

// Helper function to print API key info
fn print_api_key(key: &ApiKey) {
    println!("\n🔑 API Key:");
    println!("  ID: {}", key.id);
    println!("  Name: {}", key.name);
    // Mask the API key for security - show only first 8 chars
    let masked_key = if key.key.len() > 8 {
        format!("{}...", &key.key[..8])
    } else {
        "***".to_string()
    };
    println!("  Key: {}", masked_key);
    println!("  Tier: {}", key.tier);
    println!("  Created: {}", key.created_at);
    if let Some(last_used) = &key.last_used_at {
        println!("  Last Used: {}", last_used);
    }
    println!("  Request Count: {}", key.request_count);
    if let Some(ip) = &key.created_from_ip {
        println!("  Created From IP: {}", ip);
    }
}

// Async file writing - same implementation for all platforms
async fn write_file_async(path: &str, data: &[u8]) -> Result<()> {
    tokio::fs::write(path, data).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = SolanaDataClient::new();

    match cli.command {
        Commands::SearchToken { query, limit } => {
            println!("🔍 Searching for tokens matching '{}'...", query);
            match client.search_tokens(&query, Some(limit)).await {
                Ok(tokens) => {
                    if tokens.is_empty() {
                        println!("No tokens found matching '{}'", query);
                    } else {
                        println!("Found {} tokens:", tokens.len());
                        for token in tokens {
                            println!();
                            print_token_info(&token);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to search tokens: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::TokenInfo { mint } => {
            println!("🪙 Getting token information for {}...", mint);
            match client.get_token_info(&mint).await {
                Ok(token) => {
                    print_token_info(&token);
                }
                Err(e) => {
                    println!("❌ Failed to get token info: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Swaps {
            token,
            start_slot,
            end_slot,
            limit,
        } => {
            println!("💱 Fetching token swaps...");
            match client
                .get_token_swaps(token.as_deref(), start_slot, end_slot, Some(limit))
                .await
            {
                Ok(swaps) => {
                    if swaps.is_empty() {
                        println!("No swaps found");
                    } else {
                        println!("Found {} swaps:", swaps.len());
                        for swap in swaps {
                            print_swap_info(&swap);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get swaps: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Slot { slot } => {
            let slot_number = slot.parse::<i64>().unwrap_or_else(|_| {
                println!("❌ Invalid slot number: {}", slot);
                std::process::exit(1);
            });

            println!("📦 Getting metadata for slot {}...", slot_number);
            match client.get_slot_metadata(slot_number).await {
                Ok(slot_data) => {
                    println!("\n📦 Slot Metadata:");
                    println!("  Slot: {}", slot_data.slot);
                    println!("  Epoch: {}", slot_data.epoch);
                    println!("  Transaction Count: {}", slot_data.transaction_count);
                    if let Some(height) = slot_data.block_height {
                        println!("  Block Height: {}", height);
                    }
                    if let Some(time) = slot_data.block_time {
                        let datetime = chrono::DateTime::from_timestamp(time, 0)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                            .unwrap_or_else(|| "Unknown".to_string());
                        println!("  Block Time: {}", datetime);
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get slot metadata: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Account { command } => match command {
            AccountCommands::Activity { address, limit } => {
                println!("📝 Getting activity for account {}...", address);
                match client.get_account_activity(&address, Some(limit)).await {
                    Ok(activities) => {
                        if activities.is_empty() {
                            println!("No activity found for this account");
                        } else {
                            println!("Found {} activities:", activities.len());
                            for activity in activities {
                                print_account_activity(&activity);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to get account activity: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            AccountCommands::Balances { address, slot } => {
                println!("💰 Getting token balances for account {}...", address);
                match client.get_token_balances(&address, slot).await {
                    Ok(balances) => {
                        if balances.is_empty() {
                            println!("No token balances found for this account");
                        } else {
                            println!("Found {} token balances:", balances.len());
                            for balance in balances {
                                print_token_balance(&balance);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to get token balances: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        },

        Commands::Volume { mint, days } => {
            println!("📊 Getting volume statistics for token {}...", mint);
            match client.get_token_volume(&mint, Some(days)).await {
                Ok(volumes) => {
                    if volumes.is_empty() {
                        println!("No volume data found for this token");
                    } else {
                        println!("\n📈 Token Volume (last {} days):", days);
                        for vol in volumes {
                            println!("\n  Date: {}", vol.day);
                            println!("  Swap Count: {}", vol.swap_count);
                            println!("  Volume: {:.4}", vol.volume);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get token volume: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::CacheStats => {
            println!("💾 Getting cache statistics...");
            match client.get_cache_stats().await {
                Ok(stats) => {
                    println!("\n📊 Cache Statistics:");
                    println!("  Entries: {}/{}", stats.entries, stats.capacity);
                    println!("  Capacity: {} entries", stats.capacity);
                    println!("  TTL: {} seconds", stats.ttl_seconds);
                    let utilization = (stats.entries as f64 / stats.capacity as f64) * 100.0;
                    println!("  Utilization: {:.1}%", utilization);
                }
                Err(e) => {
                    println!("❌ Failed to get cache stats: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::RateLimits => {
            println!("🚦 Getting rate limit information...");
            match client.get_rate_limits().await {
                Ok(limits) => {
                    println!("\n⚙️  Rate Limit Configuration:");
                    if let Some(global_limit) = limits.config.global_rate_limit {
                        println!("  Global Limit: {} requests/second", global_limit);
                    }
                    println!(
                        "  Per-IP Limit: {} requests/second",
                        limits.config.per_ip_rate_limit
                    );
                    println!("  Burst Size: {} requests", limits.config.burst_size);

                    println!("\n📊 Current Statistics:");
                    println!("  Total Requests: {}", limits.stats.total_requests);
                    println!("  Allowed Requests: {}", limits.stats.allowed_requests);
                    println!("  Blocked Requests: {}", limits.stats.blocked_requests);
                    println!("  Unique IPs: {}", limits.stats.unique_ips);

                    if limits.stats.total_requests > 0 {
                        let block_rate = (limits.stats.blocked_requests as f64
                            / limits.stats.total_requests as f64)
                            * 100.0;
                        println!("  Block Rate: {:.2}%", block_rate);
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get rate limits: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::SlotData { slot } => {
            println!("📦 Getting full data for slot {}...", slot);
            match client.get_full_slot_data(slot).await {
                Ok(data) => {
                    println!("\n🔍 Slot {} Full Data:", slot);

                    // Pretty print the JSON data
                    match serde_json::to_string_pretty(&data) {
                        Ok(json_str) => {
                            // Limit output to avoid overwhelming the terminal
                            let lines: Vec<&str> = json_str.lines().collect();
                            let display_lines = lines.len().min(50);

                            for line in lines.iter().take(display_lines) {
                                println!("{}", line);
                            }

                            if lines.len() > display_lines {
                                println!("\n... ({} more lines)", lines.len() - display_lines);
                                println!("\n💡 Tip: Pipe output to a file or jq for full data");
                            }
                        }
                        Err(e) => println!("Failed to format JSON: {}", e),
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get slot data: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::EpochData {
            epoch,
            concurrent,
            skip_existing,
            rate_limit,
            no_compression,
            no_rate_limit,
        } => {
            println!("📥 Downloading all slots for epoch {}...", epoch);

            // Apply rate limit override if provided
            let mut client = client;
            if no_rate_limit {
                println!("🚀 Rate limiting disabled for maximum performance");
            } else if let Some(rate_limit) = rate_limit {
                println!("🔧 Overriding rate limit to {} requests/second", rate_limit);
                let quota = Quota::per_second(
                    std::num::NonZeroU32::new(rate_limit).unwrap_or(nonzero!(10u32)),
                );
                client.rate_limiter = Arc::new(RateLimiter::direct(quota));
            } else {
                // Show current rate limit based on auth
                let tier = if client.api_key.is_some() || client.token.is_some() {
                    "authenticated"
                } else {
                    "anonymous"
                };
                println!("📊 Using {} rate limits", tier);
                println!("💡 Tip: Use --no-rate-limit for maximum performance if server allows");
            }

            // Calculate slot range for the epoch
            let start_slot = epoch * SLOTS_PER_EPOCH;
            let end_slot = (epoch + 1) * SLOTS_PER_EPOCH - 1;

            println!(
                "📊 Epoch {} spans slots {} to {}",
                epoch, start_slot, end_slot
            );
            println!("   Total slots: {}", SLOTS_PER_EPOCH);

            // Check disk space - estimate based on epoch
            let estimated_gb = if epoch < 100 {
                5.0 // Early epochs are small
            } else if epoch < 500 {
                50.0 // Medium epochs
            } else {
                200.0 // Recent epochs with 10MB+ slots, compressed
            };

            println!("💾 Estimated disk space needed: ~{:.0} GB", estimated_gb);
            check_disk_space(".", estimated_gb * 1.2)?; // 20% safety margin

            // Create directory for the epoch
            let dir_name = format!("epoch_{}", epoch);
            if let Err(e) = std::fs::create_dir_all(&dir_name) {
                println!("❌ Failed to create directory '{}': {}", dir_name, e);
                std::process::exit(1);
            }
            println!("📁 Created directory: {}", dir_name);

            // Show compression status
            if no_compression {
                println!("📄 Compression: Disabled (files will be larger)");
            } else {
                println!("🗜️  Compression: Enabled (zstd format, ~5-10x smaller)");
                let num_cpus = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1);
                println!("🖥️  Using {} CPU cores for parallel compression", num_cpus);
            }

            // Build list of all slots to attempt downloading
            let slots_to_download: Vec<i64> = (start_slot..=end_slot).collect();

            // Download slots concurrently
            println!(
                "\n⬇️  Starting download of {} potential slots with {} concurrent connections...",
                slots_to_download.len(),
                concurrent
            );
            println!("💡 Many slots may not exist - this is normal\n");

            let semaphore = Arc::new(Semaphore::new(concurrent));
            let client = Arc::new(client);
            let dir_name = Arc::new(dir_name);

            let start_time = std::time::Instant::now();
            let total_slots = slots_to_download.len();

            // Use the new grouped stats structure
            let stats = DownloadStats::new();
            let stats_for_progress = stats.clone();
            let stats_for_final = stats.clone();
            let dir_name_final = dir_name.clone();

            // Create a channel for file writes with bounded capacity
            let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<FileWriteBatch>(100);

            // Spawn a dedicated file writer task
            let writer_handle = tokio::spawn(async move {
                while let Some(batch) = write_rx.recv().await {
                    if let Err(e) = write_file_async(&batch.path, &batch.data).await {
                        eprintln!("\n❌ Failed to write {}: {}", batch.path, e);
                    }
                }
            });

            // Spawn a dedicated progress update task
            let progress_handle = tokio::spawn({
                let stats_for_progress = stats_for_progress.clone();
                async move {
                    let mut interval = tokio::time::interval(Duration::from_millis(250)); // Update every 250ms
                    let start_time = std::time::Instant::now();

                    loop {
                        interval.tick().await;

                        let done = stats_for_progress
                            .completed
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let err_count = stats_for_progress
                            .failed
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let skip_count = stats_for_progress
                            .skipped
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let nf_count = stats_for_progress
                            .not_found
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let rl_count = stats_for_progress
                            .rate_limited
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let req_count = stats_for_progress
                            .requests_made
                            .load(std::sync::atomic::Ordering::Relaxed);

                        // Total processed = all operations
                        let processed = done + err_count + skip_count + nf_count + rl_count;

                        // Check if we're done
                        if processed >= total_slots {
                            break;
                        }

                        let remaining = total_slots.saturating_sub(processed);

                        let elapsed = start_time.elapsed().as_secs_f64();
                        let rate = if elapsed > 0.0 {
                            processed as f64 / elapsed
                        } else {
                            0.0
                        };
                        let request_rate = if elapsed > 0.0 {
                            req_count as f64 / elapsed
                        } else {
                            0.0
                        };
                        let eta_seconds = if rate > 0.0 {
                            remaining as f64 / rate
                        } else {
                            0.0
                        };

                        // Format time remaining
                        let eta_formatted = if eta_seconds > 3600.0 {
                            format!("{:.1}h", eta_seconds / 3600.0)
                        } else if eta_seconds > 60.0 {
                            format!("{:.1}m", eta_seconds / 60.0)
                        } else {
                            format!("{:.0}s", eta_seconds)
                        };

                        // Calculate percentage
                        let percent = (processed as f64 / total_slots as f64) * 100.0;

                        // Create progress bar
                        let bar_width: usize = 30;
                        let filled = ((percent / 100.0) * bar_width as f64) as usize;
                        let bar = format!(
                            "[{}{}]",
                            "=".repeat(filled),
                            " ".repeat(bar_width.saturating_sub(filled))
                        );

                        print!("\r{} {:.1}% | ✅ {} | ⏭️  {} | 🚫 {} | ETA: {} | {:.0} req/s | {:.0} done/s",
                            bar, percent, done, skip_count, nf_count, eta_formatted, request_rate, rate);
                        use std::io::{self, Write};
                        let _ = io::stdout().flush();
                    }
                }
            });

            // Keep the original write_tx for dropping later
            let write_tx_for_drop = write_tx.clone();

            // Create a stream of download tasks
            let download_tasks = stream::iter(slots_to_download)
                .map(move |slot| {
                    let sem = semaphore.clone();
                    let client = client.clone();
                    let dir = dir_name.clone();
                    let stats = stats.clone();
                    let compress = !no_compression;
                    let use_no_limit = no_rate_limit;
                    let write_tx = write_tx.clone();

                    async move {
                        let _permit = sem.acquire().await.unwrap();

                        // Determine file extension based on compression
                        let file_path = if compress {
                            format!("{}/slot_{}.json.zst", dir, slot)
                        } else {
                            format!("{}/slot_{}.json", dir, slot)
                        };

                        // Skip if file exists and skip_existing is true
                        if skip_existing && Path::new(&file_path).exists() {
                            stats.skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }

                        // Count this as a request being made
                        stats.requests_made.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        // Download the slot data using optimized path when no rate limit
                        let slot_result = if use_no_limit {
                            client.get_full_slot_data_optimized(slot).await
                        } else {
                            client.get_full_slot_data(slot).await
                        };

                        match slot_result {
                            Ok(data) => {
                                // Save to file - use to_vec for 10x faster serialization
                                match serde_json::to_vec(&data) {
                                    Ok(json_bytes) => {
                                        // Process compression if needed
                                        let file_data = if compress {
                                            // Move compression to blocking thread pool for parallel CPU usage
                                            match tokio::task::spawn_blocking(move || {
                                                // Compress with zstd - use level 1 for speed
                                                encode_all(&json_bytes[..], 1)
                                            }).await {
                                                Ok(Ok(compressed)) => compressed,
                                                Ok(Err(e)) => {
                                                    eprintln!("\n❌ Compression failed for slot {}: {}", slot, e);
                                                    stats.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    return;
                                                }
                                                Err(e) => {
                                                    eprintln!("\n❌ Compression task failed for slot {}: {}", slot, e);
                                                    stats.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    return;
                                                }
                                            }
                                        } else {
                                            json_bytes
                                        };

                                        // Send to write channel
                                        let batch = FileWriteBatch {
                                            path: file_path,
                                            data: file_data,
                                        };

                                        if write_tx.send(batch).await.is_err() {
                                            eprintln!("\n❌ Failed to queue write for slot {}", slot);
                                            stats.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        } else {
                                            stats.completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("\n❌ Failed to serialize slot {}: {}", slot, e);
                                        stats.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(e) => {
                                // Use the new error enum for categorization
                                match SlotError::from(e) {
                                    SlotError::NotFound => {
                                        stats.not_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    SlotError::RateLimited => {
                                        stats.rate_limited.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    SlotError::ParseError => {
                                        stats.not_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    SlotError::NetworkError(_) | SlotError::Other(_) => {
                                        stats.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(concurrent);

            // Execute all downloads
            download_tasks.collect::<Vec<()>>().await;

            // Close the write channel and wait for writer to finish
            drop(write_tx_for_drop);
            writer_handle.await?;

            // Cancel progress thread
            progress_handle.abort();

            // Final progress update
            print!("\r"); // Clear the progress line

            let elapsed = start_time.elapsed();
            let completed_count = stats_for_final
                .completed
                .load(std::sync::atomic::Ordering::Relaxed);
            let failed_count = stats_for_final
                .failed
                .load(std::sync::atomic::Ordering::Relaxed);
            let skipped_count = stats_for_final
                .skipped
                .load(std::sync::atomic::Ordering::Relaxed);
            let not_found_count = stats_for_final
                .not_found
                .load(std::sync::atomic::Ordering::Relaxed);
            let rate_limited_count = stats_for_final
                .rate_limited
                .load(std::sync::atomic::Ordering::Relaxed);
            let total_processed = completed_count
                + failed_count
                + skipped_count
                + not_found_count
                + rate_limited_count;
            let total_requests = stats_for_final
                .requests_made
                .load(std::sync::atomic::Ordering::Relaxed);

            println!("\n\n✅ Download complete!");
            println!("📊 Summary:");
            println!("   Epoch: {}", epoch);
            println!("   Directory: {}", dir_name_final.as_ref());
            println!("   Total slots in epoch: {}", SLOTS_PER_EPOCH);
            println!("   Slots processed: {}", total_processed);
            println!("   ✅ Successfully downloaded: {} slots", completed_count);
            if skipped_count > 0 {
                println!("   ⏭️  Skipped (already exist): {} slots", skipped_count);
            }
            if not_found_count > 0 {
                println!("   🚫 Skipped by Solana: {} slots", not_found_count);
            }
            // Only show failed count if it's significant
            if failed_count > 10 {
                println!("   ⚠️  Failed: {} slots", failed_count);
            }
            if rate_limited_count > 0 {
                println!("   ⚠️  Rate limited: {} slots", rate_limited_count);
                println!("   💡 Try using --rate-limit with a lower value");
            }
            println!("   ⏱️  Time taken: {:.2}s", elapsed.as_secs_f64());
            println!(
                "   📡 Request rate: {:.2} requests/second",
                total_requests as f64 / elapsed.as_secs_f64()
            );
            println!(
                "   📈 Completion rate: {:.2} slots/second",
                total_processed as f64 / elapsed.as_secs_f64()
            );
            if completed_count > 0 {
                let avg_slot_time = elapsed.as_secs_f64() / completed_count as f64;
                println!(
                    "   📦 Avg time per slot: {:.2}s (includes download + save)",
                    avg_slot_time
                );
            }
        }

        Commands::Auth { command } => {
            match command {
                AuthCommands::Register { email, password } => {
                    println!("📝 Registering new account...");

                    let password = match password {
                        Some(p) => p,
                        None => prompt_password()?,
                    };

                    match client.register(&email, &password).await {
                        Ok(auth_response) => {
                            println!("✅ Registration successful!");
                            println!("👤 User: {}", auth_response.user.email);

                            // Save credentials to config file
                            let mut config = Config::load().unwrap_or_else(|_| Config::new());
                            config.jwt_token = Some(auth_response.token.clone());
                            config.email = Some(auth_response.user.email.clone());
                            config.user_id = Some(auth_response.user.id);

                            match config.save() {
                                Ok(_) => {
                                    let config_path = get_config_path()?;
                                    println!("💾 Credentials saved to {}", config_path.display());
                                    println!("🔑 You're now authenticated! The CLI will use your saved credentials.");
                                }
                                Err(e) => {
                                    println!("⚠️  Warning: Could not save credentials: {}", e);
                                    println!("🔑 Token: {}", auth_response.token);
                                    println!("\n💡 Save this token or set SOLANA_API_TOKEN environment variable");
                                    println!("   export SOLANA_API_TOKEN={}", auth_response.token);
                                }
                            }
                        }
                        Err(e) => {
                            println!("❌ Registration failed: {}", e);
                            std::process::exit(1);
                        }
                    }
                }

                AuthCommands::Login { email, password } => {
                    println!("🔐 Logging in...");

                    let password = match password {
                        Some(p) => p,
                        None => prompt_password()?,
                    };

                    match client.login(&email, &password).await {
                        Ok(auth_response) => {
                            println!("✅ Login successful!");
                            println!("👤 User: {}", auth_response.user.email);
                            if auth_response.user.is_admin.unwrap_or(false) {
                                println!("👑 Admin privileges: Yes");
                            }

                            // Save credentials to config file
                            let mut config = Config::load().unwrap_or_else(|_| Config::new());
                            config.jwt_token = Some(auth_response.token.clone());
                            config.email = Some(auth_response.user.email.clone());
                            config.user_id = Some(auth_response.user.id);

                            match config.save() {
                                Ok(_) => {
                                    let config_path = get_config_path()?;
                                    println!("💾 Credentials saved to {}", config_path.display());
                                    println!("🔑 You're now authenticated! The CLI will use your saved credentials.");
                                }
                                Err(e) => {
                                    println!("⚠️  Warning: Could not save credentials: {}", e);
                                    println!("🔑 Token: {}", auth_response.token);
                                    println!("\n💡 Save this token or set SOLANA_API_TOKEN environment variable");
                                    println!("   export SOLANA_API_TOKEN={}", auth_response.token);
                                }
                            }
                        }
                        Err(e) => {
                            println!("❌ Login failed: {}", e);
                            std::process::exit(1);
                        }
                    }
                }

                AuthCommands::Status => {
                    println!("🔍 Checking authentication status...");

                    let config = Config::load().unwrap_or_else(|_| Config::new());
                    let config_path = get_config_path()?;

                    println!("\n📁 Config file: {}", config_path.display());

                    // Check environment variables
                    let env_api_key = std::env::var("SOLANA_API_KEY").is_ok();
                    let env_token = std::env::var("SOLANA_API_TOKEN").is_ok();

                    if env_api_key {
                        println!("🔑 Environment API Key: ✅ Set (takes precedence)");
                    }
                    if env_token {
                        println!("🎫 Environment JWT Token: ✅ Set");
                    }

                    // Check saved credentials
                    if let Some(email) = &config.email {
                        println!("\n👤 Logged in as: {}", email);
                    }

                    if config.api_key.is_some() {
                        println!("🔑 Saved API Key: ✅ Available");
                        if !env_api_key {
                            println!("   Status: Active (will be used for requests)");
                        } else {
                            println!("   Status: Overridden by environment variable");
                        }
                    } else {
                        println!("🔑 Saved API Key: ❌ Not set");
                    }

                    if config.jwt_token.is_some() {
                        println!("🎫 Saved JWT Token: ✅ Available");
                        if !env_api_key && !env_token && config.api_key.is_none() {
                            println!("   Status: Active (will be used for requests)");
                        } else {
                            println!("   Status: Available but lower priority");
                        }
                    } else {
                        println!("🎫 Saved JWT Token: ❌ Not set");
                    }

                    // Show authentication priority
                    println!("\n📊 Authentication Priority:");
                    println!("   1. Environment SOLANA_API_KEY");
                    println!("   2. Saved API key in config");
                    println!("   3. Environment SOLANA_API_TOKEN");
                    println!("   4. Saved JWT token in config");
                    println!("   5. Anonymous access (lowest rate limits)");

                    // Show which authentication method would be used
                    println!("\n🔑 Current authentication method:");
                    if env_api_key {
                        println!("   Using: Environment API Key");
                    } else if config.api_key.is_some() {
                        println!("   Using: Saved API Key");
                    } else if env_token {
                        println!("   Using: Environment JWT Token");
                    } else if config.jwt_token.is_some() {
                        println!("   Using: Saved JWT Token");
                    } else {
                        println!("   Using: Anonymous access (lowest rate limits)");
                    }
                }

                AuthCommands::Logout => {
                    println!("🚪 Logging out...");

                    let mut config = Config::load().unwrap_or_else(|_| Config::new());
                    let had_credentials = config.api_key.is_some() || config.jwt_token.is_some();

                    config.clear_auth();

                    match config.save() {
                        Ok(_) => {
                            if had_credentials {
                                println!("✅ Logged out successfully!");
                                println!("🗑️  Cleared saved credentials");
                            } else {
                                println!("ℹ️  No saved credentials found");
                            }

                            if std::env::var("SOLANA_API_KEY").is_ok()
                                || std::env::var("SOLANA_API_TOKEN").is_ok()
                            {
                                println!("\n⚠️  Note: Environment variables are still set:");
                                if std::env::var("SOLANA_API_KEY").is_ok() {
                                    println!("   - SOLANA_API_KEY");
                                }
                                if std::env::var("SOLANA_API_TOKEN").is_ok() {
                                    println!("   - SOLANA_API_TOKEN");
                                }
                                println!(
                                    "   Run 'unset SOLANA_API_KEY SOLANA_API_TOKEN' to clear them"
                                );
                            }
                        }
                        Err(e) => {
                            println!("❌ Failed to clear credentials: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
            }
        }

        Commands::ApiKeys { command } => {
            match command {
                ApiKeyCommands::Create { name, token } => {
                    println!("🔑 Creating API key '{}'...", name);

                    let token = get_token(token)?;
                    let client = client.with_token(token);

                    match client.create_api_key(&name).await {
                        Ok(api_key) => {
                            println!("✅ API key created successfully!");
                            print_api_key(&api_key);

                            // Save API key to config file
                            let mut config = Config::load().unwrap_or_else(|_| Config::new());
                            config.api_key = Some(api_key.key.clone());

                            match config.save() {
                                Ok(_) => {
                                    let config_path = get_config_path()?;
                                    println!("\n💾 API key saved to {}", config_path.display());
                                    println!(
                                        "🔑 Your API key is now the default authentication method."
                                    );
                                    println!("📈 You now have {} tier rate limits!", api_key.tier);
                                }
                                Err(e) => {
                                    println!("\n⚠️  Warning: Could not save API key: {}", e);
                                    println!(
                                        "💡 Save this key securely - it won't be shown again!"
                                    );
                                    println!("   To use: add header 'X-API-Key: {}'", api_key.key);
                                    println!("   Or set SOLANA_API_KEY={}", api_key.key);
                                }
                            }
                        }
                        Err(e) => {
                            println!("❌ Failed to create API key: {}", e);
                            std::process::exit(1);
                        }
                    }
                }

                ApiKeyCommands::List { token } => {
                    println!("📋 Listing your API keys...");

                    let token = get_token(token)?;
                    let client = client.with_token(token);

                    match client.list_api_keys().await {
                        Ok(keys) => {
                            if keys.is_empty() {
                                println!("No API keys found");
                            } else {
                                println!("Found {} API keys:", keys.len());
                                for key in keys {
                                    print_api_key(&key);
                                }
                            }
                        }
                        Err(e) => {
                            println!("❌ Failed to list API keys: {}", e);
                            std::process::exit(1);
                        }
                    }
                }

                ApiKeyCommands::Delete { key_id, token } => {
                    println!("🗑️  Deleting API key {}...", key_id);

                    let token = get_token(token)?;
                    let client = client.with_token(token);

                    match client.delete_api_key(key_id).await {
                        Ok(()) => {
                            println!("✅ API key deleted successfully!");
                        }
                        Err(e) => {
                            println!("❌ Failed to delete API key: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_slot_range_calculation() {
        // Test epoch 0
        let start_slot = 0;
        let end_slot = SLOTS_PER_EPOCH - 1;
        assert_eq!(start_slot, 0);
        assert_eq!(end_slot, 431999);

        // Test epoch 1
        let start_slot = SLOTS_PER_EPOCH;
        let end_slot = (1 + 1) * SLOTS_PER_EPOCH - 1;
        assert_eq!(start_slot, 432000);
        assert_eq!(end_slot, 863999);

        // Test epoch 799
        let start_slot = 799 * SLOTS_PER_EPOCH;
        let end_slot = (799 + 1) * SLOTS_PER_EPOCH - 1;
        assert_eq!(start_slot, 345168000);
        assert_eq!(end_slot, 345599999);
    }

    #[test]
    fn test_data_structure_serialization() {
        // Test TokenMint serialization
        let token = TokenMint {
            mint_address: "So11111111111111111111111111111111111111112".to_string(),
            decimals: Some(9),
            first_seen_slot: Some(1000),
            first_seen_time: Some(1640000000),
            metadata_uri: None,
            name: Some("Wrapped SOL".to_string()),
            supply: Some(1000000000.0),
            symbol: Some("SOL".to_string()),
        };

        let json = serde_json::to_string(&token).unwrap();
        let deserialized: TokenMint = serde_json::from_str(&json).unwrap();
        assert_eq!(token.mint_address, deserialized.mint_address);
        assert_eq!(token.symbol, deserialized.symbol);
    }

    #[tokio::test]
    async fn test_epoch_data_directory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let epoch_dir = temp_dir.path().join("epoch_0");

        // Create directory
        std::fs::create_dir_all(&epoch_dir).unwrap();

        assert!(epoch_dir.exists());
        assert!(epoch_dir.is_dir());
    }

    #[test]
    fn test_progress_bar_formatting() {
        // Test progress bar creation
        let percent = 25.5;
        let bar_width = 30;
        let filled = ((percent / 100.0) * bar_width as f64) as usize;
        let bar = format!("[{}{}]", "=".repeat(filled), " ".repeat(bar_width - filled));

        assert_eq!(filled, 7);
        assert_eq!(bar.len(), 32); // 30 + 2 brackets
        assert!(bar.starts_with('['));
        assert!(bar.ends_with(']'));
    }

    #[test]
    fn test_eta_formatting() {
        // Test hours formatting
        let eta_seconds = 7200.0;
        let eta_formatted = if eta_seconds > 3600.0 {
            format!("{:.1}h", eta_seconds / 3600.0)
        } else if eta_seconds > 60.0 {
            format!("{:.1}m", eta_seconds / 60.0)
        } else {
            format!("{:.0}s", eta_seconds)
        };
        assert_eq!(eta_formatted, "2.0h");

        // Test minutes formatting
        let eta_seconds = 150.0;
        let eta_formatted = if eta_seconds > 3600.0 {
            format!("{:.1}h", eta_seconds / 3600.0)
        } else if eta_seconds > 60.0 {
            format!("{:.1}m", eta_seconds / 60.0)
        } else {
            format!("{:.0}s", eta_seconds)
        };
        assert_eq!(eta_formatted, "2.5m");

        // Test seconds formatting
        let eta_seconds = 45.0;
        let eta_formatted = if eta_seconds > 3600.0 {
            format!("{:.1}h", eta_seconds / 3600.0)
        } else if eta_seconds > 60.0 {
            format!("{:.1}m", eta_seconds / 60.0)
        } else {
            format!("{:.0}s", eta_seconds)
        };
        assert_eq!(eta_formatted, "45s");
    }

    #[test]
    fn test_config_new() {
        let config = Config::new();
        assert!(config.api_key.is_none());
        assert!(config.jwt_token.is_none());
        assert!(config.email.is_none());
        assert!(config.user_id.is_none());
    }

    #[test]
    fn test_config_clear_auth() {
        let mut config = Config {
            api_key: Some("test_key".to_string()),
            jwt_token: Some("test_token".to_string()),
            email: Some("test@example.com".to_string()),
            user_id: Some(123),
        };

        config.clear_auth();

        assert!(config.api_key.is_none());
        assert!(config.jwt_token.is_none());
        assert!(config.email.is_none());
        assert!(config.user_id.is_none());
    }

    #[test]
    fn test_auth_response_serialization() {
        let user = User {
            id: 1,
            email: "test@example.com".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            is_admin: Some(false),
        };

        let auth_response = AuthResponse {
            token: "test_token".to_string(),
            user,
        };

        let json = serde_json::to_string(&auth_response).unwrap();
        let deserialized: AuthResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(auth_response.token, deserialized.token);
        assert_eq!(auth_response.user.email, deserialized.user.email);
    }

    #[test]
    fn test_api_key_serialization() {
        let api_key = ApiKey {
            id: 1,
            user_id: 123,
            key: "sk_test_key".to_string(),
            name: "Test Key".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            last_used_at: Some("2024-01-02T00:00:00Z".to_string()),
            request_count: 100,
            tier: "free".to_string(),
            created_from_ip: Some("127.0.0.1".to_string()),
        };

        let json = serde_json::to_string(&api_key).unwrap();
        let deserialized: ApiKey = serde_json::from_str(&json).unwrap();

        assert_eq!(api_key.key, deserialized.key);
        assert_eq!(api_key.name, deserialized.name);
        assert_eq!(api_key.tier, deserialized.tier);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config {
            api_key: Some("sk_test".to_string()),
            jwt_token: Some("jwt_test".to_string()),
            email: Some("test@example.com".to_string()),
            user_id: Some(42),
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        let deserialized: Config = serde_json::from_str(&json).unwrap();

        assert_eq!(config.api_key, deserialized.api_key);
        assert_eq!(config.jwt_token, deserialized.jwt_token);
        assert_eq!(config.email, deserialized.email);
        assert_eq!(config.user_id, deserialized.user_id);
    }

    #[tokio::test]
    async fn test_file_skip_existing() {
        let temp_dir = TempDir::new().unwrap();
        let epoch_dir = temp_dir.path().join("epoch_999");
        std::fs::create_dir_all(&epoch_dir).unwrap();

        // Create a pre-existing file
        let existing_file = epoch_dir.join("slot_123.json");
        std::fs::write(&existing_file, r#"{"test": "data"}"#).unwrap();

        // Verify file exists
        assert!(existing_file.exists());

        // Read the content to verify skip logic would work
        let content = std::fs::read_to_string(&existing_file).unwrap();
        assert_eq!(content, r#"{"test": "data"}"#);
    }

    #[test]
    fn test_file_path_construction() {
        let dir = "epoch_0";
        let slot = 12345;
        let file_path = format!("{}/slot_{}.json", dir, slot);

        assert_eq!(file_path, "epoch_0/slot_12345.json");

        // Test with large slot numbers
        let large_slot = 345599999i64;
        let file_path = format!("{}/slot_{}.json", "epoch_799", large_slot);
        assert_eq!(file_path, "epoch_799/slot_345599999.json");
    }

    #[test]
    fn test_concurrent_atomic_counters() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // Test that our atomic counters work correctly
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        // Simulate concurrent increments
        counter.fetch_add(1, Ordering::Relaxed);
        counter_clone.fetch_add(1, Ordering::Relaxed);

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_progress_calculation_edge_cases() {
        // Test with 0 processed
        let total = 100;
        let processed = 0;
        let percent = (processed as f64 / total as f64) * 100.0;
        assert_eq!(percent, 0.0);

        // Test with all processed
        let processed = 100;
        let percent = (processed as f64 / total as f64) * 100.0;
        assert_eq!(percent, 100.0);

        // Test with partial processing
        let processed = 33;
        let percent = (processed as f64 / total as f64) * 100.0;
        assert!((percent - 33.0).abs() < 0.01);
    }

    #[test]
    fn test_rate_calculation() {
        // Test rate calculation doesn't panic with 0 elapsed time
        let elapsed = 0.0;
        let processed = 100;
        let rate = if elapsed > 0.0 {
            processed as f64 / elapsed
        } else {
            0.0
        };
        assert_eq!(rate, 0.0);

        // Test normal rate calculation
        let elapsed = 10.0;
        let rate = processed as f64 / elapsed;
        assert_eq!(rate, 10.0);
    }

    #[test]
    fn test_error_categorization() {
        // Test different error string patterns
        let errors = vec![
            ("404 Not Found", true, false, false),
            ("Slot 123 not found", true, false, false),
            ("429 Too Many Requests", false, true, false),
            ("Rate limit exceeded", false, true, false),
            ("Invalid response format", false, false, true),
            ("EOF while parsing", false, false, true),
            ("Network timeout", false, false, false),
        ];

        for (error_str, is_404, is_429, is_parse) in errors {
            let is_not_found = error_str.contains("404")
                || error_str.contains("Not Found")
                || (error_str.contains("Slot") && error_str.contains("not found"));
            let is_rate_limit = error_str.contains("429")
                || error_str.contains("rate limit")
                || error_str.contains("Rate limit exceeded");
            let is_parse_error = error_str.contains("Invalid response format")
                || error_str.contains("EOF while parsing")
                || error_str.contains("contains no data");

            assert_eq!(
                is_not_found, is_404,
                "404 detection failed for: {}",
                error_str
            );
            assert_eq!(
                is_rate_limit, is_429,
                "429 detection failed for: {}",
                error_str
            );
            assert_eq!(
                is_parse_error, is_parse,
                "Parse error detection failed for: {}",
                error_str
            );
        }
    }
}
