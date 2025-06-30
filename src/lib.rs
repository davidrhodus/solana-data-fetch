// Export necessary types for testing
use anyhow::Result;
pub use reqwest::Client;
use serde::{Deserialize, Serialize};

pub const API_BASE_URL: &str = "https://api.pipe-solana.com";

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenMint {
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
pub struct SlotMetadata {
    pub slot: i64,
    pub epoch: i32,
    pub transaction_count: i32,
    pub block_height: Option<i64>,
    pub block_time: Option<i64>,
    pub indexed_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheStats {
    pub entries: i64,
    pub capacity: i64,
    pub ttl_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub global_rate_limit: Option<i32>, // Now optional
    pub per_ip_rate_limit: i32,
    pub burst_size: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RateLimitStats {
    pub allowed_requests: i64,
    pub blocked_requests: i64,
    pub total_requests: i64,
    pub unique_ips: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RateLimitsResponse {
    pub config: RateLimitConfig,
    pub stats: RateLimitStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenSwap {
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

#[derive(Debug)]
pub struct SolanaDataClient {
    client: Client,
    base_url: String,
}

impl SolanaDataClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            base_url: API_BASE_URL.to_string(),
        }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    pub async fn search_tokens(&self, query: &str, limit: Option<i64>) -> Result<Vec<TokenMint>> {
        let mut url = format!("{}/api/tokens/search?q={}", self.base_url, query);
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={limit}"));
        }

        let response = self.client.get(&url).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to search tokens: {}",
                response.status()
            ))
        }
    }

    pub async fn get_token_info(&self, token_mint: &str) -> Result<TokenMint> {
        let url = format!("{}/api/tokens/{}", self.base_url, token_mint);
        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to get token info: {}",
                response.status()
            ))
        }
    }

    pub async fn get_slot_metadata(&self, slot_number: i64) -> Result<SlotMetadata> {
        let url = format!("{}/api/slots/{}/metadata", self.base_url, slot_number);
        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to get slot metadata: {}",
                response.status()
            ))
        }
    }

    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        let url = format!("{}/stats", self.base_url);
        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to get cache stats: {}",
                response.status()
            ))
        }
    }

    pub async fn get_rate_limits(&self) -> Result<RateLimitsResponse> {
        let url = format!("{}/rate-limits", self.base_url);
        let response = self.client.get(&url).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to get rate limits: {}",
                response.status()
            ))
        }
    }

    pub async fn get_token_swaps(
        &self,
        token_mint: Option<&str>,
        start_slot: Option<i64>,
        end_slot: Option<i64>,
        limit: Option<i64>,
    ) -> Result<Vec<TokenSwap>> {
        let mut url = format!("{}/api/swaps", self.base_url);
        let mut params = Vec::new();

        if let Some(token) = token_mint {
            params.push(format!("token_mint={token}"));
        }
        if let Some(start) = start_slot {
            params.push(format!("start_slot={start}"));
        }
        if let Some(end) = end_slot {
            params.push(format!("end_slot={end}"));
        }
        if let Some(limit) = limit {
            params.push(format!("limit={limit}"));
        }

        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        let response = self.client.get(&url).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow::anyhow!(
                "Failed to get token swaps: {}",
                response.status()
            ))
        }
    }
}

impl Default for SolanaDataClient {
    fn default() -> Self {
        Self::new()
    }
}
