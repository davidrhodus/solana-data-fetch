// Integration tests that hit the real API
// Run with: cargo test --test api_test

use solana_data_fetch::*;
use std::thread;
use std::time::Duration;

#[tokio::test]
async fn test_api_token_search_real() {
    thread::sleep(Duration::from_millis(200));

    let client = SolanaDataClient::new();
    let result = client.search_tokens("USDC", Some(5)).await;

    // Allow rate limiting as a valid response
    match result {
        Ok(tokens) => {
            // If successful, check we got results
            assert!(!tokens.is_empty(), "No USDC tokens found");
        }
        Err(e) => {
            // Check if it's a rate limit error (429)
            let error_str = e.to_string();
            if !error_str.contains("429") {
                panic!("Token search failed: {:?}", e);
            }
            // Rate limiting is acceptable in tests
            println!("Rate limited in test - this is acceptable");
        }
    }
}

#[tokio::test]
async fn test_api_cache_stats_real() {
    let client = SolanaDataClient::new();
    let result = client.get_cache_stats().await;

    assert!(result.is_ok(), "Cache stats failed: {:?}", result.err());
    let stats = result.unwrap();
    assert!(stats.capacity > 0);
    assert_eq!(stats.ttl_seconds, 3600);
}

#[tokio::test]
async fn test_api_rate_limits_real() {
    let client = SolanaDataClient::new();
    let result = client.get_rate_limits().await;

    assert!(
        result.is_ok(),
        "Rate limits request failed: {:?}",
        result.err()
    );
    let limits = result.unwrap();
    assert!(limits.config.per_ip_rate_limit > 0);
    assert!(limits.config.burst_size > 0);
}

#[tokio::test]
async fn test_api_slot_metadata_real() {
    let client = SolanaDataClient::new();

    // Use a known slot number
    let slot = 100_000;

    let result = client.get_slot_metadata(slot).await;
    assert!(result.is_ok(), "Slot metadata failed: {:?}", result.err());

    let metadata = result.unwrap();
    assert_eq!(metadata.slot, slot);
    assert!(metadata.transaction_count >= 0);
}

#[tokio::test]
async fn test_api_nonexistent_slot() {
    let client = SolanaDataClient::new();
    let result = client.get_slot_metadata(999999999999).await;

    assert!(result.is_err(), "Expected error for non-existent slot");
}

#[tokio::test]
async fn test_api_token_info_sol() {
    let client = SolanaDataClient::new();
    let sol_mint = "So11111111111111111111111111111111111111112";

    let result = client.get_token_info(sol_mint).await;
    assert!(result.is_ok(), "SOL token info failed: {:?}", result.err());

    let token = result.unwrap();
    assert_eq!(token.mint_address, sol_mint);
}

#[tokio::test]
async fn test_api_recent_swaps() {
    let client = SolanaDataClient::new();
    let result = client.get_token_swaps(None, None, None, Some(5)).await;

    assert!(result.is_ok(), "Get swaps failed: {:?}", result.err());
    let swaps = result.unwrap();

    // May or may not have swaps, but shouldn't error
    if !swaps.is_empty() {
        assert!(swaps[0].slot > 0);
        assert!(!swaps[0].signature.is_empty());
    }
}

// Rate control tests against production API
#[tokio::test]
async fn test_rate_limiting_anonymous() {
    use std::time::Instant;

    let client = SolanaDataClient::new(); // Anonymous client (10 RPS)
    let start = Instant::now();

    // Try to make 15 requests rapidly
    let mut successes = 0;
    let mut rate_limited = 0;

    for i in 0..15 {
        let result = client.get_slot_metadata(100_000 + i).await;
        match result {
            Ok(_) => successes += 1,
            Err(e) => {
                if e.to_string().contains("429") || e.to_string().contains("rate limit") {
                    rate_limited += 1;
                }
            }
        }
    }

    let duration = start.elapsed();

    // With 10 RPS limit, 15 requests should take at least 1 second
    assert!(
        duration.as_secs_f64() >= 1.0,
        "Requests too fast: {:?}",
        duration
    );

    // We should see some rate limiting behavior
    println!(
        "Anonymous rate test: {} successes, {} rate limited in {:?}",
        successes, rate_limited, duration
    );
}

#[tokio::test]
async fn test_rate_limiter_spacing() {
    use std::time::Instant;

    let client = SolanaDataClient::new();
    let mut timings = Vec::new();

    // Make 5 requests and measure timing
    for i in 0..5 {
        let start = Instant::now();
        let _ = client.get_slot_metadata(200_000 + i).await;
        timings.push(start.elapsed());
    }

    // With 10 RPS, requests should be spaced ~100ms apart
    for (i, timing) in timings.iter().enumerate().skip(1) {
        let gap = timing.as_millis();
        // Allow some variance but should be roughly 100ms
        assert!(gap >= 80, "Request {} came too quickly: {}ms", i, gap);
    }
}

#[tokio::test]
async fn test_burst_then_sustained() {
    use std::time::{Duration, Instant};

    let client = SolanaDataClient::new();

    // First burst of requests
    let burst_start = Instant::now();
    for i in 0..5 {
        let _ = client.get_slot_metadata(300_000 + i).await;
    }
    let burst_duration = burst_start.elapsed();

    // Wait a bit
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Another burst
    let second_start = Instant::now();
    for i in 0..5 {
        let _ = client.get_slot_metadata(400_000 + i).await;
    }
    let second_duration = second_start.elapsed();

    println!(
        "First burst: {:?}, Second burst: {:?}",
        burst_duration, second_duration
    );

    // Both bursts should respect rate limits
    assert!(burst_duration.as_millis() >= 400, "First burst too fast");
    assert!(second_duration.as_millis() >= 400, "Second burst too fast");
}

#[tokio::test]
async fn test_concurrent_requests_rate_limited() {
    use futures::future::join_all;
    use std::time::Instant;

    let client = std::sync::Arc::new(SolanaDataClient::new());
    let start = Instant::now();

    // Launch 10 concurrent requests
    let mut handles = vec![];
    for i in 0..10 {
        let client = client.clone();
        let handle = tokio::spawn(async move { client.get_slot_metadata(500_000 + i).await });
        handles.push(handle);
    }

    // Wait for all to complete
    let results = join_all(handles).await;
    let duration = start.elapsed();

    // Rate limiter may allow some burst, but should still show some limiting
    // Allow for burst behavior but ensure it's not instant
    assert!(
        duration.as_millis() >= 200,
        "Concurrent requests too fast: {:?}",
        duration
    );

    // All should eventually succeed (no panics)
    let successful = results.iter().filter(|r| r.is_ok()).count();
    assert!(successful > 0, "No requests succeeded");

    println!(
        "Concurrent test: {} successful requests in {:?}",
        successful, duration
    );
}
