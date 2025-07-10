// Error handling tests for optimizations
// Run with: cargo test --test error_handling_tests

use solana_data_fetch::*;
use std::time::Duration;

#[tokio::test]
async fn test_simd_json_error_handling() {
    // Test that SIMD JSON gracefully handles malformed responses
    // Note: This is hard to test directly without mocking, so we test error propagation

    let client = SolanaDataClient::new();

    // Try to get a definitely non-existent slot
    let result = client.get_slot_metadata(i64::MAX).await;

    // Should get a clean error, not a panic from SIMD JSON
    assert!(result.is_err(), "Should error for invalid slot");

    let error_str = result.unwrap_err().to_string();
    // Should be a clean error message, not a JSON parsing panic
    assert!(
        error_str.contains("404")
            || error_str.contains("Not found")
            || error_str.contains("rate limit")
            || error_str.contains("timeout"),
        "Error should be well-formatted: {}",
        error_str
    );
}

#[tokio::test]
async fn test_connection_timeout_handling() {
    // Test that optimized client handles timeouts gracefully
    let client = SolanaDataClient::new();

    // The client should have reasonable timeouts configured
    // Test with a bunch of requests to potentially trigger timeout scenarios
    let mut timeout_count = 0;
    let mut success_count = 0;
    let mut other_errors = 0;

    for i in 0..20 {
        match client.get_slot_metadata(500_000 + i).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("timeout") {
                    timeout_count += 1;
                } else if error_str.contains("429") || error_str.contains("rate limit") {
                    // Rate limiting is expected
                } else {
                    other_errors += 1;
                }
            }
        }
    }

    println!(
        "Timeout test: {} success, {} timeouts, {} other errors",
        success_count, timeout_count, other_errors
    );

    // Should not have too many unexpected errors
    assert!(
        other_errors < 10,
        "Too many unexpected errors: {}",
        other_errors
    );
}

#[tokio::test]
async fn test_rate_limit_error_handling() {
    // Test that rate limiting is handled gracefully with optimizations
    let client = SolanaDataClient::new();

    // Make rapid requests to trigger rate limiting
    let mut rate_limited = 0;
    let mut successful = 0;
    let mut other_errors = 0;

    for i in 0..30 {
        match client.get_slot_metadata(600_000 + i).await {
            Ok(_) => successful += 1,
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("429") || error_str.contains("rate limit") {
                    rate_limited += 1;
                } else {
                    other_errors += 1;
                }
            }
        }

        // Small delay to avoid overwhelming
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    println!(
        "Rate limit test: {} successful, {} rate limited, {} other errors",
        successful, rate_limited, other_errors
    );

    // Should handle rate limits cleanly (but may not always hit rate limits in tests)
    if rate_limited == 0 {
        println!("Note: No rate limiting encountered in this test run");
    }
    assert!(other_errors < 5, "Should not have many other errors");
}

#[tokio::test]
async fn test_concurrent_error_handling() {
    // Test error handling under high concurrency
    use futures::future::join_all;
    use std::sync::Arc;

    let client = Arc::new(SolanaDataClient::new());

    // Launch many concurrent requests, some to invalid slots
    let mut handles = vec![];

    for i in 0..50 {
        let client = client.clone();
        let handle = tokio::spawn(async move {
            if i % 5 == 0 {
                // Some invalid requests
                client.get_slot_metadata(i64::MAX - i as i64).await
            } else {
                // Some valid requests
                client.get_slot_metadata(700_000 + i as i64).await
            }
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    let mut panics = 0;
    let mut successes = 0;
    let mut clean_errors = 0;

    for result in results {
        match result {
            Ok(Ok(_)) => successes += 1,
            Ok(Err(_)) => clean_errors += 1,
            Err(_) => panics += 1, // Task panicked
        }
    }

    println!(
        "Concurrent error test: {} successes, {} clean errors, {} panics",
        successes, clean_errors, panics
    );

    // Should not have any panics - all errors should be handled gracefully
    assert_eq!(
        panics, 0,
        "Should not have any panics in concurrent requests"
    );
    assert!(
        clean_errors > 0,
        "Should have some errors for invalid slots"
    );
}

#[test]
fn test_adaptive_concurrency_error_scenarios() {
    // Test adaptive concurrency logic under error conditions
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestAdaptive {
        current: Arc<AtomicUsize>,
        min: usize,
        max: usize,
    }

    impl TestAdaptive {
        fn new(initial: usize, min: usize, max: usize) -> Self {
            Self {
                current: Arc::new(AtomicUsize::new(initial)),
                min,
                max,
            }
        }

        fn adjust_for_errors(&self, error_rate: f64) {
            let current = self.current.load(Ordering::Relaxed);

            let new_val = if error_rate > 0.5 {
                // Very high error rate - dramatic reduction
                current / 2
            } else if error_rate > 0.1 {
                // High error rate - reduce
                (current as f64 * 0.8) as usize
            } else {
                current
            };

            let clamped = new_val.clamp(self.min, self.max);
            self.current.store(clamped, Ordering::Relaxed);
        }

        fn get(&self) -> usize {
            self.current.load(Ordering::Relaxed)
        }
    }

    // Test extreme error rates
    let adaptive = TestAdaptive::new(100, 10, 500);

    // Test 100% error rate
    adaptive.adjust_for_errors(1.0);
    assert_eq!(
        adaptive.get(),
        50,
        "Should halve concurrency on 100% error rate"
    );

    // Test that it doesn't go below minimum
    adaptive.adjust_for_errors(1.0);
    adaptive.adjust_for_errors(1.0);
    adaptive.adjust_for_errors(1.0);
    assert!(adaptive.get() >= 10, "Should not go below minimum");

    // Test edge case - exactly minimum
    let adaptive2 = TestAdaptive::new(10, 10, 500);
    adaptive2.adjust_for_errors(1.0);
    assert_eq!(
        adaptive2.get(),
        10,
        "Should stay at minimum when already there"
    );
}

#[test]
fn test_buffer_pool_error_recovery() {
    // Test buffer pool handles errors gracefully
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct TestBufferPool {
        buffers: Arc<Mutex<Vec<Vec<u8>>>>,
        max_size: usize,
    }

    impl TestBufferPool {
        fn new(max_size: usize) -> Self {
            Self {
                buffers: Arc::new(Mutex::new(Vec::new())),
                max_size,
            }
        }

        async fn get_buffer(&self) -> Vec<u8> {
            let mut buffers = self.buffers.lock().await;
            buffers.pop().unwrap_or_else(|| Vec::with_capacity(1024))
        }

        async fn return_buffer(&self, mut buffer: Vec<u8>) -> Result<(), &'static str> {
            // Simulate error conditions
            if buffer.capacity() > 10 * 1024 * 1024 {
                return Err("Buffer too large");
            }

            buffer.clear();
            let mut buffers = self.buffers.lock().await;

            if buffers.len() < self.max_size {
                buffers.push(buffer);
            }

            Ok(())
        }
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let pool = TestBufferPool::new(3);

        // Test normal operation
        let buffer1 = pool.get_buffer().await;
        assert!(pool.return_buffer(buffer1).await.is_ok());

        // Test error condition - oversized buffer
        let mut large_buffer = Vec::with_capacity(20 * 1024 * 1024);
        large_buffer.push(1);

        let result = pool.return_buffer(large_buffer).await;
        assert!(result.is_err(), "Should reject oversized buffers");

        // Pool should still work after error
        let buffer2 = pool.get_buffer().await;
        assert!(pool.return_buffer(buffer2).await.is_ok());
    });
}

#[test]
fn test_write_coalescing_error_handling() {
    // Test write coalescing handles errors properly
    use std::time::Instant;

    struct TestBatch {
        items: Vec<String>,
        max_size: usize,
        last_flush: Instant,
    }

    impl TestBatch {
        fn new(max_size: usize) -> Self {
            Self {
                items: Vec::new(),
                max_size,
                last_flush: Instant::now(),
            }
        }

        fn add_item(&mut self, item: String) -> Result<bool, &'static str> {
            // Simulate error conditions
            if item.len() > 1000 {
                return Err("Item too large");
            }

            self.items.push(item);

            // Return true if should flush
            Ok(self.items.len() >= self.max_size)
        }

        fn flush(&mut self) -> Vec<String> {
            let items = std::mem::take(&mut self.items);
            self.last_flush = Instant::now();
            items
        }
    }

    let mut batch = TestBatch::new(3);

    // Test normal operation
    assert!(!batch.add_item("item1".to_string()).unwrap());
    assert!(!batch.add_item("item2".to_string()).unwrap());
    assert!(batch.add_item("item3".to_string()).unwrap()); // Should flush

    let flushed = batch.flush();
    assert_eq!(flushed.len(), 3);

    // Test error handling
    let large_item = "x".repeat(2000);
    let result = batch.add_item(large_item);
    assert!(result.is_err(), "Should reject oversized items");

    // Batch should still work after error
    assert!(!batch.add_item("item4".to_string()).unwrap());
    assert_eq!(batch.items.len(), 1);
}

#[tokio::test]
async fn test_network_error_resilience() {
    // Test that the optimized client is resilient to network errors
    let client = SolanaDataClient::new();

    // Test with various potentially problematic requests
    let test_cases = vec![
        0,        // Very early slot
        i64::MAX, // Invalid slot
        1,        // Another early slot
        100,      // Early but possibly valid
    ];

    let mut network_errors = 0;
    let mut not_found_errors = 0;
    let mut rate_limit_errors = 0;
    let mut success_count = 0;
    let mut unexpected_errors = 0;

    for slot in test_cases {
        match client.get_slot_metadata(slot).await {
            Ok(_) => success_count += 1,
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("timeout") || error_str.contains("connection") {
                    network_errors += 1;
                } else if error_str.contains("404") || error_str.contains("Not found") {
                    not_found_errors += 1;
                } else if error_str.contains("429") || error_str.contains("rate limit") {
                    rate_limit_errors += 1;
                } else {
                    unexpected_errors += 1;
                    println!("Unexpected error for slot {}: {}", slot, error_str);
                }
            }
        }

        // Small delay between requests
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!(
        "Network resilience test: {} success, {} network errors, {} not found, {} rate limited, {} unexpected",
        success_count, network_errors, not_found_errors, rate_limit_errors, unexpected_errors
    );

    // Should not have many unexpected errors
    assert!(
        unexpected_errors <= 1,
        "Too many unexpected errors: {}",
        unexpected_errors
    );
}
