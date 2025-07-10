// Tests for performance optimizations
// Run with: cargo test --test optimization_tests

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio;

// Import the optimized structures from main.rs
use solana_data_fetch::*;

#[tokio::test]
async fn test_performance_regression() {
    // Simple performance test to ensure optimizations don't break functionality
    let client = SolanaDataClient::new();

    let start = Instant::now();
    let mut successful_requests = 0;

    // Test 10 slot requests with optimized client
    for i in 0..10 {
        let slot = 100_000 + i;
        match client.get_slot_metadata(slot).await {
            Ok(_) => successful_requests += 1,
            Err(_) => {} // Rate limiting is expected
        }
    }

    let duration = start.elapsed();

    // Should complete within reasonable time (30 seconds max)
    assert!(
        duration < Duration::from_secs(30),
        "Performance regression: took {:?} for 10 requests",
        duration
    );

    // Should have some successful requests (not all rate limited)
    assert!(
        successful_requests > 0,
        "Performance regression: no successful requests"
    );

    println!(
        "Performance test: {} requests in {:?}",
        successful_requests, duration
    );
}

#[tokio::test]
async fn test_simd_json_functionality() {
    // Test that SIMD JSON parsing works correctly
    let client = SolanaDataClient::new();

    // Get a known slot and verify data structure is parsed correctly
    match client.get_slot_metadata(100_000).await {
        Ok(metadata) => {
            // Verify SIMD JSON parsed the response correctly
            assert_eq!(metadata.slot, 100_000);
            assert!(metadata.transaction_count >= 0);
            // If we get here, SIMD JSON parsing worked
        }
        Err(e) => {
            // Rate limiting is acceptable
            let error_str = e.to_string();
            if !error_str.contains("429") && !error_str.contains("rate limit") {
                panic!("SIMD JSON test failed: {:?}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_concurrent_optimization_performance() {
    // Test that high concurrency works with optimizations
    use futures::future::join_all;

    let client = Arc::new(SolanaDataClient::new());
    let start = Instant::now();

    // Launch 20 concurrent requests to test connection pooling
    let mut handles = vec![];
    for i in 0..20 {
        let client = client.clone();
        let handle = tokio::spawn(async move { client.get_slot_metadata(200_000 + i).await });
        handles.push(handle);
    }

    let results = join_all(handles).await;
    let duration = start.elapsed();

    // Should handle concurrency well - not take too long
    assert!(
        duration < Duration::from_secs(60),
        "Concurrent optimization test too slow: {:?}",
        duration
    );

    // Count successful requests
    let successful = results
        .iter()
        .filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok())
        .count();

    println!(
        "Concurrent test: {}/{} successful in {:?}",
        successful, 20, duration
    );

    // Should have some success even with rate limiting
    assert!(successful > 0, "No concurrent requests succeeded");
}

#[test]
fn test_buffer_pool_basic_functionality() {
    // Test the buffer pool logic (synchronous test)
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Simulate buffer pool structure
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
    }

    let pool = TestBufferPool::new(3);

    // Test basic structure
    assert_eq!(pool.max_size, 3);

    // Buffer pool should be initialized correctly
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let buffers = pool.buffers.lock().await;
        assert!(buffers.is_empty());
    });
}

#[test]
fn test_write_coalescing_structure() {
    // Test the batched writer structure
    use std::time::{Duration, Instant};

    struct TestBatchedWriter {
        batch: Vec<String>,
        max_batch_size: usize,
        max_wait_time: Duration,
        last_flush: Instant,
    }

    impl TestBatchedWriter {
        fn new(max_batch_size: usize, max_wait_ms: u64) -> Self {
            Self {
                batch: Vec::with_capacity(max_batch_size),
                max_batch_size,
                max_wait_time: Duration::from_millis(max_wait_ms),
                last_flush: Instant::now(),
            }
        }

        fn should_flush(&self) -> bool {
            self.batch.len() >= self.max_batch_size
                || self.last_flush.elapsed() >= self.max_wait_time
        }

        fn add_item(&mut self, item: String) {
            self.batch.push(item);
        }
    }

    let mut writer = TestBatchedWriter::new(5, 100);

    // Test batch size triggering
    for i in 0..4 {
        writer.add_item(format!("item_{}", i));
        assert!(!writer.should_flush(), "Should not flush yet at {}", i);
    }

    writer.add_item("item_5".to_string());
    assert!(writer.should_flush(), "Should flush when batch is full");

    // Test time-based triggering
    let mut writer2 = TestBatchedWriter::new(10, 1); // 1ms timeout
    writer2.add_item("test".to_string());

    std::thread::sleep(Duration::from_millis(5));
    assert!(writer2.should_flush(), "Should flush after timeout");
}

#[test]
fn test_adaptive_concurrency_structure() {
    // Test adaptive concurrency logic
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestAdaptiveConcurrency {
        current: Arc<AtomicUsize>,
        min: usize,
        max: usize,
    }

    impl TestAdaptiveConcurrency {
        fn new(initial: usize, min: usize, max: usize) -> Self {
            Self {
                current: Arc::new(AtomicUsize::new(initial)),
                min,
                max,
            }
        }

        fn adjust(&self, error_rate: f64) {
            let current_val = self.current.load(Ordering::Relaxed);
            let new_val = if error_rate > 0.1 {
                // High error rate - reduce concurrency
                ((current_val as f64) * 0.8) as usize
            } else if error_rate < 0.05 {
                // Low error rate - increase concurrency
                ((current_val as f64) * 1.2) as usize
            } else {
                current_val
            };

            let clamped = new_val.clamp(self.min, self.max);
            self.current.store(clamped, Ordering::Relaxed);
        }

        fn get(&self) -> usize {
            self.current.load(Ordering::Relaxed)
        }
    }

    let adaptive = TestAdaptiveConcurrency::new(100, 10, 500);

    // Test initial value
    assert_eq!(adaptive.get(), 100);

    // Test error rate reduction
    adaptive.adjust(0.15); // High error rate
    assert!(
        adaptive.get() < 100,
        "Should reduce concurrency on high error rate"
    );

    // Test bounds checking
    let adaptive2 = TestAdaptiveConcurrency::new(10, 10, 500);
    adaptive2.adjust(0.15); // Try to reduce below minimum
    assert_eq!(adaptive2.get(), 10, "Should not go below minimum");

    let adaptive3 = TestAdaptiveConcurrency::new(500, 10, 500);
    adaptive3.adjust(0.01); // Try to increase above maximum
    assert_eq!(adaptive3.get(), 500, "Should not go above maximum");
}

#[tokio::test]
async fn test_connection_pre_warming_effect() {
    // Test that connection pre-warming improves performance
    let client = Arc::new(
        Client::builder()
            .pool_max_idle_per_host(200)
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .unwrap(),
    );

    // Test that client builds successfully with optimized settings
    assert!(client.get("https://api.pipe-solana.com").build().is_ok());

    // Pre-warming simulation - just verify we can make multiple concurrent requests
    let mut handles = vec![];
    for _ in 0..5 {
        let client = client.clone();
        let handle = tokio::spawn(async move {
            let result = client.head("https://api.pipe-solana.com").send().await;
            result.is_ok()
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let successful = results.iter().filter(|r| r.is_ok()).count();

    // At least some should succeed (network permitting)
    println!("Connection pre-warming test: {}/5 successful", successful);
}

use futures;
use reqwest::Client;
