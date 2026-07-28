//! RocksDB Performance Metrics Collection
//!
//! This module provides focused metrics collection for RocksDB database
//! performance monitoring. It tracks low-level database operations,
//! latencies, and error rates.
//!
//! ## Metric Categories:
//! - **Database Operations**: Read/write operations and their latencies
//! - **Transaction Management**: Transaction commits and durations
//! - **Error Tracking**: Database-level error monitoring
//! - **Storage Performance**: Key-value operation metrics
//!
//! These metrics are recorded centrally by the typed KV layer
//! ([`crate::backend::kv::rocksdb`]) — every RocksDB get/put/delete/commit goes
//! through it — rather than scattered across the backend's methods.

use std::sync::OnceLock;

// OpenTelemetry imports for metrics collection
use opentelemetry::metrics::{Counter, Histogram};

/// Global RocksDB metrics collector instance
static ROCKSDB_METRICS: OnceLock<RocksDbPerformanceMetrics> = OnceLock::new();

/// RocksDB Performance Metrics Collector
///
/// Focuses exclusively on RocksDB database performance metrics,
/// following OpenTelemetry semantic conventions for database monitoring.
pub struct RocksDbPerformanceMetrics {
    // === Database Operations ===
    /// Total number of RocksDB get operations (key lookups)
    pub db_gets_total:    Counter<u64>,
    /// Total number of RocksDB put operations (key writes)
    pub db_puts_total:    Counter<u64>,
    /// Total number of RocksDB delete operations
    pub db_deletes_total: Counter<u64>,

    // === Performance Metrics ===
    /// Histogram of RocksDB get operation latencies in milliseconds
    pub db_get_duration_ms: Histogram<f64>,
    /// Histogram of RocksDB put operation latencies in milliseconds
    pub db_put_duration_ms: Histogram<f64>,

    // === Transaction Metrics ===
    /// Total number of RocksDB transaction commits
    pub db_transactions_total:      Counter<u64>,
    /// Histogram of RocksDB transaction durations in milliseconds
    pub db_transaction_duration_ms: Histogram<f64>,

    // === Error Metrics ===
    /// Total number of database errors by type
    pub db_errors_total: Counter<u64>,
}

impl RocksDbPerformanceMetrics {
    /// Initialize RocksDB performance metrics
    ///
    /// Creates all metric instruments focused on RocksDB database
    /// performance. Follows OpenTelemetry semantic conventions for
    /// database monitoring.
    ///
    /// ## Metric Naming Convention:
    /// - `kiseki_rocksdb_*` - RocksDB database performance metrics
    /// - `kiseki_rocksdb_transaction_*` - Transaction-related metrics
    /// - `kiseki_rocksdb_error_*` - Database error metrics
    fn new() -> Self {
        let meter = opentelemetry::global::meter("kiseki-rocksdb-performance");

        Self {
            // Database Operations
            db_gets_total: meter
                .u64_counter("kiseki_rocksdb_gets_total")
                .with_description("Total number of RocksDB get operations")
                .build(),

            db_puts_total: meter
                .u64_counter("kiseki_rocksdb_puts_total")
                .with_description("Total number of RocksDB put operations")
                .build(),

            db_deletes_total: meter
                .u64_counter("kiseki_rocksdb_deletes_total")
                .with_description("Total number of RocksDB delete operations")
                .build(),

            // Performance Metrics
            db_get_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_get_duration_ms")
                .with_description("RocksDB get operation latency in milliseconds")
                .build(),

            db_put_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_put_duration_ms")
                .with_description("RocksDB put operation latency in milliseconds")
                .build(),

            // Transaction Metrics
            db_transactions_total: meter
                .u64_counter("kiseki_rocksdb_transactions_total")
                .with_description("Total number of RocksDB transaction commits")
                .build(),

            db_transaction_duration_ms: meter
                .f64_histogram("kiseki_rocksdb_transaction_duration_ms")
                .with_description("RocksDB transaction duration in milliseconds")
                .build(),

            // Error Metrics
            db_errors_total: meter
                .u64_counter("kiseki_rocksdb_errors_total")
                .with_description("Total number of RocksDB database errors")
                .build(),
        }
    }
}

/// Get the global RocksDB performance metrics instance
pub fn get_rocksdb_metrics() -> &'static RocksDbPerformanceMetrics {
    ROCKSDB_METRICS.get_or_init(RocksDbPerformanceMetrics::new)
}

// === RocksDB Performance Metrics Macros ===

/// Macro for RocksDB operation counter increments
macro_rules! rocksdb_counter {
    (db_gets_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_gets_total
            .add(1, &[]);
    }};
    (db_puts_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_puts_total
            .add(1, &[]);
    }};
    (db_deletes_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_deletes_total
            .add(1, &[]);
    }};
    (db_transactions_total) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_transactions_total
            .add(1, &[]);
    }};
}

/// Macro for RocksDB histogram operations
macro_rules! rocksdb_histogram {
    (db_get_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_get_duration_ms
            .record($value, &[]);
    }};
    (db_put_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_put_duration_ms
            .record($value, &[]);
    }};
    (db_transaction_duration_ms, $value:expr) => {{
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_transaction_duration_ms
            .record($value, &[]);
    }};
}

/// Macro for RocksDB error recording with typed error constants
macro_rules! rocksdb_error {
    ($error_type:expr) => {{
        let labels = &[opentelemetry::KeyValue::new("error_type", $error_type)];
        crate::backend::rocksdb_metrics::get_rocksdb_metrics()
            .db_errors_total
            .add(1, labels);
    }};
}

/// Combined macro for RocksDB operations with timing and counter
macro_rules! rocksdb_timed_op {
    ($counter_name:ident, $histogram_name:ident, $operation:expr) => {{
        let start = std::time::Instant::now();
        let result = $operation;
        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        rocksdb_counter!($counter_name);
        rocksdb_histogram!($histogram_name, duration_ms);

        result
    }};
}

pub(crate) use rocksdb_counter;
pub(crate) use rocksdb_error;
pub(crate) use rocksdb_histogram;
pub(crate) use rocksdb_timed_op;
