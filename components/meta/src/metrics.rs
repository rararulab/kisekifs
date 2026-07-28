// Copyright 2024 kisekifs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Kiseki Meta Storage Metrics Collection
//!
//! This module provides comprehensive OpenTelemetry metrics collection for the
//! kiseki filesystem metadata layer. It includes both performance metrics and
//! business logic metrics for monitoring and observability.
//!
//! ## Architecture
//!
//! The metrics system is organized into several components:
//! - **RocksDB Performance Metrics**: Database-level performance indicators
//! - **Business Logic Metrics**: Filesystem operation metrics
//! - **Error Tracking**: Comprehensive error monitoring
use std::{sync::OnceLock, time::Instant};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use tracing::debug;

/// Business Logic Metrics for Filesystem Operations
///
/// These metrics track high-level filesystem operations and business logic,
/// providing insights into user behavior and system usage patterns.
pub struct BusinessMetrics {
    // === Filesystem Operations ===
    /// Total number of inodes created by type (files, directories, symlinks)
    pub inodes_created_total: Counter<u64>,
    /// Total number of inodes deleted by type
    pub inodes_deleted_total: Counter<u64>,
    /// Total number of directory operations (mkdir, rmdir, rename)
    pub directory_ops_total:  Counter<u64>,
    /// Total number of file operations (create, unlink, truncate, read, write)
    pub file_ops_total:       Counter<u64>,
    /// Total number of symbolic link operations
    pub symlink_ops_total:    Counter<u64>,
    /// Total number of hard link operations
    pub hardlink_ops_total:   Counter<u64>,

    // === Performance Metrics ===
    /// Histogram of filesystem operation durations in milliseconds
    pub fs_operation_duration_ms: Histogram<f64>,
    /// Total number of lookup operations (path resolution)
    pub lookups_total:            Counter<u64>,
    /// Histogram of lookup operation durations
    pub lookup_duration_ms:       Histogram<f64>,

    // === Cache and Resource Metrics ===
    /// Cache hit rate for file attributes
    pub attr_cache_hits_total:   Counter<u64>,
    /// Cache miss rate for file attributes
    pub attr_cache_misses_total: Counter<u64>,
    /// Number of currently open files
    pub open_files_total:        Counter<u64>,
    /// Total filesystem space used in bytes
    pub filesystem_used_bytes:   Counter<u64>,
    /// Total number of files in filesystem
    pub filesystem_file_count:   Counter<u64>,

    // === Error Tracking ===
    /// Total number of operation errors by type and operation
    pub operation_errors_total: Counter<u64>,
}

impl BusinessMetrics {
    /// Create new business metrics collector
    fn new() -> Self {
        let meter = opentelemetry::global::meter("kiseki-meta-business");

        Self {
            // Filesystem Operations
            inodes_created_total: meter
                .u64_counter("kiseki_meta_inodes_created_total")
                .with_description("Total number of inodes created by type")
                .build(),

            inodes_deleted_total: meter
                .u64_counter("kiseki_meta_inodes_deleted_total")
                .with_description("Total number of inodes deleted by type")
                .build(),

            directory_ops_total: meter
                .u64_counter("kiseki_meta_directory_ops_total")
                .with_description("Total number of directory operations")
                .build(),

            file_ops_total: meter
                .u64_counter("kiseki_meta_file_ops_total")
                .with_description("Total number of file operations")
                .build(),

            symlink_ops_total: meter
                .u64_counter("kiseki_meta_symlink_ops_total")
                .with_description("Total number of symbolic link operations")
                .build(),

            hardlink_ops_total: meter
                .u64_counter("kiseki_meta_hardlink_ops_total")
                .with_description("Total number of hard link operations")
                .build(),

            // Performance Metrics
            fs_operation_duration_ms: meter
                .f64_histogram("kiseki_meta_fs_operation_duration_ms")
                .with_description("Filesystem operation duration in milliseconds")
                .build(),

            lookups_total: meter
                .u64_counter("kiseki_meta_lookups_total")
                .with_description("Total number of path lookup operations")
                .build(),

            lookup_duration_ms: meter
                .f64_histogram("kiseki_meta_lookup_duration_ms")
                .with_description("Path lookup operation duration in milliseconds")
                .build(),

            // Cache and Resource Metrics
            attr_cache_hits_total: meter
                .u64_counter("kiseki_meta_attr_cache_hits_total")
                .with_description("Total number of attribute cache hits")
                .build(),

            attr_cache_misses_total: meter
                .u64_counter("kiseki_meta_attr_cache_misses_total")
                .with_description("Total number of attribute cache misses")
                .build(),

            open_files_total: meter
                .u64_counter("kiseki_meta_open_files_total")
                .with_description("Number of currently open files")
                .build(),

            filesystem_used_bytes: meter
                .u64_counter("kiseki_meta_filesystem_used_bytes")
                .with_description("Total filesystem space used in bytes")
                .build(),

            filesystem_file_count: meter
                .u64_counter("kiseki_meta_filesystem_file_count")
                .with_description("Total number of files in filesystem")
                .build(),

            // Error Tracking
            operation_errors_total: meter
                .u64_counter("kiseki_meta_operation_errors_total")
                .with_description("Total number of operation errors by type")
                .build(),
        }
    }
}

/// Global business metrics instance
static BUSINESS_METRICS: OnceLock<BusinessMetrics> = OnceLock::new();

/// Get the global business metrics instance
pub fn get_business_metrics() -> &'static BusinessMetrics {
    BUSINESS_METRICS.get_or_init(BusinessMetrics::new)
}

/// Record filesystem operation with timing and labels
///
/// This is a high-level helper function for recording business logic operations
/// with automatic timing and proper labeling.
pub fn record_fs_operation_with_timing<F, T>(operation_type: &str, operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0; // Convert to milliseconds

    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("operation", operation_type.to_string())];
    metrics.fs_operation_duration_ms.record(duration, labels);

    result
}

/// Record lookup operation with timing
pub fn record_lookup_operation<F, T>(operation: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = operation();
    let duration = start.elapsed().as_secs_f64() * 1000.0;

    let metrics = get_business_metrics();
    metrics.lookups_total.add(1, &[]);
    metrics.lookup_duration_ms.record(duration, &[]);

    result
}

/// Record inode creation by type
pub fn record_inode_created(inode_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("type", inode_type.to_string())];
    metrics.inodes_created_total.add(1, labels);
}

/// Record inode deletion by type
pub fn record_inode_deleted(inode_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("type", inode_type.to_string())];
    metrics.inodes_deleted_total.add(1, labels);
}

/// Record directory operation
pub fn record_directory_operation(operation_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("operation", operation_type.to_string())];
    metrics.directory_ops_total.add(1, labels);
}

/// Record file operation
pub fn record_file_operation(operation_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("operation", operation_type.to_string())];
    metrics.file_ops_total.add(1, labels);
}

/// Record symbolic link operation
pub fn record_symlink_operation(operation_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("operation", operation_type.to_string())];
    metrics.symlink_ops_total.add(1, labels);
}

/// Record hard link operation
pub fn record_hardlink_operation(operation_type: &str) {
    let metrics = get_business_metrics();
    let labels = &[KeyValue::new("operation", operation_type.to_string())];
    metrics.hardlink_ops_total.add(1, labels);
}

/// Record cache hit
pub fn record_cache_hit() {
    let metrics = get_business_metrics();
    metrics.attr_cache_hits_total.add(1, &[]);
}

/// Record cache miss
pub fn record_cache_miss() {
    let metrics = get_business_metrics();
    metrics.attr_cache_misses_total.add(1, &[]);
}

/// Record filesystem size change
pub fn record_filesystem_size_change(delta_bytes: i64) {
    let metrics = get_business_metrics();
    if delta_bytes >= 0 {
        metrics.filesystem_used_bytes.add(delta_bytes as u64, &[]);
    } else {
        // For negative values, we should use a different approach
        // For now, we'll just record as positive change
        debug!("Filesystem size decreased by {} bytes", delta_bytes.abs());
    }
}

/// Record filesystem file count change
pub fn record_filesystem_file_count_change(delta_count: i64) {
    let metrics = get_business_metrics();
    if delta_count >= 0 {
        metrics.filesystem_file_count.add(delta_count as u64, &[]);
    } else {
        debug!(
            "Filesystem file count decreased by {} files",
            delta_count.abs()
        );
    }
}

/// Record operation error
pub fn record_operation_error(error_type: &str, operation: &str) {
    let metrics = get_business_metrics();
    let labels = &[
        KeyValue::new(labels::ERROR_TYPE.to_string(), error_type.to_string()),
        KeyValue::new(labels::OPERATION.to_string(), operation.to_string()),
    ];
    metrics.operation_errors_total.add(1, labels);
}

/// Example usage of metrics macros with constants
///
/// ```ignore
/// use kiseki_meta::{metrics_counter, metrics_cache, metrics_business_op};
/// use kiseki_meta::metrics::labels;
///
/// // Simple counter increment
/// metrics_counter!(lookups_total);
///
/// // Cache operations
/// metrics_cache!(HIT);
/// metrics_cache!(MISS);
///
/// // Business operations with type safety
/// metrics_business_op!(MKDIR, labels::TYPE_DIRECTORY);
/// metrics_business_op!(CREATE, labels::TYPE_REGULAR_FILE);
/// metrics_business_op!(SYMLINK_CREATE);
///
/// // Counter with custom labels
/// metrics_counter_with_labels!(inodes_created_total,
///     labels::TYPE => labels::TYPE_SYMLINK);
///
/// // Histogram with timing
/// let _timer = metrics_start_timer!(lookup_duration_ms);
/// // ... do work
/// // Timer automatically records when dropped
/// ```
/// Constants for metrics labels and operations
///
/// These constants prevent typos and provide consistency across metrics
/// collection.
pub mod labels {
    // Label keys
    pub const OPERATION: &str = "operation";
    pub const TYPE: &str = "type";
    pub const ERROR_TYPE: &str = "error_type";

    // File system operations
    pub const OP_MKDIR: &str = "mkdir";
    pub const OP_RMDIR: &str = "rmdir";
    pub const OP_CREATE: &str = "create";
    pub const OP_UNLINK: &str = "unlink";
    pub const OP_RENAME: &str = "rename";
    pub const OP_LOOKUP: &str = "lookup";
    pub const OP_TRUNCATE: &str = "truncate";
    pub const OP_READ: &str = "read";
    pub const OP_WRITE: &str = "write";

    // Inode types
    pub const TYPE_REGULAR_FILE: &str = "regular_file";
    pub const TYPE_DIRECTORY: &str = "directory";
    pub const TYPE_SYMLINK: &str = "symlink";
    pub const TYPE_OTHER: &str = "other";

    // Link operations
    pub const OP_HARDLINK_CREATE: &str = "create";
    pub const OP_HARDLINK_REMOVE: &str = "remove";
    pub const OP_SYMLINK_CREATE: &str = "create";
    pub const OP_SYMLINK_READ: &str = "read";

    // Cache operations
    pub const CACHE_HIT: &str = "hit";
    pub const CACHE_MISS: &str = "miss";

    // RocksDB error types
    pub const ERROR_ROCKSDB: &str = "rocksdb_error";
    pub const ERROR_SERIALIZATION: &str = "serialization_error";
    pub const ERROR_CORRUPTION: &str = "corruption_error";
    pub const ERROR_WRITE_BATCH: &str = "write_batch_error";
    pub const ERROR_TRANSACTION_COMMIT: &str = "transaction_commit_error";
}

/// Macros for simplified metrics recording
///
/// These macros provide a lightweight way to record metrics without
/// wrapping business logic or requiring complex setup code.
/// Record a simple counter increment
///
/// Usage: `metrics_counter!(lookups_total)`
#[macro_export]
macro_rules! metrics_counter {
    ($counter:ident) => {{
        let metrics = $crate::metrics::get_business_metrics();
        metrics.$counter.add(1, &[]);
    }};
}

/// Record a counter increment with labels
///
/// Usage: `metrics_counter_with_labels!(inodes_created_total, "type" =>
/// "directory")`
#[macro_export]
macro_rules! metrics_counter_with_labels {
    ($counter:ident, $($key:expr => $value:expr),*) => {{
        let metrics = $crate::metrics::get_business_metrics();
        let labels = &[$(opentelemetry::KeyValue::new($key.to_string(), $value.to_string())),*];
        metrics.$counter.add(1, labels);
    }};
}

/// Record a histogram value
///
/// Usage: `metrics_histogram!(lookup_duration_ms, duration_ms)`
#[macro_export]
macro_rules! metrics_histogram {
    ($histogram:ident, $value:expr) => {{
        let metrics = $crate::metrics::get_business_metrics();
        metrics.$histogram.record($value, &[]);
    }};
}

/// Record a histogram value with labels
///
/// Usage: `metrics_histogram_with_labels!(fs_operation_duration_ms, duration,
/// "operation" => "lookup")`
#[macro_export]
macro_rules! metrics_histogram_with_labels {
    ($histogram:ident, $value:expr, $($key:expr => $label:expr),*) => {{
        let metrics = $crate::metrics::get_business_metrics();
        let labels = &[$(opentelemetry::KeyValue::new($key.to_string(), $label.to_string())),*];
        metrics.$histogram.record($value, labels);
    }};
}

/// Start timing an operation and return a guard that automatically records when
/// dropped
///
/// Usage:
/// ```ignore
/// let _timer = metrics_start_timer!(lookup_duration_ms);
/// // ... do operation
/// // Timer automatically records duration when _timer goes out of scope
/// ```
#[macro_export]
macro_rules! metrics_start_timer {
    ($histogram:ident) => {{ $crate::metrics::TimerGuard::new(stringify!($histogram)) }};
}

/// Timer guard for automatic duration recording
pub struct TimerGuard {
    start:          std::time::Instant,
    histogram_name: &'static str,
}

impl TimerGuard {
    pub fn new(histogram_name: &'static str) -> Self {
        Self {
            start: std::time::Instant::now(),
            histogram_name,
        }
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64() * 1000.0; // Convert to milliseconds
        let metrics = get_business_metrics();

        match self.histogram_name {
            "lookup_duration_ms" => metrics.lookup_duration_ms.record(duration, &[]),
            "fs_operation_duration_ms" => metrics.fs_operation_duration_ms.record(duration, &[]),
            _ => {
                debug!("Unknown histogram: {}", self.histogram_name);
            }
        }
    }
}

/// Convenient macro for common business operations
///
/// Usage: `metrics_business_op!(MKDIR, TYPE_DIRECTORY)`
#[macro_export]
macro_rules! metrics_business_op {
    (MKDIR, $inode_type:expr) => {{
        $crate::metrics_counter_with_labels!(directory_ops_total,
            $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_MKDIR);
        $crate::metrics_counter_with_labels!(inodes_created_total,
            $crate::metrics::labels::TYPE => $inode_type);
    }};
    (RMDIR, $inode_type:expr) => {{
        $crate::metrics_counter_with_labels!(directory_ops_total,
            $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_RMDIR);
        $crate::metrics_counter_with_labels!(inodes_deleted_total,
            $crate::metrics::labels::TYPE => $inode_type);
    }};
    (CREATE, $inode_type:expr) => {{
        $crate::metrics_counter_with_labels!(file_ops_total,
            $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_CREATE);
        $crate::metrics_counter_with_labels!(inodes_created_total,
            $crate::metrics::labels::TYPE => $inode_type);
    }};
    (UNLINK, $inode_type:expr) => {{
        $crate::metrics_counter_with_labels!(file_ops_total,
            $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_UNLINK);
        $crate::metrics_counter_with_labels!(inodes_deleted_total,
            $crate::metrics::labels::TYPE => $inode_type);
    }};
    (SYMLINK_CREATE) => {{
        $crate::metrics_counter_with_labels!(symlink_ops_total,
            $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_SYMLINK_CREATE);
        $crate::metrics_counter_with_labels!(inodes_created_total,
            $crate::metrics::labels::TYPE => $crate::metrics::labels::TYPE_SYMLINK);
    }};
    (HARDLINK, $op:expr) => {{
        $crate::metrics_counter_with_labels!(hardlink_ops_total,
            $crate::metrics::labels::OPERATION => $op);
    }};
    (RENAME, $inode_type:expr) => {{
        match $inode_type {
            $crate::metrics::labels::TYPE_DIRECTORY => {
                $crate::metrics_counter_with_labels!(directory_ops_total,
                    $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_RENAME)
            },
            $crate::metrics::labels::TYPE_REGULAR_FILE => {
                $crate::metrics_counter_with_labels!(file_ops_total,
                    $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_RENAME)
            },
            $crate::metrics::labels::TYPE_SYMLINK => {
                $crate::metrics_counter_with_labels!(symlink_ops_total,
                    $crate::metrics::labels::OPERATION => $crate::metrics::labels::OP_RENAME)
            },
            _ => {}
        }
    }};
}

/// Macro for recording cache operations
///
/// Usage: `metrics_cache!(HIT)` or `metrics_cache!(MISS)`
#[macro_export]
macro_rules! metrics_cache {
    (HIT) => {{
        $crate::metrics_counter!(attr_cache_hits_total);
    }};
    (MISS) => {{
        $crate::metrics_counter!(attr_cache_misses_total);
    }};
}

/// Macro for recording filesystem size changes
///
/// Usage: `metrics_fs_size_change!(1024)` for increase or
/// `metrics_fs_size_change!(-1024)` for decrease
#[macro_export]
macro_rules! metrics_fs_size_change {
    ($delta:expr) => {{
        let delta_value = $delta;
        if delta_value >= 0 {
            let metrics = $crate::metrics::get_business_metrics();
            metrics.filesystem_used_bytes.add(delta_value as u64, &[]);
        } else {
            // For negative values, we log but don't record negative metrics
            tracing::debug!("Filesystem size decreased by {} bytes", (-delta_value));
        }
    }};
}

/// Macro for recording filesystem file count changes
///
/// Usage: `metrics_fs_file_count_change!(1)` for increase or
/// `metrics_fs_file_count_change!(-1)` for decrease
#[macro_export]
macro_rules! metrics_fs_file_count_change {
    ($delta:expr) => {{
        let delta_value = $delta;
        if delta_value >= 0 {
            let metrics = $crate::metrics::get_business_metrics();
            metrics.filesystem_file_count.add(delta_value as u64, &[]);
        } else {
            tracing::debug!(
                "Filesystem file count decreased by {} files",
                (-delta_value)
            );
        }
    }};
}
