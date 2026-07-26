// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{attribute, resource};
use sentry::ClientInitGuard;
use serde::{Deserialize, Serialize};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    EnvFilter, Registry, filter, fmt::Layer, layer::SubscriberExt, prelude::*,
};

use crate::sentry_init::init_sentry;

const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";
pub const DEFAULT_LOG_DIR: &str = "/tmp/kiseki.logs";
pub const DEFAULT_TOKIO_CONSOLE_ADDR: &str = "127.0.0.1:6669";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOptions {
    pub dir:                  String,
    pub level:                Option<String>,
    pub enable_otlp_tracing:  bool,
    pub otlp_endpoint:        Option<String>,
    pub tracing_sample_ratio: Option<f64>,
    pub append_stdout:        bool,
    pub tokio_console_addr:   Option<String>,
}

impl PartialEq for LoggingOptions {
    fn eq(&self, other: &Self) -> bool {
        self.dir == other.dir
            && self.level == other.level
            && self.enable_otlp_tracing == other.enable_otlp_tracing
            && self.otlp_endpoint == other.otlp_endpoint
            && self.tracing_sample_ratio == other.tracing_sample_ratio
            && self.append_stdout == other.append_stdout
            && self.tokio_console_addr == other.tokio_console_addr
    }
}

impl Eq for LoggingOptions {}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            dir:                  DEFAULT_LOG_DIR.to_string(),
            level:                None,
            enable_otlp_tracing:  false,
            otlp_endpoint:        None,
            tracing_sample_ratio: None,
            append_stdout:        true,
            tokio_console_addr:   Some(DEFAULT_TOKIO_CONSOLE_ADDR.to_string()),
        }
    }
}

impl LoggingOptions {
    pub fn with_dir(self, dir: String) -> Self { Self { dir, ..self } }

    pub fn with_enable_otlp_tracing(self, v: bool) -> Self {
        Self {
            enable_otlp_tracing: v,
            ..self
        }
    }
}

const DEFAULT_LOG_TARGETS: &str = "info";

#[allow(clippy::print_stdout)]
// The otlp `exporter` temporary is moved into the tracer provider builder a few
// lines later; keeping the intermediate binding reads far clearer than nesting
// its multi-line `.build().expect(...)` inside `.with_batch_exporter(...)`.
#[allow(clippy::significant_drop_tightening)]
pub fn init_global_logging(
    app_name: &str,
    opts: &LoggingOptions,
) -> (
    Vec<WorkerGuard>,
    Option<ClientInitGuard>,
    Option<SdkTracerProvider>,
) {
    let mut guards = vec![];
    let dir = &opts.dir;
    let level = &opts.level;
    let enable_otlp_tracing = opts.enable_otlp_tracing;

    let self_filter =
        filter::filter_fn(|metadata| metadata.target().starts_with(kiseki_common::KISEKI));

    let stdout_logging_layer = if opts.append_stdout {
        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        guards.push(stdout_guard);
        Some(
            Layer::new()
                .with_writer(stdout_writer)
                .with_file(true)
                .with_line_number(true)
                .with_target(true)
                .pretty()
                .with_filter(self_filter.clone()),
        )
    } else {
        None
    };

    // JSON log layer.
    let rolling_appender = RollingFileAppender::new(Rotation::HOURLY, dir, app_name);
    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let file_logging_layer = Layer::new()
        .with_writer(rolling_writer)
        .with_filter(self_filter.clone());
    guards.push(rolling_writer_guard);

    // error JSON log layer.
    let err_rolling_appender =
        RollingFileAppender::new(Rotation::HOURLY, dir, format!("{}-{}", app_name, "err"));
    let (err_rolling_writer, err_rolling_writer_guard) =
        tracing_appender::non_blocking(err_rolling_appender);
    let err_file_logging_layer = Layer::new()
        .with_writer(err_rolling_writer)
        .with_filter(self_filter.clone());
    guards.push(err_rolling_writer_guard);

    // resolve log level settings from:
    // - options from command line or config files
    // - environment variable: RUST_LOG
    // - default settings
    let rust_log_env = std::env::var(EnvFilter::DEFAULT_ENV).ok();
    let targets_string = level
        .as_deref()
        .or(rust_log_env.as_deref())
        .unwrap_or(DEFAULT_LOG_TARGETS);
    let layer_filter = targets_string
        .parse::<filter::Targets>()
        .expect("error parsing log level string");
    // let filter = Targets::new().with_target("kiseki", LevelFilter::DEBUG);
    let sampler = opts
        .tracing_sample_ratio
        .map_or(Sampler::AlwaysOn, Sampler::TraceIdRatioBased);

    let (sentry_layer, sentry_guard) = match init_sentry() {
        None => (None, None),
        Some(sentry_guard) => (
            Some(sentry_tracing::layer().with_filter(self_filter.clone())),
            Some(sentry_guard),
        ),
    };

    // Must enable 'tokio_unstable' cfg to use this feature.
    // For example: `RUSTFLAGS="--cfg tokio_unstable" cargo run -F
    // common-telemetry/console -- standalone start`
    #[cfg(feature = "tokio-console")]
    let subscriber = {
        let tokio_console_layer = if let Some(tokio_console_addr) = &opts.tokio_console_addr {
            let addr: std::net::SocketAddr = tokio_console_addr.parse().unwrap_or_else(|e| {
                panic!("Invalid binding address '{tokio_console_addr}' for tokio-console: {e}");
            });
            println!("tokio-console listening on {addr}");

            Some(
                console_subscriber::ConsoleLayer::builder()
                    .server_addr(addr)
                    .spawn(),
            )
        } else {
            None
        };

        Registry::default()
            .with(tokio_console_layer)
            .with(stdout_logging_layer.map(|x| x.with_filter(layer_filter.clone())))
            .with(file_logging_layer.with_filter(layer_filter))
            .with(err_file_logging_layer.with_filter(filter::LevelFilter::ERROR))
            .with(sentry_layer)
    };

    #[cfg(not(feature = "tokio-console"))]
    let subscriber = Registry::default()
        .with(stdout_logging_layer.map(|x| x.with_filter(layer_filter.clone())))
        .with(file_logging_layer.with_filter(layer_filter))
        .with(err_file_logging_layer.with_filter(filter::LevelFilter::ERROR))
        .with(sentry_layer);

    let tracer_provider = if enable_otlp_tracing {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let endpoint = opts.otlp_endpoint.as_ref().map_or_else(
            || DEFAULT_OTLP_ENDPOINT.to_string(),
            |e| format!("http://{e}"),
        );
        println!("find otlp tracing config: {endpoint}");
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("otlp tracer install failed");
        let resource = opentelemetry_sdk::Resource::builder_empty()
            .with_attributes([
                KeyValue::new(resource::SERVICE_NAME, app_name.to_string()),
                KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                KeyValue::new(attribute::PROCESS_PID, std::process::id().to_string()),
            ])
            .build();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(sampler)
            .with_resource(resource)
            .build();
        let tracer = provider.tracer(app_name.to_string());
        global::set_tracer_provider(provider.clone());
        let tracing_layer = Some(tracing_opentelemetry::layer().with_tracer(tracer));
        let subscriber = subscriber.with(tracing_layer);
        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
        Some(provider)
    } else {
        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
        None
    };

    (guards, sentry_guard, tracer_provider)
}

pub struct LoggingGuard {
    worker_guards:   Vec<WorkerGuard>,
    sentry_guard:    Option<ClientInitGuard>,
    tracer_provider: Option<SdkTracerProvider>,
    runtime:         Option<tokio::runtime::Runtime>,
}

impl LoggingGuard {
    pub fn shutdown(mut self, deadline: std::time::Duration) { self.shutdown_inner(deadline); }

    fn shutdown_inner(&mut self, deadline: std::time::Duration) {
        if self.tracer_provider.is_none()
            && self.runtime.is_none()
            && self.worker_guards.is_empty()
            && self.sentry_guard.is_none()
        {
            return;
        }

        let provider = self.tracer_provider.take();
        let runtime = self.runtime.take();
        let worker_guards = std::mem::take(&mut self.worker_guards);
        let sentry_guard = self.sentry_guard.take();
        let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("kiseki-telemetry-shutdown".to_string())
            .spawn(move || {
                let provider_error = provider
                    .and_then(|provider| provider.shutdown().err())
                    .map(|error| error.to_string());
                if let Some(runtime) = runtime {
                    runtime.shutdown_timeout(deadline);
                }
                drop(worker_guards);
                drop(sentry_guard);
                let _ = completed_tx.send(provider_error);
            });

        let Ok(_worker) = worker else {
            tracing::warn!("failed to start bounded telemetry shutdown worker");
            return;
        };
        match completed_rx.recv_timeout(deadline) {
            Ok(Some(error)) => {
                tracing::warn!(%error, "failed to shut down OTLP tracer provider");
            }
            Ok(None) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                tracing::warn!(?deadline, "telemetry shutdown exceeded its deadline");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                tracing::warn!("telemetry shutdown worker exited without reporting completion");
            }
        }
    }
}

impl Drop for LoggingGuard {
    fn drop(&mut self) { self.shutdown_inner(std::time::Duration::from_secs(1)) }
}

pub fn init_global_logging_without_runtime(app_name: &str, opts: &LoggingOptions) -> LoggingGuard {
    let runtime = opts.enable_otlp_tracing.then(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("otlp runtime thread")
            .worker_threads(1)
            .build()
            .expect("failed to build OTLP runtime")
    });
    let runtime_guard = runtime.as_ref().map(tokio::runtime::Runtime::enter);
    let (worker_guards, sentry_guard, tracer_provider) = init_global_logging(app_name, opts);
    drop(runtime_guard);
    LoggingGuard {
        worker_guards,
        sentry_guard,
        tracer_provider,
        runtime,
    }
}

#[allow(dead_code)]
pub fn install_fmt_log() {
    // Tests install logging from multiple threads. `try_init` lets the first
    // caller win without turning subsequent initialization attempts into test
    // failures.
    let _ = tracing_subscriber::fmt().pretty().try_init();
}
