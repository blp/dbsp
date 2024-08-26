use crate::{
    catalog::RecordFormat,
    controller::ConnectorConfig,
    ensure_default_crypto_provider,
    transport::http::{
        HttpInputEndpoint, HttpInputTransport, HttpOutputEndpoint, HttpOutputTransport,
    },
    CircuitCatalog, Controller, ControllerError, DbspCircuitHandle, FormatConfig,
    InputEndpointConfig, InputFormat, OutputEndpoint, OutputEndpointConfig, OutputFormat,
    PipelineConfig, TransportInputEndpoint,
};
use actix_web::body::MessageBody;
use actix_web::dev::Service;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::header,
    post, rt,
    web::{self, Data as WebData, Json, Payload, Query},
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
};
use arrow::array::RecordBatch;
use arrow_json::LineDelimitedWriter;
use clap::Parser;
use colored::Colorize;
use datafusion::error::DataFusionError;
use dbsp::circuit::CircuitConfig;
use dbsp::operator::sample::MAX_QUANTILES;
use env_logger::Env;
use feldera_types::program_schema::SqlIdentifier;
use feldera_types::{
    query::{AdHocResultFormat, AdhocQueryArgs, OutputQuery},
    transport::http::SERVER_PORT_FILE,
};
use futures_util::FutureExt;
use log::{debug, error, info, log, trace, warn, Level};
use minitrace::collector::Config;
use feldera_types::{
    config::default_max_batch_size, format::json::JsonFlavor, transport::http::EgressMode,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::io::Write;
use std::path::PathBuf;
use std::{
    borrow::Cow,
    net::TcpListener,
    sync::{
        mpsc::{self, Sender as StdSender},
        Arc, Mutex, RwLock, Weak,
    },
    thread,
};
use tokio::{
    spawn,
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
};
use uuid::Uuid;

pub mod error;
mod prometheus;

pub use self::error::{ErrorResponse, PipelineError, MAX_REPORTED_PARSE_ERRORS};
use self::prometheus::PrometheusMetrics;

/// By default actix will start the number of threads equal to the number of
/// cores, which is an overkill and can lead to file descriptor exhaustion when
/// running multiple pipelines on systems with >100 cores.  We use a fixed
/// number of threads instead.  Four should be more than enough, but we can make
/// it configurable if needed.
static NUM_HTTP_WORKERS: usize = 4;

/// Tracks the health of the pipeline.
///
/// Enables the server to report the state of the pipeline while it is
/// initializing, when it has failed to initialize, or failed.
enum PipelinePhase {
    /// Initialization in progress.
    Initializing,

    /// Initialization has failed.
    InitializationError(Arc<ControllerError>),

    /// Initialization completed successfully.  Current state of the
    /// pipeline can be read using `controller.status()`.
    InitializationComplete,

    /// Pipeline encountered a fatal error.
    Failed(Arc<ControllerError>),
}

/// Generate an appropriate error when `state.controller` is set to
/// `None`, which can mean that the pipeline is initializing, failed to
/// initialize, has been shut down or failed.
fn missing_controller_error(state: &ServerState) -> PipelineError {
    match &*state.phase.read().unwrap() {
        PipelinePhase::Initializing => PipelineError::Initializing,
        PipelinePhase::InitializationError(e) => {
            PipelineError::InitializationError { error: e.clone() }
        }
        PipelinePhase::InitializationComplete => PipelineError::Terminating,
        PipelinePhase::Failed(e) => PipelineError::ControllerError { error: e.clone() },
    }
}

struct ServerState {
    phase: RwLock<PipelinePhase>,
    metadata: RwLock<String>,
    controller: Mutex<Option<Controller>>,
    prometheus: RwLock<Option<PrometheusMetrics>>,
    /// Channel used to send a `kill` command to
    /// the self-destruct task when shutting down
    /// the server.
    terminate_sender: Option<Sender<()>>,
}

impl ServerState {
    fn new(terminate_sender: Option<Sender<()>>) -> Self {
        Self {
            phase: RwLock::new(PipelinePhase::Initializing),
            metadata: RwLock::new(String::new()),
            controller: Mutex::new(None),
            prometheus: RwLock::new(None),
            terminate_sender,
        }
    }
}

#[derive(Parser, Debug, Default)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Pipeline configuration YAML file
    #[arg(short, long)]
    config_file: String,

    /// Pipeline metadata JSON file
    #[arg(short, long)]
    metadata_file: Option<String>,

    /// Persistent Storage location.
    ///
    /// If no default is specified, a temporary directory is used.
    #[arg(short, long)]
    pub(crate) storage_location: Option<PathBuf>,

    /// TCP bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind_address: String,

    /// Run the server on this port if it is available. If the port is in
    /// use or no default port is specified, an unused TCP port is allocated
    /// automatically
    #[arg(short = 'p', long)]
    default_port: Option<u16>,
}

/// Server main function.
///
/// This function is intended to be invoked from the code generated by,
/// e.g., the SQL compiler.  It performs the following steps needed to start
/// a circuit server:
///
/// * Setup logging.
/// * Parse command line arguments.
/// * Start the server.
///
/// # Arguments
///
/// * `circuit_factory` - a function that creates a circuit and builds an
///   input/output stream catalog.
pub fn server_main<F>(circuit_factory: F) -> Result<(), ControllerError>
where
    F: FnOnce(
            CircuitConfig,
        )
            -> Result<(Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
{
    let args = ServerArgs::try_parse().map_err(|e| ControllerError::cli_args_error(&e))?;

    run_server(args, circuit_factory).map_err(|e| {
        error!("{e}");
        e
    })
}

pub fn run_server<F>(args: ServerArgs, circuit_factory: F) -> Result<(), ControllerError>
where
    F: FnOnce(
            CircuitConfig,
        )
            -> Result<(Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
{
    ensure_default_crypto_provider();

    let bind_address = args.bind_address.clone();
    let port = args.default_port.unwrap_or(0);
    let listener = TcpListener::bind((bind_address, port))
        .map_err(|e| ControllerError::io_error(format!("binding to TCP port {port}"), e))?;

    let port = listener
        .local_addr()
        .map_err(|e| {
            ControllerError::io_error(
                "retrieving local socket address of the TCP listener".to_string(),
                e,
            )
        })?
        .port();

    let (terminate_sender, mut terminate_receiver) = channel(1);

    let state = WebData::new(ServerState::new(Some(terminate_sender)));
    let state_clone = state.clone();

    // The bootstrap thread will read the config, including pipeline name,
    // and initalize the logger.  Use this channel to wait for the log to
    // be ready, so that the first few messages from the server don't get
    // lost.
    let (loginit_sender, loginit_receiver) = mpsc::channel();

    // Initialize the pipeline in a separate thread.  On success, this thread
    // will create a `Controller` instance and store it in `state.controller`.
    thread::spawn(move || bootstrap(args, circuit_factory, state_clone, loginit_sender));
    let _ = loginit_receiver.recv();

    let server = HttpServer::new(move || {
        let state = state.clone();
        build_app(
            App::new().wrap_fn(|req, srv| {
                trace!("Request: {} {}", req.method(), req.path());
                srv.call(req).map(|res| {
                    match &res {
                        Ok(response) => {
                            let level = if response.status().is_success() {
                                Level::Debug
                            } else {
                                Level::Error
                            };
                            let req = response.request();
                            log!(
                                level,
                                "Response: {} (size: {:?}) to request {} {}",
                                response.status(),
                                response.response().body().size(),
                                req.method(),
                                req.path()
                            );
                        }
                        Err(e) => {
                            error!("Service response error: {e}");
                        }
                    }
                    res
                })
            }),
            state,
        )
    })
    // Set timeout for graceful shutdown of workers.
    // The default in actix is 30s. We may consider making this configurable.
    .shutdown_timeout(10)
    .workers(NUM_HTTP_WORKERS)
    .listen(listener)
    .map_err(|e| ControllerError::io_error("binding server to the listener".to_string(), e))?
    .run();

    rt::System::new().block_on(async {
        // Spawn a task that will shutdown the server on `/kill`.
        let server_handle = server.handle();
        spawn(async move {
            terminate_receiver.recv().await;
            server_handle.stop(true).await
        });

        info!("Started HTTP server on port {port}");

        // We don't want outside observers (e.g., the local runner) to observe a partially
        // written port file, so we write it to a temporary file first, and then rename the
        // temporary.
        let tmp_server_port_file = format!("{SERVER_PORT_FILE}.tmp");
        tokio::fs::write(&tmp_server_port_file, format!("{}\n", port))
            .await
            .map_err(|e| ControllerError::io_error("writing server port file".to_string(), e))?;
        tokio::fs::rename(&tmp_server_port_file, SERVER_PORT_FILE)
            .await
            .map_err(|e| ControllerError::io_error("renaming server port file".to_string(), e))?;
        server
            .await
            .map_err(|e| ControllerError::io_error("in the HTTP server".to_string(), e))
    })?;

    Ok(())
}

fn parse_config(config_file: &str) -> Result<PipelineConfig, ControllerError> {
    let yaml_config = std::fs::read(config_file).map_err(|e| {
        ControllerError::io_error(format!("reading configuration file '{}'", config_file), e)
    })?;

    let yaml_config = String::from_utf8(yaml_config).map_err(|e| {
        ControllerError::pipeline_config_parse_error(&format!(
            "invalid UTF8 string in configuration file '{}' ({e})",
            &config_file
        ))
    })?;

    // Still running without logger here.
    eprintln!("Pipeline configuration:\n{yaml_config}");

    serde_yaml::from_str(yaml_config.as_str())
        .map_err(|e| ControllerError::pipeline_config_parse_error(&e))
}

// Initialization thread function.
fn bootstrap<F>(
    args: ServerArgs,
    circuit_factory: F,
    state: WebData<ServerState>,
    loginit_sender: StdSender<()>,
) where
    F: FnOnce(
            CircuitConfig,
        )
            -> Result<(Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
{
    do_bootstrap(args, circuit_factory, &state, loginit_sender).unwrap_or_else(|e| {
        // Store error in `state.phase`, so that it can be
        // reported by the server.
        error!("Error initializing the pipeline: {e}.");
        *state.phase.write().unwrap() = PipelinePhase::InitializationError(Arc::new(e));
    })
}

/// True if the pipeline cannot operate after `error` and must be shut down.
fn is_fatal_controller_error(error: &ControllerError) -> bool {
    matches!(
        error,
        ControllerError::DbspError { .. } | ControllerError::DbspPanic
    )
}

/// Handle errors from the controller.
fn error_handler(state: &Weak<ServerState>, error: ControllerError) {
    error!("{error}");

    let state = match state.upgrade() {
        None => return,
        Some(state) => state,
    };

    if is_fatal_controller_error(&error) {
        // Prepare to handle poisoned locks in the following code.

        if let Ok(mut controller) = state.controller.lock() {
            if controller.is_some() {
                *controller = None;
                // Don't risk calling `controller.stop()`.  If the error is caused
                // by a panic in a DBSP worker thread, the `join` call in
                // `controller.stop()` will panic.  We may consider calling `stop()`
                // on errors do not indicate a panic, but that still seems risky.

                /*if let Some(controller) = controller.take() {
                    let _ = controller.stop();
                }*/
                if let Ok(mut phase) = state.phase.write() {
                    *phase = PipelinePhase::Failed(Arc::new(error));
                }
            }
        }

        /*if let Some(sender) = &state.terminate_sender {
            let _ = sender.try_send(());
        }
        if let Err(e) = std::fs::remove_file(SERVER_PORT_FILE) {
            warn!("Failed to remove server port file: {e}");
        }*/
    }
}

fn do_bootstrap<F>(
    args: ServerArgs,
    circuit_factory: F,
    state: &WebData<ServerState>,
    loginit_sender: StdSender<()>,
) -> Result<(), ControllerError>
where
    F: FnOnce(
            CircuitConfig,
        )
            -> Result<(Box<dyn DbspCircuitHandle>, Box<dyn CircuitCatalog>), ControllerError>
        + Send
        + 'static,
{
    // Print error directly to stdout until we've initialized the logger.
    let config = parse_config(&args.config_file).map_err(|e| {
        let _ = loginit_sender.send(());
        eprintln!("{e}");
        e
    })?;

    // Create env logger.
    let pipeline_name = format!("[{}]", config.name.clone().unwrap_or_default()).cyan();
    // By default, logging is set to INFO level for the Feldera crates:
    // - "project" for the generated project<uuid> crate
    // - "dbsp" for the dbsp crate
    // - "dbsp_adapters" for the adapters crate which is renamed
    // - "dbsp_nexmark" for the nexmark crate which is renamed
    // - "feldera_types" for the feldera-types crate
    // For all others, the WARN level is used.
    // Note that this can be overridden by setting the RUST_LOG environment variable.
    env_logger::Builder::from_env(Env::default().default_filter_or(
        "warn,project=info,dbsp=info,dbsp_adapters=info,dbsp_nexmark=info,feldera_types=info",
    ))
    .format(move |buf, record| {
        let t = chrono::Utc::now();
        let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S"));
        writeln!(
            buf,
            "{t} {} {pipeline_name} {}",
            buf.default_styled_level(record.level()),
            record.args()
        )
    })
    .try_init()
    .unwrap_or_else(|e| {
        // This happens in unit tests when another test has initialized logging.
        eprintln!("Failed to initialize logging: {e}.")
    });
    let _ = loginit_sender.send(());

    if config.global.tracing {
        use std::net::{SocketAddr, ToSocketAddrs};
        let socket_addrs: Vec<SocketAddr> = config
            .global
            .tracing_endpoint_jaeger
            .to_socket_addrs()
            .expect("Valid `tracing_endpoint_jaeger` value (e.g., localhost:6831)")
            .collect();
        let reporter = minitrace_jaeger::JaegerReporter::new(
            *socket_addrs
                .first()
                .expect("Valid `tracing_endpoint_jaeger` value (e.g., localhost:6831)"),
            config
                .name
                .clone()
                .unwrap_or("unknown pipeline".to_string()),
        )
        .unwrap();
        minitrace::set_reporter(reporter, Config::default());
    }

    *state.metadata.write().unwrap() = match &args.metadata_file {
        None => String::new(),
        Some(metadata_file) => {
            let meta = std::fs::read(metadata_file).map_err(|e| {
                ControllerError::io_error(format!("reading metadata file '{}'", metadata_file), e)
            })?;
            String::from_utf8(meta).map_err(|e| {
                ControllerError::pipeline_config_parse_error(&format!(
                    "invalid UTF8 string in the metadata file '{}' ({e})",
                    metadata_file
                ))
            })?
        }
    };

    let weak_state_ref = Arc::downgrade(state);

    let controller = Controller::with_config(
        circuit_factory,
        &config,
        Box::new(move |e| error_handler(&weak_state_ref, e))
            as Box<dyn Fn(ControllerError) + Send + Sync>,
    )?;

    *state.prometheus.write().unwrap() = Some(
        PrometheusMetrics::new(&controller).map_err(|e| ControllerError::prometheus_error(&e))?,
    );
    *state.controller.lock().unwrap() = Some(controller);

    info!("Pipeline initialization complete");
    *state.phase.write().unwrap() = PipelinePhase::InitializationComplete;

    Ok(())
}

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    app.app_data(state)
        .route(
            "/",
            web::get().to(move || async {
                HttpResponse::Ok().body("<html><head><title>DBSP server</title></head></html>")
            }),
        )
        .service(start)
        .service(pause)
        .service(shutdown)
        .service(query)
        .service(stats)
        .service(metrics)
        .service(metadata)
        .service(heap_profile)
        .service(dump_profile)
        .service(input_endpoint)
        .service(output_endpoint)
        .service(pause_input_endpoint)
        .service(start_input_endpoint)
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.start();
            Ok(HttpResponse::Ok().json("The pipeline is running"))
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.pause();
            Ok(HttpResponse::Ok().json("Pipeline paused"))
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/query")]
async fn query(state: WebData<ServerState>, args: Query<AdhocQueryArgs>) -> impl Responder {
    let session_ctxt = {
        let controller = state.controller.lock().unwrap();
        controller.as_ref().map(|c| c.session_context())
    };

    match session_ctxt {
        Some(session) => {
            let df = session.sql(&args.sql).await?;
            let response = df.collect().await?;

            match args.format {
                AdHocResultFormat::Text => {
                    let pretty_results = arrow::util::pretty::pretty_format_batches(&response)
                        .map_err(DataFusionError::from)?
                        .to_string();
                    Ok(HttpResponse::Ok()
                        .content_type(mime::TEXT_PLAIN)
                        .body(pretty_results))
                }
                AdHocResultFormat::Json => {
                    let mut buf = Vec::with_capacity(4096);
                    let mut writer = LineDelimitedWriter::new(&mut buf);
                    writer
                        .write_batches(response.iter().collect::<Vec<&RecordBatch>>().as_slice())
                        .map_err(DataFusionError::from)?;
                    writer.finish().map_err(DataFusionError::from)?;

                    Ok(HttpResponse::Ok()
                        .content_type(mime::APPLICATION_JSON)
                        .body(buf))
                }
                AdHocResultFormat::Parquet => {
                    let mut buf = Vec::with_capacity(4096);
                    let writer_properties = WriterProperties::builder().build();
                    let schema = response[0].schema();
                    let mut writer =
                        ArrowWriter::try_new(&mut buf, schema.clone(), Some(writer_properties))
                            .map_err(PipelineError::from)?;
                    for batch in response {
                        writer.write(&batch)?;
                    }
                    let file_name = format!(
                        "results_{}.parquet",
                        chrono::Utc::now().format("%Y%m%d_%H%M%S")
                    );
                    writer.close().expect("closing writer");
                    Ok(HttpResponse::Ok()
                        .insert_header(header::ContentDisposition::attachment(file_name))
                        .content_type(mime::APPLICATION_OCTET_STREAM)
                        .body(buf))
                }
            }
        }
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/stats")]
async fn stats(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            let json_string = serde_json::to_string(controller.status()).unwrap();
            Ok(HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(json_string))
        }
        None => Err(missing_controller_error(&state)),
    }
}

/// This endpoint is invoked by the Prometheus server.
#[get("/metrics")]
async fn metrics(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => match state
            .prometheus
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .metrics(controller)
        {
            Ok(metrics) => Ok(HttpResponse::Ok()
                .content_type(mime::TEXT_PLAIN)
                .body(metrics)),
            Err(e) => Err(PipelineError::PrometheusError {
                error: e.to_string(),
            }),
        },
        None => Err(missing_controller_error(&state)),
    }
}

#[get("/metadata")]
async fn metadata(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(state.metadata.read().unwrap().clone())
}

#[get("/heap_profile")]
async fn heap_profile() -> impl Responder {
    #[cfg(target_os = "linux")]
    {
        let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
        if !prof_ctl.activated() {
            return Err(PipelineError::HeapProfilerError {
                error: "jemalloc profiling is disabled".to_string(),
            });
        }
        match prof_ctl.dump_pprof() {
            Ok(profile) => Ok(HttpResponse::Ok()
                .content_type("application/protobuf")
                .body(profile)),
            Err(e) => Err(PipelineError::HeapProfilerError {
                error: e.to_string(),
            }),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err::<HttpResponse, PipelineError>(PipelineError::HeapProfilerError {
            error: "heap profiling is only supported on Linux".to_string(),
        })
    }
}

#[get("/dump_profile")]
async fn dump_profile(state: WebData<ServerState>) -> impl Responder {
    let (sender, receiver) = oneshot::channel();
    match &*state.controller.lock().unwrap() {
        None => return Err(missing_controller_error(&state)),
        Some(controller) => {
            controller.start_graph_profile(Box::new(move |profile| {
                if sender.send(profile).is_err() {
                    error!("`/dump_profile` result could not be sent");
                }
            }));
        }
    };
    let profile = receiver.await.unwrap()?;

    Ok(HttpResponse::Ok()
        .insert_header(header::ContentType("application/zip".parse().unwrap()))
        .insert_header(header::ContentDisposition::attachment("profile.zip"))
        .body(profile.as_zip()))
}

#[get("/shutdown")]
async fn shutdown(state: WebData<ServerState>) -> impl Responder {
    let controller = state.controller.lock().unwrap().take();
    if let Some(controller) = controller {
        match controller.stop() {
            Ok(()) => {
                if let Some(sender) = &state.terminate_sender {
                    let _ = sender.send(()).await;
                }
                if let Err(e) = tokio::fs::remove_file(SERVER_PORT_FILE).await {
                    warn!("Failed to remove server port file: {e}");
                }
                Ok(HttpResponse::Ok().json("Pipeline terminated"))
            }
            Err(e) => Err(e),
        }
    } else {
        // TODO: handle ongoing initialization
        Ok(HttpResponse::Ok().json("Pipeline already terminated"))
    }
}

#[derive(Debug, Deserialize)]
struct IngressArgs {
    #[serde(default = "HttpInputTransport::default_format")]
    format: String,
    /// Push data to the pipeline even if the pipeline is in a paused state.
    #[serde(default)]
    force: bool,
}

#[post("/ingress/{table_name}")]
async fn input_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<IngressArgs>,
    payload: Payload,
) -> impl Responder {
    debug!("{req:?}");
    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };
    // debug!("Table name {table_name:?}");

    // Generate endpoint name.
    let endpoint_name = format!("api-ingress-{table_name}-{}", Uuid::new_v4());

    // Create HTTP endpoint.
    let endpoint = HttpInputEndpoint::new(&endpoint_name, args.force);

    // Create endpoint config.
    let config = InputEndpointConfig {
        stream: Cow::from(table_name),
        connector_config: ConnectorConfig {
            transport: HttpInputTransport::config(),
            format: Some(parser_config_from_http_request(
                &endpoint_name,
                &args.format,
                &req,
            )?),
            output_buffer_config: Default::default(),
            max_batch_size: default_max_batch_size(),
            max_queued_records: HttpInputTransport::default_max_buffered_records(),
            paused: false,
        },
    };

    // Connect endpoint.
    let endpoint_id = match &*state.controller.lock().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return Err(PipelineError::ApiConnectionLimit);
            }

            match controller.add_input_endpoint(
                &endpoint_name,
                config,
                Box::new(endpoint.clone()) as Box<dyn TransportInputEndpoint>,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    debug!("Failed to create API endpoint: '{e}'");
                    Err(e)?
                }
            }
        }
        None => {
            return Err(missing_controller_error(&state));
        }
    };

    // Call endpoint to complete request.
    let response = endpoint.complete_request(payload).await;
    drop(endpoint);

    // Delete endpoint on completion/error.
    if let Some(controller) = state.controller.lock().unwrap().as_ref() {
        controller.disconnect_input(&endpoint_id);
        controller.unregister_api_connection();
    }

    response
}

/// Create an instance of `FormatConfig` from format name and
/// HTTP request using the `InputFormat::config_from_http_request` method.
pub fn parser_config_from_http_request(
    endpoint_name: &str,
    format_name: &str,
    request: &HttpRequest,
) -> Result<FormatConfig, ControllerError> {
    let format = <dyn InputFormat>::get_format(format_name)
        .ok_or_else(|| ControllerError::unknown_input_format(endpoint_name, format_name))?;

    let config = format.config_from_http_request(endpoint_name, request)?;

    // Convert config to YAML format.
    // FIXME: this is hacky. Perhaps we can parameterize `FormatConfig` with the
    // exact type stored in the `config` field, so it can be either YAML or a
    // strongly typed format-specific config.
    Ok(FormatConfig {
        name: Cow::from(format_name.to_string()),
        config: serde_yaml::to_value(config)
            .map_err(|e| ControllerError::parser_config_parse_error(endpoint_name, &e, ""))?,
    })
}

/// Create an instance of `FormatConfig` from format name and
/// HTTP request using the `InputFormat::config_from_http_request` method.
pub fn encoder_config_from_http_request(
    endpoint_name: &str,
    format_name: &str,
    request: &HttpRequest,
) -> Result<FormatConfig, ControllerError> {
    let format = <dyn OutputFormat>::get_format(format_name)
        .ok_or_else(|| ControllerError::unknown_output_format(endpoint_name, format_name))?;

    let config = format.config_from_http_request(endpoint_name, request)?;

    Ok(FormatConfig {
        name: Cow::from(format_name.to_string()),
        config: serde_yaml::to_value(config)
            .map_err(|e| ControllerError::encoder_config_parse_error(endpoint_name, &e, ""))?,
    })
}

/// URL-encoded arguments to the `/egress` endpoint.
#[derive(Debug, Deserialize)]
struct EgressArgs {
    /// Query to execute on the table.
    #[serde(default)]
    query: OutputQuery,

    /// Output mode.
    #[serde(default)]
    mode: EgressMode,

    /// Apply backpressure on the pipeline when the HTTP client cannot receive
    /// data fast enough.
    ///
    /// When this flag is set to false (the default), the HTTP connector drops data
    /// chunks if the client is not keeping up with its output.  This prevents
    /// a slow HTTP client from slowing down the entire pipeline.
    ///
    /// When the flag is set to true, the connector waits for the client to receive
    /// each chunk and blocks the pipeline if the client cannot keep up.
    #[serde(default)]
    backpressure: bool,

    /// Data format used to encode the output of the query, e.g., 'csv',
    /// 'json' etc.
    #[serde(default = "HttpOutputTransport::default_format")]
    format: String,

    /// For [`quantiles`](`OutputQuery::Quantiles`) queries:
    /// the number of quantiles to output.
    #[serde(default = "dbsp::operator::sample::default_quantiles")]
    quantiles: u32,
}

#[post("/egress/{table_name}")]
async fn output_endpoint(
    state: WebData<ServerState>,
    req: HttpRequest,
    args: Query<EgressArgs>,
    body: Option<Json<JsonValue>>,
) -> impl Responder {
    debug!("/egress request:{req:?}");

    let state = state.into_inner();

    let table_name = match req.match_info().get("table_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "table_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };

    // Check for unsupported combinations.
    match (args.mode, args.query) {
        (EgressMode::Watch, OutputQuery::Quantiles) => {
            return Err(PipelineError::QuantileStreamingNotSupported);
        }
        (EgressMode::Snapshot, OutputQuery::Table) => {
            return Err(PipelineError::TableSnapshotNotImplemented);
        }
        _ => {}
    };

    if args.query == OutputQuery::Neighborhood {
        if body.is_none() {
            return Err(PipelineError::MissingNeighborhoodSpec);
        }
    } else if args.query == OutputQuery::Quantiles
        && (args.quantiles as usize > MAX_QUANTILES || args.quantiles == 0)
    {
        return Err(PipelineError::NumQuantilesOutOfRange {
            quantiles: args.quantiles,
        });
    }

    // Generate endpoint name depending on the query and output mode.
    let endpoint_name = format!(
        "api-{}-{table_name}-{}{}",
        match args.mode {
            EgressMode::Watch => "watch",
            EgressMode::Snapshot => "snapshot",
        },
        match args.query {
            OutputQuery::Table => "",
            OutputQuery::Neighborhood => "neighborhood-",
            OutputQuery::Quantiles => "quantiles-",
        },
        Uuid::new_v4()
    );

    // debug!("Endpoint name: '{endpoint_name}'");

    // Create HTTP endpoint.
    let endpoint = HttpOutputEndpoint::new(
        &endpoint_name,
        &args.format,
        matches!(
            args.query,
            OutputQuery::Neighborhood | OutputQuery::Quantiles
        ),
        args.mode == EgressMode::Watch,
        args.backpressure,
    );

    // Create endpoint config.
    let config = OutputEndpointConfig {
        stream: Cow::from(table_name),
        query: args.query,
        connector_config: ConnectorConfig {
            transport: HttpOutputTransport::config(),
            format: Some(encoder_config_from_http_request(
                &endpoint_name,
                &args.format,
                &req,
            )?),
            output_buffer_config: Default::default(),
            max_batch_size: default_max_batch_size(),
            max_queued_records: HttpOutputTransport::default_max_buffered_records(),
            paused: false,
        },
    };

    // Declare `response` in this scope, before we lock `state.controller`.  This
    // makes sure that on error the finalizer for `response` also runs in this
    // scope, preventing the deadlock caused by the finalizer trying to lock the
    // controller.
    let response: HttpResponse;

    // Connect endpoint.
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            if controller.register_api_connection().is_err() {
                return Err(PipelineError::ApiConnectionLimit);
            }

            let endpoint_id = match controller.add_output_endpoint(
                &endpoint_name,
                &config,
                Box::new(endpoint.clone()) as Box<dyn OutputEndpoint>,
            ) {
                Ok(endpoint_id) => endpoint_id,
                Err(e) => {
                    controller.unregister_api_connection();
                    Err(e)?
                }
            };

            // We need to pass a callback to `request` to disconnect the endpoint when the
            // request completes.  Use a donwgraded reference to `state`, so
            // this closure doesn't prevent the controller from shutting down.
            let weak_state = Arc::downgrade(&state);

            // Call endpoint to create a response with a streaming body, which will be
            // evaluated after we return the response object to actix.
            response = endpoint.request(Box::new(move || {
                // Delete endpoint on completion/error.
                // We don't control the lifetime of the reponse object after
                // returning it to actix, so the only way to run cleanup code
                // when the HTTP request terminates is to piggyback on the
                // destructor.
                if let Some(state) = weak_state.upgrade() {
                    // This code will be invoked from `drop`, which means that
                    // it can run as part of a panic handler, so we need to
                    // handle a poisoned lock without causing a nested panic.
                    if let Ok(guard) = state.controller.lock() {
                        if let Some(controller) = guard.as_ref() {
                            controller.disconnect_output(&endpoint_id);
                            controller.unregister_api_connection();
                        }
                    }
                }
            }));

            // The endpoint is ready to receive data from the pipeline.
            match args.query {
                // Send reset signal to produce a complete neighborhood snapshot.
                OutputQuery::Neighborhood => {
                    let body = body.unwrap().into_inner();

                    let json = serde_json::to_string(&json!([json!(true), body])).map_err(|e| {
                        PipelineError::InvalidNeighborhoodSpec {
                            spec: body.clone(),
                            parse_error: e.to_string(),
                        }
                    })?;

                    if let Err(e) = controller
                        .catalog()
                        .lock()
                        .unwrap()
                        .output_handles(&SqlIdentifier::from(config.stream))
                        // The following `unwrap` is safe because `table_name` was previously
                        // validated by `add_output_endpoint`.
                        .unwrap()
                        .neighborhood_descr_handle
                        .as_ref()
                        .ok_or_else(|| PipelineError::NeighborhoodNotSupported)?
                        .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))?
                        .set_for_all(json.as_bytes())
                    {
                        // Dropping `response` triggers the finalizer closure, which will
                        // disconnect this endpoint.
                        return Err(PipelineError::InvalidNeighborhoodSpec {
                            spec: body,
                            parse_error: e.to_string(),
                        });
                    }
                    controller.request_step();
                }
                // Write quantiles size.
                OutputQuery::Quantiles => {
                    controller
                        .catalog()
                        .lock()
                        .unwrap()
                        .output_handles(&SqlIdentifier::from(config.stream))
                        .unwrap()
                        .num_quantiles_handle
                        .as_ref()
                        .ok_or(PipelineError::QuantilesNotSupported)?
                        .set_for_all(args.quantiles as usize);
                    controller.request_step();
                }
                OutputQuery::Table => {}
            }
        }
        None => return Err(missing_controller_error(&state)),
    };

    Ok(response)
}

#[get("/input_endpoints/{endpoint_name}/pause")]
async fn pause_input_endpoint(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let endpoint_name = match req.match_info().get("endpoint_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "endpoint_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };

    match &*state.controller.lock().unwrap() {
        Some(controller) => controller.pause_input_endpoint(&endpoint_name)?,
        None => {
            return Err(missing_controller_error(&state));
        }
    };

    Ok(HttpResponse::Ok())
}

#[get("/input_endpoints/{endpoint_name}/start")]
async fn start_input_endpoint(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let endpoint_name = match req.match_info().get("endpoint_name") {
        None => {
            return Err(PipelineError::MissingUrlEncodedParam {
                param: "endpoint_name",
            });
        }
        Some(table_name) => table_name.to_string(),
    };

    match &*state.controller.lock().unwrap() {
        Some(controller) => controller.start_input_endpoint(&endpoint_name)?,
        None => {
            return Err(missing_controller_error(&state));
        }
    };

    Ok(HttpResponse::Ok())
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::{bootstrap, build_app, ServerArgs, ServerState};
    use crate::{
        controller::MAX_API_CONNECTIONS,
        ensure_default_crypto_provider,
        test::{
            generate_test_batches,
            http::{TestHttpReceiver, TestHttpSender},
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, TestStruct,
        },
    };
    use actix_web::{
        http::StatusCode,
        middleware::Logger,
        web::{Bytes, Data as WebData},
        App,
    };
    use futures_util::StreamExt;
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use serde_json::{self, json, Value as JsonValue};
    use std::{
        io::Write,
        thread,
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::NamedTempFile;

    #[actix_web::test]
    async fn test_server() {
        ensure_default_crypto_provider();

        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(0, 100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        //let _ = log::set_logger(&TEST_LOGGER);
        //log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new("test_server_output_topic", "csv", "", None);

        // Config string
        let config_str = r#"
name: test
inputs:
    test_input1:
        stream: test_input1
        paused: true
        transport:
            name: kafka_input
            config:
                auto.offset.reset: "earliest"
                topics: [test_server_input_topic]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                topic: test_server_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#;

        let mut config_file = NamedTempFile::new().unwrap();
        config_file.write_all(config_str.as_bytes()).unwrap();

        println!("Creating HTTP server");

        let state = WebData::new(ServerState::new(None));
        let state_clone = state.clone();

        let args = ServerArgs {
            config_file: config_file.path().display().to_string(),
            metadata_file: None,
            bind_address: "127.0.0.1".to_string(),
            default_port: None,
            storage_location: None,
        };
        thread::spawn(move || {
            bootstrap(
                args,
                |workers| Ok(test_circuit::<TestStruct>(workers, &TestStruct::schema())),
                state_clone,
                std::sync::mpsc::channel().0,
            )
        });

        let server =
            actix_test::start(move || build_app(App::new().wrap(Logger::default()), state.clone()));

        let start = Instant::now();
        while server.get("/stats").send().await.unwrap().status() == StatusCode::SERVICE_UNAVAILABLE
        {
            assert!(start.elapsed() < Duration::from_millis(20_000));
            sleep(Duration::from_millis(200));
        }

        // Request quantiles while the table is empty.  This should return an empty
        // quantile.
        let mut quantiles_resp = server
            .post("/egress/test_output1?mode=snapshot&query=quantiles")
            .send()
            .await
            .unwrap();
        assert!(quantiles_resp.status().is_success());
        let body = quantiles_resp.body().await.unwrap();
        assert_eq!(
            body,
            Bytes::from_static(b"{\"sequence_number\":0,\"text_data\":\"\"}\r\n")
        );

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start pipeline.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(3000));

        // Input connector is still paused: expect empty outputs.
        let mut quantiles_resp = server
            .post("/egress/test_output1?mode=snapshot&query=quantiles")
            .send()
            .await
            .unwrap();
        assert!(quantiles_resp.status().is_success());
        let body = quantiles_resp.body().await.unwrap();
        assert_eq!(
            body,
            Bytes::from_static(b"{\"sequence_number\":0,\"text_data\":\"\"}\r\n")
        );

        // Unpause input endpoint.
        println!("/input_endpoints/test_input1/start");
        let resp = server
            .get("/input_endpoints/test_input1/start")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Wait for data.
        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/stats");
        let resp = server.get("/stats").send().await.unwrap();
        assert!(resp.status().is_success());

        println!("/metadata");
        let resp = server.get("/metadata").send().await.unwrap();
        assert!(resp.status().is_success());

        // Pause input endpoint.
        println!("/input_endpoints/test_input1/pause");
        let resp = server
            .get("/input_endpoints/test_input1/pause")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        // Pause pipeline.
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        // Send more data, receive none
        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start pipeline; still no data because the endpoint is paused.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Resume input endpoint, receive data.
        println!("/input_endpoints/test_input1/start");
        let resp = server
            .get("/input_endpoints/test_input1/start")
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        loop {
            let stats = server
                .get("/stats")
                .send()
                .await
                .unwrap()
                .json::<JsonValue>()
                .await
                .unwrap();
            // println!("stats: {stats:#}");
            let num_errors = stats.get("inputs").unwrap().as_array().unwrap()[0]
                .get("metrics")
                .unwrap()
                .get("num_parse_errors")
                .unwrap()
                .as_u64()
                .unwrap();
            if num_errors == 1 {
                break;
            }
        }

        // Make sure that HTTP connections get dropped on client disconnect
        // (see comment in `HttpOutputEndpoint::request`).  We create 2x the
        // number of supported simultaneous API connections and drop the client
        // side instantly, which should cause the server side to close within
        // 6 seconds.  If everything works as intended, this should _not_
        // trigger the API connection limit error.
        for _ in 0..2 * MAX_API_CONNECTIONS {
            assert!(server
                .post("/egress/test_output1")
                .send()
                .await
                .unwrap()
                .status()
                .is_success());
            sleep(Duration::from_millis(75));
        }

        println!("Connecting to HTTP output endpoint");
        let mut resp1 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        let mut resp2 = server
            .post("/egress/test_output1?backpressure=true")
            .send()
            .await
            .unwrap();

        println!("Streaming test");
        let req = server.post("/ingress/test_input1");

        TestHttpSender::send_stream(req, &data).await;
        println!("data sent");

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;

        // Force-push data in paused state.
        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        println!("Force-push data via HTTP");
        let req = server.post("/ingress/test_input1?force=true");

        TestHttpSender::send_stream(req, &data).await;

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        TestHttpReceiver::wait_for_output_unordered(&mut resp1, &data).await;
        TestHttpReceiver::wait_for_output_unordered(&mut resp2, &data).await;
        drop(resp1);
        drop(resp2);

        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert!(resp.status().is_success());

        sleep(Duration::from_millis(5000));

        // Request quantiles.
        let mut quantiles_resp = server
            .post("/egress/test_output1?mode=snapshot&query=quantiles&backpressure=true")
            .send()
            .await
            .unwrap();
        assert!(quantiles_resp.status().is_success());
        let body = quantiles_resp.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Quantiles: {body}");

        // Request quantiles for the input collection -- inputs must also behave as
        // outputs.
        let mut input_quantiles = server
            .post("/egress/test_input1?mode=snapshot&query=quantiles&backpressure=true")
            .send()
            .await
            .unwrap();
        assert!(input_quantiles.status().is_success());
        let body = input_quantiles.body().await;
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Input quantiles: {body}");

        // Request neighborhood snapshot.
        let mut hood_resp1 = server
            .post("/egress/test_output1?mode=snapshot&query=neighborhood&backpressure=true")
            .send_json(
                &json!({"anchor": {"id": 1000, "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert!(hood_resp1.status().is_success());
        let body = hood_resp1.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Neighborhood: {body}");

        // Request default neighborhood snapshot.
        let mut hood_resp_default = server
            .post("/egress/test_output1?mode=snapshot&query=neighborhood&backpressure=true")
            .send_json(&json!({"before": 50, "after": 30}))
            .await
            .unwrap();
        assert!(hood_resp_default.status().is_success());
        let body = hood_resp_default.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Default neighborhood: {body}");

        // Request neighborhood snapshot: invalid request.
        let mut hood_inv_resp = server
            .post("/egress/test_output1?mode=snapshot&query=neighborhood&backpressure=true")
            .send_json(
                &json!({"anchor": {"id": "string_instead_of_integer", "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert_eq!(hood_inv_resp.status(), StatusCode::BAD_REQUEST);
        let body = hood_inv_resp.body().await;
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&body.unwrap()).unwrap();
        println!("Neighborhood: {body}");

        // Request neighborhood stream.
        let mut hood_resp2 = server
            .post("/egress/test_output1?mode=watch&query=neighborhood&backpressure=true")
            .send_json(
                &json!({"anchor": {"id": 1000, "b": true, "s": "foo"}, "before": 50, "after": 30}),
            )
            .await
            .unwrap();
        assert!(hood_resp2.status().is_success());

        let bytes = hood_resp2.next().await.unwrap().unwrap();
        // println!("Response: {body:?}");
        let body = serde_json::from_slice::<JsonValue>(&bytes).unwrap();
        println!("Neighborhood: {body}");

        println!("/pause");
        let resp = server.get("/pause").send().await.unwrap();
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        // Shutdown
        println!("/shutdown");
        let resp = server.get("/shutdown").send().await.unwrap();
        // println!("Response: {resp:?}");
        assert!(resp.status().is_success());

        // Start after shutdown must fail.
        println!("/start");
        let resp = server.get("/start").send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
