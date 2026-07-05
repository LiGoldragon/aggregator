use std::{
    io::{Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use meta_signal_aggregator::{
    MetaAggregatorFrame, MetaAggregatorFrameBody, MetaAggregatorOperationKind, MetaAggregatorReply,
    MetaAggregatorRequest, SocketMode,
};
use signal_aggregator::{
    AggregatorFrame, AggregatorFrameBody, AggregatorReply, AggregatorRequest, RejectionReason,
};
use signal_frame::{NonEmpty, Reply as FrameReply, RequestRejectionReason, SubReply};

use crate::{
    CollectionClock, ConfigurationStore, Error, NexusPlane, Result, RuntimeConfiguration,
    RuntimeConfigurationValidation, SemaPlane, SignalPlane,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatorDaemonCommand {
    arguments: DaemonCommandArguments,
}

impl AggregatorDaemonCommand {
    pub fn from_environment() -> Self {
        Self {
            arguments: DaemonCommandArguments::from_environment(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let configuration_path = self.arguments.configuration_path()?;
        let configuration_store = ConfigurationStore::at_path(configuration_path);
        let configuration = configuration_store.read_configuration()?;
        let sema = Arc::new(Mutex::new(SemaPlane::with_configuration_store(
            configuration.clone(),
            configuration_store,
        )));
        PrototypeDaemon::new(configuration, sema, CollectionClock::from_environment()?).run()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonCommandArguments {
    arguments: Vec<String>,
}

impl DaemonCommandArguments {
    pub fn from_environment() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    pub fn configuration_path(&self) -> Result<PathBuf> {
        if let Some(path) = self.flag_value("--configuration") {
            return Ok(PathBuf::from(path));
        }
        std::env::var("AGGREGATOR_CONFIGURATION")
            .map(PathBuf::from)
            .map_err(|_| Error::argument("missing --configuration or AGGREGATOR_CONFIGURATION"))
    }

    pub fn flag_value(&self, name: &str) -> Option<&str> {
        self.arguments
            .windows(2)
            .find(|window| window[0] == name)
            .map(|window| window[1].as_str())
    }
}

#[derive(Debug, Clone)]
pub struct PrototypeDaemon {
    configuration: meta_signal_aggregator::AggregatorConfiguration,
    sema: Arc<Mutex<SemaPlane>>,
    clock: CollectionClock,
}

impl PrototypeDaemon {
    pub fn new(
        configuration: meta_signal_aggregator::AggregatorConfiguration,
        sema: Arc<Mutex<SemaPlane>>,
        clock: CollectionClock,
    ) -> Self {
        Self {
            configuration,
            sema,
            clock,
        }
    }

    pub fn run(&self) -> Result<()> {
        let ordinary_socket = PathBuf::from(self.configuration.ordinary_socket_path.as_str());
        let meta_socket = PathBuf::from(self.configuration.meta_socket_path.as_str());
        let ordinary_service = OrdinarySocketService::new(
            ordinary_socket,
            self.configuration.ordinary_socket_mode,
            self.sema.clone(),
            self.clock.clone(),
        );
        let meta_service = MetaSocketService::new(
            meta_socket,
            self.configuration.meta_socket_mode,
            self.sema.clone(),
        );
        let meta_listener = meta_service.listen()?;
        let ordinary_listener = ordinary_service.listen()?;
        let ordinary_thread =
            thread::spawn(move || ordinary_service.serve_listener(ordinary_listener));
        let meta_thread = thread::spawn(move || meta_service.serve_listener(meta_listener));
        ordinary_thread
            .join()
            .map_err(|_| Error::argument("ordinary socket thread panicked"))??;
        meta_thread
            .join()
            .map_err(|_| Error::argument("meta socket thread panicked"))??;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OrdinarySocketService {
    socket_path: PathBuf,
    socket_mode: SocketMode,
    sema: Arc<Mutex<SemaPlane>>,
    clock: CollectionClock,
}

impl OrdinarySocketService {
    pub fn new(
        socket_path: PathBuf,
        socket_mode: SocketMode,
        sema: Arc<Mutex<SemaPlane>>,
        clock: CollectionClock,
    ) -> Self {
        Self {
            socket_path,
            socket_mode,
            sema,
            clock,
        }
    }

    pub fn listen(&self) -> Result<UnixListener> {
        PrototypeSocket::new(self.socket_path.clone(), self.socket_mode).listen()
    }

    pub fn serve(&self) -> Result<()> {
        let listener = self.listen()?;
        self.serve_listener(listener)
    }

    pub fn serve_listener(&self, listener: UnixListener) -> Result<()> {
        for stream in listener.incoming() {
            let stream =
                stream.map_err(|error| Error::io("accepting ordinary connection", error))?;
            let _connection_result = self.handle_stream(stream);
        }
        Ok(())
    }

    pub fn handle_stream(&self, stream: UnixStream) -> Result<()> {
        let mut exchange = SocketExchange::new(stream);
        let request_bytes = exchange.read_bytes()?;
        let frame = match AggregatorFrame::decode_length_prefixed(&request_bytes) {
            Ok(frame) => frame,
            Err(_) => return Ok(()),
        };
        let reply_frame =
            OrdinarySocketFrame::new(frame, self.sema.clone(), self.clock.clone()).reply_frame()?;
        exchange.write_bytes(
            &reply_frame
                .encode_length_prefixed()
                .map_err(|error| Error::frame("ordinary reply encode", error))?,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MetaSocketService {
    socket_path: PathBuf,
    socket_mode: SocketMode,
    sema: Arc<Mutex<SemaPlane>>,
}

impl MetaSocketService {
    pub fn new(socket_path: PathBuf, socket_mode: SocketMode, sema: Arc<Mutex<SemaPlane>>) -> Self {
        Self {
            socket_path,
            socket_mode,
            sema,
        }
    }

    pub fn listen(&self) -> Result<UnixListener> {
        PrototypeSocket::new(self.socket_path.clone(), self.socket_mode).listen()
    }

    pub fn serve(&self) -> Result<()> {
        let listener = self.listen()?;
        self.serve_listener(listener)
    }

    pub fn serve_listener(&self, listener: UnixListener) -> Result<()> {
        for stream in listener.incoming() {
            let stream = stream.map_err(|error| Error::io("accepting meta connection", error))?;
            let _connection_result = self.handle_stream(stream);
        }
        Ok(())
    }

    pub fn handle_stream(&self, stream: UnixStream) -> Result<()> {
        let mut exchange = SocketExchange::new(stream);
        let request_bytes = exchange.read_bytes()?;
        let frame = match MetaAggregatorFrame::decode_length_prefixed(&request_bytes) {
            Ok(frame) => frame,
            Err(_) => return Ok(()),
        };
        let reply_frame = MetaSocketFrame::new(frame, self.sema.clone()).reply_frame()?;
        exchange.write_bytes(
            &reply_frame
                .encode_length_prefixed()
                .map_err(|error| Error::frame("meta reply encode", error))?,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrototypeSocket {
    path: PathBuf,
    mode: SocketMode,
}

impl PrototypeSocket {
    pub fn new(path: PathBuf, mode: SocketMode) -> Self {
        Self { path, mode }
    }

    pub fn listen(&self) -> Result<UnixListener> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::io("creating socket directory", error))?;
        }
        self.remove_stale_socket()?;
        let listener = UnixListener::bind(&self.path)
            .map_err(|error| Error::io("binding unix socket", error))?;
        self.apply_mode()?;
        Ok(listener)
    }

    pub fn remove_stale_socket(&self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }
        let file_type = std::fs::symlink_metadata(&self.path)
            .map_err(|error| Error::io("reading existing socket path metadata", error))?
            .file_type();
        if file_type.is_socket() {
            std::fs::remove_file(&self.path)
                .map_err(|error| Error::io("removing stale socket", error))?;
            Ok(())
        } else {
            Err(Error::startup_configuration(format!(
                "configured socket path {} already exists and is not a Unix socket",
                self.path.display()
            )))
        }
    }

    pub fn apply_mode(&self) -> Result<()> {
        let mode = self.mode.into_u32();
        if mode > 0o777 {
            return Err(Error::startup_configuration(format!(
                "configured socket mode {mode:#o} is outside permission bits"
            )));
        }
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(mode))
            .map_err(|error| Error::io("setting unix socket mode", error))
    }
}

#[derive(Debug)]
pub struct SocketExchange {
    stream: UnixStream,
}

impl SocketExchange {
    pub fn new(stream: UnixStream) -> Self {
        Self { stream }
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.stream
            .read_to_end(&mut bytes)
            .map_err(|error| Error::io("reading socket request", error))?;
        Ok(bytes)
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream
            .write_all(bytes)
            .map_err(|error| Error::io("writing socket reply", error))
    }
}

#[derive(Debug, Clone)]
pub struct OrdinarySocketFrame {
    frame: AggregatorFrame,
    sema: Arc<Mutex<SemaPlane>>,
    clock: CollectionClock,
}

impl OrdinarySocketFrame {
    pub fn new(
        frame: AggregatorFrame,
        sema: Arc<Mutex<SemaPlane>>,
        clock: CollectionClock,
    ) -> Self {
        Self { frame, sema, clock }
    }

    pub fn reply_frame(self) -> Result<AggregatorFrame> {
        match self.frame.into_body() {
            AggregatorFrameBody::Request { exchange, request } => {
                let handler = OrdinaryRequestHandler::new(self.sema, self.clock);
                let replies = request
                    .payloads
                    .into_iter()
                    .map(|request| SubReply::Ok(handler.handle(request)))
                    .collect::<Vec<_>>();
                let per_operation = NonEmpty::try_from_vec(replies).map_err(|error| {
                    Error::protocol("ordinary request shape", error.to_string())
                })?;
                Ok(AggregatorFrame::new(AggregatorFrameBody::Reply {
                    exchange,
                    reply: FrameReply::committed(per_operation),
                }))
            }
            AggregatorFrameBody::Reply { exchange, .. } => {
                Ok(AggregatorFrame::new(AggregatorFrameBody::Reply {
                    exchange,
                    reply: FrameReply::rejected(RequestRejectionReason::Internal),
                }))
            }
            other => Err(Error::protocol(
                "ordinary request shape",
                format!("expected request frame, got {other:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetaSocketFrame {
    frame: MetaAggregatorFrame,
    sema: Arc<Mutex<SemaPlane>>,
}

impl MetaSocketFrame {
    pub fn new(frame: MetaAggregatorFrame, sema: Arc<Mutex<SemaPlane>>) -> Self {
        Self { frame, sema }
    }

    pub fn reply_frame(self) -> Result<MetaAggregatorFrame> {
        match self.frame.into_body() {
            MetaAggregatorFrameBody::Request { exchange, request } => {
                let handler = MetaRequestHandler::new(self.sema);
                let replies = request
                    .payloads
                    .into_iter()
                    .map(|request| SubReply::Ok(handler.handle(request)))
                    .collect::<Vec<_>>();
                let per_operation = NonEmpty::try_from_vec(replies)
                    .map_err(|error| Error::protocol("meta request shape", error.to_string()))?;
                Ok(MetaAggregatorFrame::new(MetaAggregatorFrameBody::Reply {
                    exchange,
                    reply: FrameReply::committed(per_operation),
                }))
            }
            MetaAggregatorFrameBody::Reply { exchange, .. } => {
                Ok(MetaAggregatorFrame::new(MetaAggregatorFrameBody::Reply {
                    exchange,
                    reply: FrameReply::rejected(RequestRejectionReason::Internal),
                }))
            }
            other => Err(Error::protocol(
                "meta request shape",
                format!("expected request frame, got {other:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrdinaryRequestHandler {
    sema: Arc<Mutex<SemaPlane>>,
    clock: CollectionClock,
    signal: SignalPlane,
}

impl OrdinaryRequestHandler {
    pub fn new(sema: Arc<Mutex<SemaPlane>>, clock: CollectionClock) -> Self {
        Self {
            sema,
            clock,
            signal: SignalPlane,
        }
    }

    pub fn handle(&self, request: AggregatorRequest) -> AggregatorReply {
        match request {
            AggregatorRequest::Version(_) => self.signal.version_report(),
            AggregatorRequest::Collect(request) => {
                if let Some(rejection) = self.signal.collect_rejection(&request) {
                    return rejection;
                }
                let configuration = match self
                    .sema
                    .lock()
                    .ok()
                    .and_then(|sema| sema.active_configuration())
                {
                    Some(configuration) => configuration,
                    None => {
                        return self.signal.reject_collect(
                            request.request_identifier,
                            RejectionReason::ConfigurationUnavailable,
                        );
                    }
                };
                let runtime_configuration =
                    match RuntimeConfiguration::validate_from_meta(&configuration) {
                        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
                        RuntimeConfigurationValidation::Rejected(_) => {
                            return self.signal.reject_collect(
                                request.request_identifier,
                                RejectionReason::ConfigurationUnavailable,
                            );
                        }
                    };
                let request_identifier = request.request_identifier.clone();
                match NexusPlane::with_runtime_configuration(
                    runtime_configuration,
                    self.clock.clone(),
                )
                .collect(request)
                {
                    Ok(package) => AggregatorReply::EvidenceCollected(package),
                    Err(_) => self
                        .signal
                        .reject_collect(request_identifier, RejectionReason::CollectionUnavailable),
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetaRequestHandler {
    sema: Arc<Mutex<SemaPlane>>,
}

impl MetaRequestHandler {
    pub fn new(sema: Arc<Mutex<SemaPlane>>) -> Self {
        Self { sema }
    }

    pub fn handle(&self, request: MetaAggregatorRequest) -> MetaAggregatorReply {
        let Ok(mut sema) = self.sema.lock() else {
            return MetaAggregatorReply::ConfigurationRejected(
                meta_signal_aggregator::ConfigurationRejected {
                    operation: MetaAggregatorOperationKind::ObserveConfiguration,
                    reason: meta_signal_aggregator::ConfigurationRejectionReason::StoreUnavailable,
                },
            );
        };
        match request {
            MetaAggregatorRequest::Configure(change) => sema.configure(change),
            MetaAggregatorRequest::ObserveConfiguration(_) => sema.observe_configuration(),
            MetaAggregatorRequest::ValidateConfiguration(candidate) => {
                sema.validate_candidate(candidate)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketPath<'a> {
    path: &'a Path,
}

impl<'a> SocketPath<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        self.path
    }
}
