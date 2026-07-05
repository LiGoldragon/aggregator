use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use meta_signal_aggregator::{
    MetaAggregatorOperationKind, MetaAggregatorReply, MetaAggregatorRequest,
};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::{AggregatorReply, AggregatorRequest, RejectionReason};

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
        let sema_store =
            ConfigurationStore::at_path(PathBuf::from(configuration.store_path.as_str()));
        let sema = Arc::new(Mutex::new(SemaPlane::with_configuration_store(
            configuration.clone(),
            sema_store,
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
        let ordinary_service =
            OrdinarySocketService::new(ordinary_socket, self.sema.clone(), self.clock.clone());
        let meta_service = MetaSocketService::new(meta_socket, self.sema.clone());
        let ordinary_thread = thread::spawn(move || ordinary_service.serve());
        let meta_thread = thread::spawn(move || meta_service.serve());
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
    sema: Arc<Mutex<SemaPlane>>,
    clock: CollectionClock,
}

impl OrdinarySocketService {
    pub fn new(socket_path: PathBuf, sema: Arc<Mutex<SemaPlane>>, clock: CollectionClock) -> Self {
        Self {
            socket_path,
            sema,
            clock,
        }
    }

    pub fn serve(&self) -> Result<()> {
        let listener = PrototypeSocket::new(self.socket_path.clone()).listen()?;
        for stream in listener.incoming() {
            let stream =
                stream.map_err(|error| Error::io("accepting ordinary connection", error))?;
            self.handle_stream(stream)?;
        }
        Ok(())
    }

    pub fn handle_stream(&self, stream: UnixStream) -> Result<()> {
        let mut exchange = SocketExchange::new(stream);
        let request_text = exchange.read_text()?;
        let request = NotaSource::new(&request_text)
            .parse::<AggregatorRequest>()
            .map_err(|error| Error::nota("ordinary request decode", error.to_string()))?;
        let reply =
            OrdinaryRequestHandler::new(self.sema.clone(), self.clock.clone()).handle(request);
        exchange.write_text(&reply.to_nota())
    }
}

#[derive(Debug, Clone)]
pub struct MetaSocketService {
    socket_path: PathBuf,
    sema: Arc<Mutex<SemaPlane>>,
}

impl MetaSocketService {
    pub fn new(socket_path: PathBuf, sema: Arc<Mutex<SemaPlane>>) -> Self {
        Self { socket_path, sema }
    }

    pub fn serve(&self) -> Result<()> {
        let listener = PrototypeSocket::new(self.socket_path.clone()).listen()?;
        for stream in listener.incoming() {
            let stream = stream.map_err(|error| Error::io("accepting meta connection", error))?;
            self.handle_stream(stream)?;
        }
        Ok(())
    }

    pub fn handle_stream(&self, stream: UnixStream) -> Result<()> {
        let mut exchange = SocketExchange::new(stream);
        let request_text = exchange.read_text()?;
        let request = NotaSource::new(&request_text)
            .parse::<MetaAggregatorRequest>()
            .map_err(|error| Error::nota("meta request decode", error.to_string()))?;
        let reply = MetaRequestHandler::new(self.sema.clone()).handle(request);
        exchange.write_text(&reply.to_nota())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrototypeSocket {
    path: PathBuf,
}

impl PrototypeSocket {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn listen(&self) -> Result<UnixListener> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| Error::io("creating socket directory", error))?;
        }
        if self.path.exists() {
            std::fs::remove_file(&self.path)
                .map_err(|error| Error::io("removing stale socket", error))?;
        }
        UnixListener::bind(&self.path).map_err(|error| Error::io("binding unix socket", error))
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

    pub fn read_text(&mut self) -> Result<String> {
        let mut text = String::new();
        self.stream
            .read_to_string(&mut text)
            .map_err(|error| Error::io("reading socket request", error))?;
        Ok(text)
    }

    pub fn write_text(&mut self, text: &str) -> Result<()> {
        self.stream
            .write_all(text.as_bytes())
            .map_err(|error| Error::io("writing socket reply", error))
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
