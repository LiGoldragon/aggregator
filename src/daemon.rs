use std::{
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use meta_signal_aggregator::{
    OperationKind as MetaOperationKind, Query as MetaQuery, Response as MetaResponse, SocketMode,
};
use signal_aggregator::{
    OperationKind, OperationRejectionReason, Query, RejectionReason, Response,
};

use crate::{
    CollectionClock, ConfigurationStore, Error, NexusPlane, Result, RuntimeConfiguration,
    RuntimeConfigurationValidation, SemaPlane, SignalPlane, wire::SignalFrame,
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

    pub fn handle_stream(&self, mut stream: UnixStream) -> Result<()> {
        let Ok(query) = SignalFrame::read::<Query>("ordinary query", &mut stream) else {
            return Ok(());
        };
        let response =
            OrdinaryRequestHandler::new(self.sema.clone(), self.clock.clone()).handle(query);
        SignalFrame::write("ordinary response", &mut stream, &response)
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

    pub fn handle_stream(&self, mut stream: UnixStream) -> Result<()> {
        let Ok(query) = SignalFrame::read::<MetaQuery>("meta query", &mut stream) else {
            return Ok(());
        };
        let response = MetaRequestHandler::new(self.sema.clone()).handle(query);
        SignalFrame::write("meta response", &mut stream, &response)
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
        let mode = u32::try_from(self.mode).map_err(|_| {
            Error::startup_configuration(format!(
                "configured socket mode {} is not a permission value",
                self.mode
            ))
        })?;
        if mode > 0o777 {
            return Err(Error::startup_configuration(format!(
                "configured socket mode {mode:#o} is outside permission bits"
            )));
        }
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(mode))
            .map_err(|error| Error::io("setting unix socket mode", error))
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

    pub fn handle(&self, request: Query) -> Response {
        match request {
            Query::Version(_) => self.signal.version_report(),
            Query::ObserveHealth(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::ObserveHealth)
                {
                    Ok(nexus) => match nexus.observe_health(request) {
                        Ok(reply) => Response::RuntimeHealthObserved(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::Collect(request) => self.handle_collect(request),
            Query::InventorySessions(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::InventorySessions,
                ) {
                    Ok(nexus) => match nexus.inventory_sessions(request) {
                        Ok(reply) => Response::SessionsInventoried(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::LookupSession(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::LookupSession)
                {
                    Ok(nexus) => match nexus.lookup_session(request) {
                        Ok(reply) => Response::SessionLookedUp(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::WriteSessionArchive(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::WriteSessionArchive,
                ) {
                    Ok(nexus) => match nexus.write_session_archive(request) {
                        Ok(reply) => Response::SessionArchiveWritten(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::QuerySessionArchive(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::QuerySessionArchive,
                ) {
                    Ok(nexus) => match nexus.query_session_archive(request) {
                        Ok(reply) => Response::SessionArchiveQueried(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ReadSessionArchive(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::ReadSessionArchive,
                ) {
                    Ok(nexus) => match nexus.read_session_archive(request) {
                        Ok(reply) => Response::SessionArchiveRead(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ListSessions(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::ListSessions)
                {
                    Ok(nexus) => match nexus.list_sessions(request) {
                        Ok(reply) => Response::SessionsListed(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ListSubagents(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::ListSubagents)
                {
                    Ok(nexus) => match nexus.list_subagents(request) {
                        Ok(reply) => Response::SubagentsListed(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ListOutputs(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::ListOutputs)
                {
                    Ok(nexus) => match nexus.list_outputs(request) {
                        Ok(reply) => Response::OutputsListed(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ListOutputSegments(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::ListOutputSegments,
                ) {
                    Ok(nexus) => match nexus.list_output_segments(request) {
                        Ok(reply) => Response::OutputSegmentsListed(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::EstimateOutput(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::EstimateOutput)
                {
                    Ok(nexus) => match nexus.estimate_output(request) {
                        Ok(reply) => Response::OutputEstimated(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ReadOutput(request) => {
                match self
                    .nexus_for_operation(&request.request_identifier, OperationKind::ReadOutput)
                {
                    Ok(nexus) => match nexus.read_output(request) {
                        Ok(reply) => Response::OutputRead(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ListTranscriptBlocks(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::ListTranscriptBlocks,
                ) {
                    Ok(nexus) => match nexus.list_transcript_blocks(request) {
                        Ok(reply) => Response::TranscriptBlocksListed(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::SearchTranscriptBlocks(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::SearchTranscriptBlocks,
                ) {
                    Ok(nexus) => match nexus.search_transcript_blocks(request) {
                        Ok(reply) => Response::TranscriptBlocksSearched(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::EstimateTranscriptBlock(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::EstimateTranscriptBlock,
                ) {
                    Ok(nexus) => match nexus.estimate_transcript_block(request) {
                        Ok(reply) => Response::TranscriptBlockEstimated(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
            Query::ReadTranscriptBlock(request) => {
                match self.nexus_for_operation(
                    &request.request_identifier,
                    OperationKind::ReadTranscriptBlock,
                ) {
                    Ok(nexus) => match nexus.read_transcript_block(request) {
                        Ok(reply) => Response::TranscriptBlockRead(reply),
                        Err(rejection) => Response::OperationRejected(rejection),
                    },
                    Err(rejection) => Response::OperationRejected(rejection),
                }
            }
        }
    }

    pub fn handle_collect(&self, request: signal_aggregator::EvidenceRequest) -> Response {
        if let Some(rejection) = self.signal.collect_rejection(&request) {
            return rejection;
        }
        let runtime_configuration = match self.runtime_configuration() {
            Some(configuration) => configuration,
            None => {
                return self.signal.reject_collect(
                    request.request_identifier,
                    RejectionReason::ConfigurationUnavailable,
                );
            }
        };
        let request_identifier = request.request_identifier.clone();
        match NexusPlane::with_runtime_configuration(runtime_configuration, self.clock.clone())
            .collect(request)
        {
            Ok(package) => Response::EvidenceCollected(package),
            Err(_) => self
                .signal
                .reject_collect(request_identifier, RejectionReason::CollectionUnavailable),
        }
    }

    pub fn nexus_for_operation(
        &self,
        request_identifier: &signal_aggregator::RequestIdentifier,
        operation: OperationKind,
    ) -> std::result::Result<NexusPlane, signal_aggregator::OperationRejected> {
        let Some(runtime_configuration) = self.runtime_configuration() else {
            return Err(signal_aggregator::OperationRejected {
                request_identifier: request_identifier.clone(),
                operation_kind: operation,
                operation_rejection_reason: OperationRejectionReason::Unsupported,
                rejected_fragile_reference_option: None,
            });
        };
        Ok(NexusPlane::with_runtime_configuration(
            runtime_configuration,
            self.clock.clone(),
        ))
    }

    pub fn runtime_configuration(&self) -> Option<RuntimeConfiguration> {
        let configuration = self
            .sema
            .lock()
            .ok()
            .and_then(|sema| sema.active_configuration())?;
        match RuntimeConfiguration::validate_from_meta(&configuration) {
            RuntimeConfigurationValidation::Accepted(configuration) => Some(configuration),
            RuntimeConfigurationValidation::Rejected(_) => None,
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

    pub fn handle(&self, query: MetaQuery) -> MetaResponse {
        let Ok(mut sema) = self.sema.lock() else {
            return MetaResponse::ConfigurationRejected(
                meta_signal_aggregator::ConfigurationRejected {
                    operation_kind: MetaOperationKind::ObserveConfiguration,
                    configuration_rejection_reason:
                        meta_signal_aggregator::ConfigurationRejectionReason::StoreUnavailable,
                },
            );
        };
        match query {
            MetaQuery::Configure(change) => sema.configure(change),
            MetaQuery::ObserveConfiguration(_) => sema.observe_configuration(),
            MetaQuery::ValidateConfiguration(candidate) => sema.validate_candidate(candidate),
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
