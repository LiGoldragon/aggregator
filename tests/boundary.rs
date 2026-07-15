use std::{
    fs,
    io::Write,
    net::Shutdown,
    os::unix::{
        fs::{PermissionsExt, symlink},
        net::UnixStream,
    },
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use aggregator::{
    AdapterKind, CollectionClock, ConfigurationFixture, ConfigurationStore, Error, NexusPlane,
    ReferenceTime, RepositoryAdapterConfiguration, RuntimeConfiguration,
    RuntimeConfigurationValidation, SemaPlane, SignalPlane, TranscriptAdapterConfiguration,
    TranscriptRootConfiguration,
    adapter::{
        MaximumDiscoveredFiles, MaximumFileBytes, MaximumLineBytes, MaximumReadFailures,
        MaximumScanEntries, TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord,
        TranscriptRecordSink, TranscriptScanLimitConfiguration, TranscriptScanLimits,
        claude::{ClaudeJsonlRootReader, ClaudeTranscriptAdapter},
        codex::{CodexSessionRootReader, CodexTranscriptAdapter},
        pi::{PiRunHistoryRootReader, PiTranscriptAdapter},
        repository::{
            RepositoryAdapter, RepositoryChangeFixture, RepositoryCommandPolicy,
            RepositoryEvidenceFixture,
        },
    },
    configuration::LegacyAggregatorConfiguration,
    daemon::{PrototypeDaemon, PrototypeSocket},
    output_index::{
        IndexSnapshot, SourceHealthObserver, limits::IndexStoreLimits, store::IndexStore,
    },
};
use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange,
    ConfigurationObservation, FilesystemPath, LegacyRecoveryAccess, LegacyRecoveryRoot,
    LegacyRecoverySource, MetaAggregatorReply, MetaAggregatorRequest, ObserveConfiguration,
    OutputInterfaceConfiguration, OutputInterfaceLimitPolicy, RepositoryName, SocketMode,
    TranscriptRoot, TranscriptSource,
};
use nota::{NotaEncode, NotaSource};
use nota_text_query::{QueryTerm, WordDistance};
use signal_aggregator::{
    AggregatorReply, AggregatorRequest, ArchivePath, ArchiveProvenanceText, ArchiveSummaryText,
    ArchiveTextCompleteness, AuthoredStatus, AuthoredStatusFilter, BoundedTextProjection,
    ByteCount, ByteLimit, ByteRange, CardProjection, ContractName, DurationAmount, DurationUnit,
    EvidenceRequest, FilesystemPath as SignalFilesystemPath, FragileOutputReference,
    FragileTranscriptBlockReference, ItemCount, LimitPolicy, ListingOrder,
    OperationRejectionReason, OutputListFilter, OutputListRequest, OutputReadRange,
    OutputReadRequest, OutputSegmentListFilter, OutputSegmentListRequest, PageLimit, PageRequest,
    Projection, ReadFailureReason, RejectionReason, RelativeDuration, RepositoryIdentifier,
    RepositoryPath, RepositoryWorktreeState, RequestIdentifier, RuntimeHealthRequest,
    ScanLimitKind, SegmentLimit, SegmentProjection, SelectedSources, SessionArchiveQueryRequest,
    SessionArchiveReadRequest, SessionArchiveRecordDraft, SessionArchiveStatus,
    SessionArchiveWriteRequest, SessionInventoryCompleteness, SessionInventoryRequest,
    SessionLifecycleStatus, SessionListFilter, SessionListRequest, SessionLookupRequest,
    SessionLookupSelector, SourceHealthStatus, SourceIdentifier, SourceKind, SourceSelection,
    SubagentListFilter, SubagentListRequest, TextQuery, TimeRange, TimeWindow, Timestamp,
    TranscriptBlockEstimateRequest, TranscriptBlockFilter, TranscriptBlockKind,
    TranscriptBlockKindSelection, TranscriptBlockListRequest, TranscriptBlockReadRequest,
    TranscriptBlockSearchRequest, TranscriptBlockTextAvailability, TranscriptBlockTextQuery,
    TruncationReason, Version,
};
use tempfile::TempDir;

fn evidence_request() -> EvidenceRequest {
    EvidenceRequest {
        request_identifier: RequestIdentifier::new("req-test"),
        time_window: TimeWindow::Recent(RelativeDuration {
            amount: DurationAmount::new(1),
            unit: DurationUnit::Hours,
        }),
        source_selection: SourceSelection::AllConfigured,
        projection: Projection::MetadataOnly,
        limit_policy: LimitPolicy {
            maximum_segments: SegmentLimit::new(8),
            maximum_bytes: ByteLimit::new(1024),
        },
    }
}

#[derive(Debug, Clone, Copy)]
struct ExampleNotaFile {
    name: &'static str,
    text: &'static str,
}

impl ExampleNotaFile {
    fn new(name: &'static str, text: &'static str) -> Self {
        Self { name, text }
    }

    fn parse_as_requests(&self) {
        self.for_each_non_empty_line(|line_number, line| {
            NotaSource::new(line)
                .parse::<AggregatorRequest>()
                .unwrap_or_else(|error| {
                    panic!(
                        "{}:{} must parse as AggregatorRequest: {}",
                        self.name, line_number, error
                    )
                });
        });
    }

    fn parse_as_replies(&self) {
        self.for_each_non_empty_line(|line_number, line| {
            NotaSource::new(line)
                .parse::<AggregatorReply>()
                .unwrap_or_else(|error| {
                    panic!(
                        "{}:{} must parse as AggregatorReply: {}",
                        self.name, line_number, error
                    )
                });
        });
    }

    fn parse_as_configuration(&self) {
        NotaSource::new(self.text.trim())
            .parse::<AggregatorConfiguration>()
            .unwrap_or_else(|error| {
                panic!(
                    "{} must parse as AggregatorConfiguration: {}",
                    self.name, error
                )
            });
    }

    fn for_each_non_empty_line(&self, mut parse_line: impl FnMut(usize, &str)) {
        for (line_index, line) in self.text.lines().enumerate() {
            let line = line.trim();
            if !line.is_empty() {
                parse_line(line_index + 1, line);
            }
        }
    }
}

#[test]
fn example_nota_files_match_contract_shapes() {
    ExampleNotaFile::new(
        "examples/collect.nota",
        include_str!("../examples/collect.nota"),
    )
    .parse_as_requests();
    ExampleNotaFile::new(
        "examples/output-interface-requests.nota",
        include_str!("../examples/output-interface-requests.nota"),
    )
    .parse_as_requests();
    ExampleNotaFile::new(
        "examples/output-interface-replies.nota",
        include_str!("../examples/output-interface-replies.nota"),
    )
    .parse_as_replies();
    ExampleNotaFile::new(
        "examples/session-inventory-archive-requests.nota",
        include_str!("../examples/session-inventory-archive-requests.nota"),
    )
    .parse_as_requests();
    ExampleNotaFile::new(
        "examples/transcript-block-search-requests.nota",
        include_str!("../examples/transcript-block-search-requests.nota"),
    )
    .parse_as_requests();
    ExampleNotaFile::new(
        "examples/transcript-block-search-replies.nota",
        include_str!("../examples/transcript-block-search-replies.nota"),
    )
    .parse_as_replies();
    ExampleNotaFile::new(
        "examples/configuration.nota",
        include_str!("../examples/configuration.nota"),
    )
    .parse_as_configuration();
}

fn read_request(
    time_window: TimeWindow,
    projection: Projection,
    maximum_segments: u64,
) -> TranscriptReadRequest {
    read_request_with_byte_limit(time_window, projection, maximum_segments, 1024)
}

fn read_request_with_byte_limit(
    time_window: TimeWindow,
    projection: Projection,
    maximum_segments: u64,
    maximum_bytes: u64,
) -> TranscriptReadRequest {
    TranscriptReadRequest::new(
        time_window,
        projection,
        LimitPolicy {
            maximum_segments: SegmentLimit::new(maximum_segments),
            maximum_bytes: ByteLimit::new(maximum_bytes),
        },
    )
}

fn small_discovery_limits(maximum_discovered_files: u64) -> TranscriptScanLimits {
    TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
        MaximumScanEntries::new(16),
        MaximumDiscoveredFiles::new(maximum_discovered_files),
        MaximumFileBytes::new(4096),
        MaximumLineBytes::new(1024),
        MaximumReadFailures::new(8),
    ))
}

fn transcript_block_filter(kind_selection: TranscriptBlockKindSelection) -> TranscriptBlockFilter {
    TranscriptBlockFilter {
        source_selection: SourceSelection::AllConfigured,
        session_reference: None,
        subagent_reference: None,
        task_identifier: None,
        kind_selection,
        authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
        time_window: None,
    }
}

fn typed_index_store(store_path: &std::path::Path) -> IndexStore {
    IndexStore::new(
        std::path::PathBuf::from(format!("{}.output-index.json", store_path.display())),
        IndexStoreLimits::default(),
    )
}

fn all_transcript_block_filter() -> TranscriptBlockFilter {
    transcript_block_filter(TranscriptBlockKindSelection::AllTranscriptBlockKinds)
}

fn only_transcript_block_filter(kind: TranscriptBlockKind) -> TranscriptBlockFilter {
    transcript_block_filter(TranscriptBlockKindSelection::OnlyTranscriptBlockKinds(
        signal_aggregator::SelectedTranscriptBlockKinds { kinds: vec![kind] },
    ))
}

fn accepted_configuration(root: &TempDir) -> AggregatorConfiguration {
    let repository = root.path().join("repository");
    let claude = root.path().join("claude");
    let codex = root.path().join("codex");
    fs::create_dir_all(&repository).expect("repository directory");
    fs::create_dir_all(&claude).expect("claude directory");
    fs::create_dir_all(&codex).expect("codex directory");
    AggregatorConfiguration {
        ordinary_socket_path: FilesystemPath::new(
            root.path().join("ordinary.sock").display().to_string(),
        ),
        ordinary_socket_mode: SocketMode::new(0o660),
        meta_socket_path: FilesystemPath::new(root.path().join("meta.sock").display().to_string()),
        meta_socket_mode: SocketMode::new(0o600),
        store_path: FilesystemPath::new(root.path().join("store.sema").display().to_string()),
        active_repositories: vec![ActiveRepository {
            name: RepositoryName::new("fixture-repository"),
            path: FilesystemPath::new(repository.display().to_string()),
        }],
        transcript_sources: vec![
            TranscriptSource::Claude(TranscriptRoot {
                path: FilesystemPath::new(claude.display().to_string()),
            }),
            TranscriptSource::Codex(TranscriptRoot {
                path: FilesystemPath::new(codex.display().to_string()),
            }),
        ],
        default_projection: Projection::MetadataOnly,
        default_limit_policy: LimitPolicy {
            maximum_segments: SegmentLimit::new(16),
            maximum_bytes: ByteLimit::new(4096),
        },
        output_interfaces: OutputInterfaceConfiguration::default(),
    }
}

fn run_binary_with_input(
    binary: &str,
    configuration_path: &std::path::Path,
    input: &str,
) -> String {
    let mut child = Command::new(binary)
        .arg("--configuration")
        .arg(configuration_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn binary");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write stdin");
    let output = child.wait_with_output().expect("wait for binary");
    assert!(
        output.status.success(),
        "binary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

struct DaemonGuard {
    child: Child,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl DaemonGuard {
    fn start(configuration_path: &std::path::Path, reference_timestamp: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_aggregator-daemon"))
            .arg("--configuration")
            .arg(configuration_path)
            .env("AGGREGATOR_REFERENCE_TIMESTAMP", reference_timestamp)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn daemon");
        Self { child }
    }
}

fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..100 {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("socket did not appear: {}", path.display());
}

fn assert_socket_mode(path: &std::path::Path, expected_mode: u32) {
    let mode = fs::metadata(path)
        .expect("socket metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, expected_mode, "socket mode for {}", path.display());
}

fn send_malformed_socket_bytes(path: &std::path::Path) {
    let mut stream = UnixStream::connect(path).expect("connect raw socket");
    stream
        .write_all(b"(Version (client_name None))")
        .expect("write malformed socket request");
    stream
        .shutdown(Shutdown::Write)
        .expect("shutdown malformed socket request");
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn adapter_skeletons_name_the_approved_sources() {
    let root = TranscriptRootConfiguration::new(std::env::temp_dir());
    let repository =
        RepositoryAdapterConfiguration::new(RepositoryName::new("primary"), std::env::temp_dir());
    assert_eq!(
        ClaudeTranscriptAdapter::new(root.clone()).kind(),
        AdapterKind::ClaudeTranscript
    );
    assert_eq!(
        CodexTranscriptAdapter::new(root.clone()).kind(),
        AdapterKind::CodexTranscript
    );
    assert_eq!(
        PiTranscriptAdapter::new(root).kind(),
        AdapterKind::PiTranscript
    );
    assert_eq!(
        RepositoryAdapter::command_policy(
            vec![repository],
            RepositoryCommandPolicy::unavailable(),
        )
        .kind(),
        AdapterKind::Repository
    );
}

#[test]
fn signal_plane_returns_typed_rejection_without_synthesis() {
    let reply = SignalPlane.reject_collect(
        RequestIdentifier::new("req-test"),
        RejectionReason::CollectionUnavailable,
    );
    let text = reply.to_nota();
    assert!(matches!(reply, AggregatorReply::EvidenceRejected(_)));
    for forbidden in ["Summary", "Review", "Recommendation", "Score", "Judgment"] {
        assert!(!text.contains(forbidden));
    }
}

#[test]
fn nexus_scaffold_does_not_collect_private_sources() {
    let nexus = NexusPlane::with_adapters(vec![AdapterKind::ClaudeTranscript]);
    let error = nexus
        .collect(evidence_request())
        .expect_err("scaffold stops before daemon collection orchestration");
    assert!(error.to_string().contains("not implemented"));
}

#[test]
fn configuration_fixture_round_trips_through_nota() {
    let configuration = ConfigurationFixture::minimal();
    let text = configuration.to_nota();
    let decoded = NotaSource::new(&text)
        .parse::<meta_signal_aggregator::AggregatorConfiguration>()
        .expect("decode configuration");
    assert_eq!(decoded, configuration);
}

#[test]
fn sema_scaffold_observes_configuration_and_rejects_configuration_without_store() {
    let root = TempDir::new().expect("temporary root");
    let mut sema = SemaPlane::empty();
    assert!(matches!(
        sema.observe_configuration(),
        MetaAggregatorReply::ConfigurationObserved(_)
    ));
    let configured = sema.configure(ConfigurationChange {
        configuration: accepted_configuration(&root),
    });
    assert!(matches!(
        configured,
        MetaAggregatorReply::ConfigurationRejected(rejection)
            if rejection.reason == meta_signal_aggregator::ConfigurationRejectionReason::StoreUnavailable
    ));
}

#[test]
fn sema_configure_persists_typed_configuration_when_store_is_available() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let store = ConfigurationStore::at_path(root.path().join("configuration.nota"));
    let mut sema = SemaPlane::with_configuration_store(configuration.clone(), store.clone());

    let configured = sema.configure(ConfigurationChange {
        configuration: configuration.clone(),
    });

    assert!(matches!(
        configured,
        MetaAggregatorReply::ConfigurationConfigured(_)
    ));
    assert_eq!(
        store.read_configuration().expect("persisted configuration"),
        configuration
    );
}

#[test]
fn configuration_store_round_trips_nota_file_storage() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let store = ConfigurationStore::at_path(root.path().join("configuration.nota"));
    store
        .write_configuration(&configuration)
        .expect("write configuration");
    let decoded = store.read_configuration().expect("read configuration");
    assert_eq!(decoded, configuration);
}

#[test]
fn prototype_socket_rejects_preexisting_regular_file() {
    let root = TempDir::new().expect("temporary root");
    let socket_path = root.path().join("ordinary.sock");
    fs::write(&socket_path, "not a socket").expect("write regular file");

    let error = PrototypeSocket::new(socket_path.clone(), SocketMode::new(0o660))
        .listen()
        .expect_err("regular file must not be removed as stale socket");

    assert!(matches!(error, Error::StartupConfiguration { .. }));
    assert!(socket_path.is_file());
}

#[test]
fn daemon_startup_rejects_meta_regular_file_before_serving_ordinary_socket() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let ordinary_socket_path =
        std::path::PathBuf::from(configuration.ordinary_socket_path.as_str());
    let meta_socket_path = std::path::PathBuf::from(configuration.meta_socket_path.as_str());
    fs::write(&meta_socket_path, "not a socket").expect("write regular file");
    let sema = Arc::new(Mutex::new(SemaPlane::with_configuration(
        configuration.clone(),
    )));
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );

    let error = PrototypeDaemon::new(configuration, sema, clock)
        .run()
        .expect_err("meta startup failure should return promptly");

    assert!(matches!(error, Error::StartupConfiguration { .. }));
    assert!(meta_socket_path.is_file());
    assert!(!ordinary_socket_path.exists());
}

#[test]
fn runtime_configuration_validates_paths_and_maps_source_selection() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let validation = RuntimeConfiguration::validate_from_meta(&configuration);
    let runtime = match validation {
        RuntimeConfigurationValidation::Accepted(runtime) => runtime,
        RuntimeConfigurationValidation::Rejected(report) => {
            panic!("unexpected rejection: {report:?}")
        }
    };
    assert_eq!(runtime.repositories().len(), 1);
    assert_eq!(runtime.transcript_sources().len(), 2);
    let selected = runtime.select_sources(&SourceSelection::Only(SelectedSources {
        sources: vec![SourceKind::Claude, SourceKind::Repository],
    }));
    assert_eq!(selected.repositories.len(), 1);
    assert_eq!(selected.transcript_sources.len(), 1);
    assert!(matches!(
        selected.transcript_sources.first(),
        Some(TranscriptAdapterConfiguration::Claude(_))
    ));
}

#[test]
fn runtime_configuration_reports_missing_paths_as_validation_issues() {
    let root = TempDir::new().expect("temporary root");
    let mut configuration = accepted_configuration(&root);
    configuration.transcript_sources = vec![TranscriptSource::Pi(TranscriptRoot {
        path: FilesystemPath::new(root.path().join("missing-pi").display().to_string()),
    })];
    let validation = RuntimeConfiguration::validate_from_meta(&configuration);
    let report = match validation {
        RuntimeConfigurationValidation::Accepted(_) => panic!("missing path was accepted"),
        RuntimeConfigurationValidation::Rejected(report) => report,
    };
    assert!(report.issues.iter().any(|issue| issue.kind
        == meta_signal_aggregator::ConfigurationValidationIssueKind::UnreadablePath));
}

#[test]
fn claude_jsonl_adapter_projects_bounded_text_and_reports_malformed_lines() {
    let root = TempDir::new().expect("temporary root");
    let transcript = root.path().join("project.jsonl");
    fs::write(
        &transcript,
        concat!(
            "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"text\":\"outside\",\"unknown\":1}\n",
            "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"hello world\"}]},\"ignored\":true}\n",
            "not-json\n",
            "{\"timestamp\":\"2026-01-02T01:00:00Z\",\"text\":\"second matching record\"}\n",
        ),
    )
    .expect("write fixture transcript");
    let adapter =
        ClaudeTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Range(TimeRange {
            start: Timestamp::new("2026-01-02T00:00:00Z"),
            end: Timestamp::new("2026-01-02T23:59:59Z"),
        }),
        Projection::BoundedText(BoundedTextProjection {
            maximum_bytes: ByteLimit::new(6),
        }),
        1,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::Malformed
    );
    assert!(!outcome.truncations.is_empty());
    match &outcome.transcript_segments[0].projection {
        SegmentProjection::Text(excerpt) => assert_eq!(excerpt.text.as_str(), "hello "),
        other => panic!("expected text projection, got {other:?}"),
    }
}

#[test]
fn transcript_reader_reports_file_limit_without_unbounded_read() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("too-large.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"this fixture exceeds the small runtime read cap\"}\n",
    )
    .expect("write oversized fixture transcript");
    let reader = ClaudeJsonlRootReader::with_limits(
        root.path().to_path_buf(),
        TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(16),
            MaximumDiscoveredFiles::new(16),
            MaximumFileBytes::new(32),
            MaximumLineBytes::new(1024),
            MaximumReadFailures::new(8),
        )),
    );
    let outcome = reader.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.truncations.len(), 1);
    let truncated_path = outcome.truncations[0]
        .path
        .as_ref()
        .map(|path| path.as_str());
    let expected_path = root.path().join("too-large.jsonl").display().to_string();
    assert_eq!(truncated_path, Some(expected_path.as_str()));
    assert!(
        outcome.truncations[0]
            .original_bytes
            .as_ref()
            .is_some_and(|count| count.into_u64() > 32)
    );
}

#[test]
fn transcript_reader_caps_file_discovery_line_size_and_failure_reports() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("first.jsonl"),
        concat!(
            "not-json\n",
            "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"this line is too large for the small test cap\"}\n",
            "also-not-json\n",
        ),
    )
    .expect("write first fixture transcript");
    fs::write(
        root.path().join("second.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"second\"}\n",
    )
    .expect("write second fixture transcript");
    let reader = ClaudeJsonlRootReader::with_limits(
        root.path().to_path_buf(),
        TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(16),
            MaximumDiscoveredFiles::new(1),
            MaximumFileBytes::new(4096),
            MaximumLineBytes::new(32),
            MaximumReadFailures::new(1),
        )),
    );
    let outcome = reader.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::Malformed
    );
    assert!(outcome.truncations.len() >= 3);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("second.jsonl"))
    }));
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("first.jsonl:2"))
    }));
}

#[derive(Debug, Default)]
struct StreamingRecordCounter {
    count: u64,
    maximum_record_bytes: u64,
}

impl TranscriptRecordSink for StreamingRecordCounter {
    fn observe_record(&mut self, record: TranscriptRecord) {
        self.count += 1;
        self.maximum_record_bytes = self.maximum_record_bytes.max(record.byte_count());
    }
}

#[test]
fn transcript_scanner_streams_records_without_retaining_source_records() {
    let root = TempDir::new().expect("temporary root");
    let payload = "x".repeat(32 * 1024);
    fs::write(
        root.path().join("session.jsonl"),
        format!(
            "{{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"{payload}\"}}\n{{\"timestamp\":\"2026-01-02T00:00:01Z\",\"text\":\"{payload}\"}}\n"
        ),
    )
    .expect("write synthetic transcript fixture");

    let mut counter = StreamingRecordCounter::default();
    let outcome = ClaudeJsonlRootReader::new(root.path().to_path_buf()).scan_records(&mut counter);

    assert_eq!(counter.count, 2);
    assert_eq!(outcome.record_count, 2);
    assert!(outcome.records.is_empty());
    assert_eq!(counter.maximum_record_bytes, 32 * 1024);
}

#[test]
fn transcript_discovery_file_limit_is_configurable_and_reported() {
    let root = TempDir::new().expect("temporary root");
    for index in 0..3 {
        fs::write(
            root.path().join(format!("{index}.jsonl")),
            format!(
                "{{\"timestamp\":\"2026-01-02T00:00:0{index}Z\",\"text\":\"record {index}\"}}\n"
            ),
        )
        .expect("write transcript fixture");
    }
    let limited = ClaudeJsonlRootReader::with_limits(
        root.path().to_path_buf(),
        TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(16),
            MaximumDiscoveredFiles::new(2),
            MaximumFileBytes::new(4096),
            MaximumLineBytes::new(1024),
            MaximumReadFailures::new(8),
        )),
    )
    .read_records();
    assert_eq!(limited.discovered_files, 2);
    assert!(limited.scan_limits.iter().any(|limit| {
        limit.kind == ScanLimitKind::DiscoveredFiles && limit.limit.into_u64() == 2
    }));

    let raised = ClaudeJsonlRootReader::with_limits(
        root.path().to_path_buf(),
        TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(16),
            MaximumDiscoveredFiles::new(4),
            MaximumFileBytes::new(4096),
            MaximumLineBytes::new(1024),
            MaximumReadFailures::new(8),
        )),
    )
    .read_records();
    assert_eq!(raised.discovered_files, 3);
    assert!(raised.scan_limits.is_empty());
}

#[test]
fn claude_reader_reports_symlinked_discovery_paths_that_escape_root_as_read_failure() {
    let root = TempDir::new().expect("temporary root");
    let outside_root = TempDir::new().expect("outside temporary root");
    let outside_directory = outside_root.path().join("outside-directory");
    fs::create_dir_all(&outside_directory).expect("outside directory");
    let outside_file = outside_root.path().join("outside-session.jsonl");
    fs::write(
        &outside_file,
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"text\":\"outside claude answer\"}\n",
    )
    .expect("write outside file");
    fs::write(
        outside_directory.join("nested.jsonl"),
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"text\":\"outside nested answer\"}\n",
    )
    .expect("write outside nested file");
    symlink(&outside_file, root.path().join("escape.jsonl")).expect("file symlink");
    symlink(&outside_directory, root.path().join("escape-directory")).expect("directory symlink");

    let outcome = ClaudeJsonlRootReader::new(root.path().to_path_buf()).collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(outcome.read_failures.iter().all(|failure| {
        failure.reason == ReadFailureReason::PermissionDenied
            && failure
                .path
                .as_ref()
                .is_some_and(|path| path.as_str().contains("escape"))
    }));
    assert!(outcome.transcript_segments.is_empty());
}

#[test]
fn claude_subagent_output_reader_follows_symlinked_output_files() {
    let root = TempDir::new().expect("temporary root");
    let outside_root = TempDir::new().expect("outside temporary root");
    let outside_file = outside_root.path().join("task.output");
    fs::write(
        &outside_file,
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"text\":\"outside subagent answer\"}\n",
    )
    .expect("write outside output");
    symlink(&outside_file, root.path().join("escape.output")).expect("output symlink");

    let outcome =
        ClaudeJsonlRootReader::subagent_output(root.path().to_path_buf()).collect(&read_request(
            TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
            Projection::MetadataOnly,
            8,
        ));

    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(
        outcome.transcript_segments[0]
            .path
            .as_str()
            .contains("escape.output")
    );
}

#[test]
fn pi_subagent_output_reader_uses_output_files() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("task.output"),
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"text\":\"pi tintinweb output\"}\n",
    )
    .expect("write output fixture");

    let outcome = ClaudeJsonlRootReader::with_limits_and_source(
        root.path().to_path_buf(),
        TranscriptScanLimits::default_runtime(),
        SourceKind::PiSubagentOutput,
    )
    .collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(
        outcome.transcript_segments[0].source,
        SourceKind::PiSubagentOutput
    );
}

#[test]
fn claude_output_extension_is_exclusive_to_subagent_output_roots() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("task.output"),
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"text\":\"subagent-only output\"}\n",
    )
    .expect("write output fixture");

    let ordinary = ClaudeJsonlRootReader::new(root.path().to_path_buf()).collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));
    assert!(ordinary.transcript_segments.is_empty());
    assert!(ordinary.read_failures.is_empty());

    let subagent =
        ClaudeJsonlRootReader::subagent_output(root.path().to_path_buf()).collect(&read_request(
            TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
            Projection::MetadataOnly,
            8,
        ));
    assert_eq!(subagent.transcript_segments.len(), 1);
    assert_eq!(
        subagent.transcript_segments[0].source,
        SourceKind::ClaudeSubagentOutput
    );
}

#[test]
fn canonical_timestamp_model_rejects_non_z_offsets_as_malformed_input() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("project.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-02T01:00:00+01:00\",\"text\":\"offset\"}\n",
            "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"canonical\"}\n",
        ),
    )
    .expect("write timestamp fixture transcript");
    let adapter =
        ClaudeTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::Malformed
    );
    assert!(
        outcome.read_failures[0]
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("project.jsonl:1"))
    );
}

#[test]
fn signal_plane_rejects_non_canonical_time_windows() {
    let request = EvidenceRequest {
        time_window: TimeWindow::Since(Timestamp::new("2026-01-02T01:00:00+01:00")),
        ..evidence_request()
    };

    let rejection = SignalPlane
        .collect_rejection(&request)
        .expect("offset window must be rejected");
    assert!(
        matches!(rejection, AggregatorReply::EvidenceRejected(rejection) if rejection.reason == RejectionReason::InvalidTimeWindow)
    );
}

#[test]
fn recent_time_window_does_not_accept_old_or_timestampless_records() {
    let source_identifier = SourceIdentifier::new("fixture-source");
    let request = read_request(
        TimeWindow::Recent(RelativeDuration {
            amount: DurationAmount::new(1),
            unit: DurationUnit::Hours,
        }),
        Projection::MetadataOnly,
        8,
    );
    let outcome = TranscriptReadOutcome::from_records(
        SourceKind::Claude,
        source_identifier.clone(),
        vec![
            TranscriptRecord::new(
                SourceKind::Claude,
                source_identifier.clone(),
                "old.jsonl".into(),
                1,
                Some(Timestamp::new("2000-01-01T00:00:00Z")),
                "old record".to_string(),
            ),
            TranscriptRecord::new(
                SourceKind::Claude,
                source_identifier,
                "missing-timestamp.jsonl".into(),
                1,
                None,
                "timestampless record".to_string(),
            ),
        ],
        Vec::new(),
        &request,
    );

    assert!(outcome.transcript_segments.is_empty());
    assert!(outcome.source_volumes.is_empty());
}

#[test]
fn recent_time_window_reports_unsupported_without_projecting_transcripts() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("project.jsonl"),
        concat!(
            "{\"timestamp\":\"2000-01-01T00:00:00Z\",\"text\":\"old\"}\n",
            "{\"text\":\"timestampless\"}\n",
        ),
    )
    .expect("write fixture transcript");
    let adapter =
        ClaudeTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Recent(RelativeDuration {
            amount: DurationAmount::new(1),
            unit: DurationUnit::Hours,
        }),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::UnsupportedFormat
    );
}

#[test]
fn nexus_lowers_recent_window_before_transcript_adapters() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/project.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"text\":\"old\"}\n",
            "{\"text\":\"timestampless\"}\n",
            "{\"timestamp\":\"2026-01-02T00:30:00Z\",\"text\":\"recent\"}\n",
        ),
    )
    .expect("write transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let request = EvidenceRequest {
        source_selection: SourceSelection::Only(SelectedSources {
            sources: vec![SourceKind::Claude],
        }),
        ..evidence_request()
    };
    let package = NexusPlane::with_runtime_configuration(runtime, clock)
        .collect(request)
        .expect("collect through nexus");

    assert_eq!(package.transcript_segments.len(), 1);
    assert_eq!(
        package.transcript_segments[0]
            .timestamp
            .as_ref()
            .map(|value| value.as_str()),
        Some("2026-01-02T00:30:00Z")
    );
    assert!(
        !package
            .read_failures
            .iter()
            .any(|failure| failure.reason == ReadFailureReason::UnsupportedFormat)
    );
}

#[test]
fn session_inventory_lookup_and_archive_round_trip_through_rkyv_store() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/session.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"title\":\"first\",\"subagent_name\":\"writer\",\"sessionId\":\"session-uuid-1\",\"role\":\"assistant\",\"text\":\"alpha one\"}\n",
            "{\"timestamp\":\"2026-01-02T00:20:00Z\",\"title\":\"second\",\"subagent_name\":\"writer\",\"sessionId\":\"session-uuid-1\",\"role\":\"assistant\",\"text\":\"beta two\"}\n",
        ),
    )
    .expect("write transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);
    let archive_path = ArchivePath::new("archive.rkyv");

    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: RequestIdentifier::new("inventory-sessions"),
            source_selection: SourceSelection::Only(SelectedSources {
                sources: vec![SourceKind::Claude],
            }),
            archive_path: Some(archive_path.clone()),
        })
        .expect("inventory sessions");
    assert_eq!(inventory.sessions.len(), 1);
    assert_eq!(
        inventory.scan_report.completeness,
        SessionInventoryCompleteness::Complete
    );
    assert_eq!(inventory.sessions[0].file_count.into_u64(), 1);
    assert_eq!(
        inventory.sessions[0].archive_status,
        SessionArchiveStatus::ArchiveUnknown
    );
    assert_eq!(
        inventory.sessions[0].lifecycle_status,
        SessionLifecycleStatus::Current
    );

    let looked_up = nexus
        .lookup_session(SessionLookupRequest {
            request_identifier: RequestIdentifier::new("lookup-session"),
            selector: SessionLookupSelector::ByReference(inventory.sessions[0].reference.clone()),
            archive_path: None,
        })
        .expect("lookup session");
    assert_eq!(looked_up.sessions.len(), 1);

    let written = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: RequestIdentifier::new("write-archive"),
            archive_path: archive_path.clone(),
            record: SessionArchiveRecordDraft {
                session: inventory.sessions[0].clone(),
                summary: ArchiveSummaryText::new("summary may include a direct quote"),
                provenance: ArchiveProvenanceText::new("bounded transcript read references"),
                created_at: Timestamp::new("2026-01-02T01:10:00Z"),
            },
        })
        .expect("write archive");

    let duplicate_written = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: RequestIdentifier::new("write-archive-duplicate"),
            archive_path: archive_path.clone(),
            record: SessionArchiveRecordDraft {
                session: inventory.sessions[0].clone(),
                summary: ArchiveSummaryText::new("summary may include a direct quote"),
                provenance: ArchiveProvenanceText::new("bounded transcript read references"),
                created_at: Timestamp::new("2026-01-02T01:10:00Z"),
            },
        })
        .expect("write duplicate archive record");
    assert_ne!(
        written.card.record_identifier,
        duplicate_written.card.record_identifier
    );

    let queried = nexus
        .query_session_archive(SessionArchiveQueryRequest {
            request_identifier: RequestIdentifier::new("query-archive"),
            archive_path: archive_path.clone(),
            session_reference: Some(inventory.sessions[0].reference.clone()),
        })
        .expect("query archive");
    assert_eq!(
        queried.records,
        vec![written.card.clone(), duplicate_written.card.clone()]
    );

    let read = nexus
        .read_session_archive(SessionArchiveReadRequest {
            request_identifier: RequestIdentifier::new("read-archive"),
            archive_path,
            record_identifier: written.card.record_identifier.clone(),
            maximum_summary_bytes: ByteLimit::new(12),
            maximum_provenance_bytes: ByteLimit::new(64),
        })
        .expect("read archive");
    assert_eq!(
        read.record.card.record_identifier,
        written.card.record_identifier
    );
    assert_eq!(read.record.summary.text.as_str(), "summary may ");
    assert_eq!(
        read.record.summary.completeness,
        ArchiveTextCompleteness::Truncated
    );
    assert_eq!(
        read.record.provenance.text.as_str(),
        "bounded transcript read references"
    );
}

#[test]
fn session_archive_rejects_paths_outside_daemon_local_archive_root() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/session.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"title\":\"first\",\"sessionId\":\"session-uuid-1\",\"role\":\"assistant\",\"text\":\"alpha one\"}\n",
    )
    .expect("write transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);
    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: RequestIdentifier::new("inventory-for-archive-rejection"),
            source_selection: SourceSelection::Only(SelectedSources {
                sources: vec![SourceKind::Claude],
            }),
            archive_path: None,
        })
        .expect("inventory sessions");
    let rejected = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: RequestIdentifier::new("write-outside-archive-root"),
            archive_path: ArchivePath::new(root.path().join("outside.rkyv").display().to_string()),
            record: SessionArchiveRecordDraft {
                session: inventory.sessions[0].clone(),
                summary: ArchiveSummaryText::new("summary"),
                provenance: ArchiveProvenanceText::new("provenance"),
                created_at: Timestamp::new("2026-01-02T01:10:00Z"),
            },
        })
        .expect_err("outside archive path must be rejected");
    assert_eq!(rejected.reason, OperationRejectionReason::Unauthorized);

    let archive_root = root.path().join("session-archive");
    fs::create_dir_all(&archive_root).expect("archive root");
    let outside = root.path().join("outside-symlink-target.rkyv");
    fs::write(&outside, "not an archive").expect("outside target");
    symlink(&outside, archive_root.join("linked.rkyv")).expect("archive symlink");
    let symlink_rejected = nexus
        .query_session_archive(SessionArchiveQueryRequest {
            request_identifier: RequestIdentifier::new("query-symlink-archive"),
            archive_path: ArchivePath::new("linked.rkyv"),
            session_reference: None,
        })
        .expect_err("archive symlink must be rejected");
    assert_eq!(
        symlink_rejected.reason,
        OperationRejectionReason::Unauthorized
    );
}

#[test]
fn output_interface_lists_subagents_outputs_segments_and_bounded_reads() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/session.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"title\":\"first\",\"subagent_name\":\"writer\",\"role\":\"assistant\",\"text\":\"alpha one\"}\n",
            "{\"timestamp\":\"2026-01-02T00:20:00Z\",\"title\":\"second\",\"subagent_name\":\"writer\",\"role\":\"assistant\",\"text\":\"beta two\"}\n",
        ),
    )
    .expect("write transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let index_store = typed_index_store(runtime.store_path());
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: RequestIdentifier::new("list-sessions"),
            filter: SessionListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    sources: vec![SourceKind::Claude],
                }),
                time_window: Some(TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z"))),
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
        })
        .expect("list sessions");
    assert_eq!(sessions.sessions.len(), 1);
    assert_eq!(
        sessions.sessions[0]
            .output_count
            .as_ref()
            .map(|count| count.into_u64()),
        Some(2)
    );
    assert_eq!(
        sessions.sessions[0]
            .last_observed_at
            .as_ref()
            .map(|timestamp| timestamp.as_str()),
        Some("2026-01-02T00:20:00Z")
    );
    assert!(index_store.pointer_path().exists());

    let subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: RequestIdentifier::new("list-subagents"),
            filter: SubagentListFilter {
                session_reference: sessions.sessions[0].reference.clone(),
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier: None,
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
        })
        .expect("list subagents");
    assert_eq!(subagents.subagents.len(), 1);
    assert_eq!(subagents.subagents[0].name.as_str(), "writer");
    assert_eq!(
        subagents.subagents[0].authored_status,
        AuthoredStatus::AgentAuthored
    );

    let outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("list-outputs"),
            filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    sources: vec![SourceKind::Claude],
                }),
                session_reference: Some(sessions.sessions[0].reference.clone()),
                subagent_reference: Some(subagents.subagents[0].reference.clone()),
                task_identifier: None,
                authored_status: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::AgentAuthored,
                ),
                time_window: Some(TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z"))),
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::BoundedPreview(BoundedTextProjection {
                maximum_bytes: ByteLimit::new(5),
            }),
        })
        .expect("list outputs");
    assert_eq!(outputs.outputs.len(), 2);
    assert_eq!(
        outputs.outputs[0]
            .preview
            .as_ref()
            .map(|preview| preview.text.as_str()),
        Some("alpha")
    );
    assert_eq!(
        outputs.outputs[0]
            .size
            .byte_count
            .as_ref()
            .map(|count| count.into_u64()),
        Some(9)
    );

    let segments = nexus
        .list_output_segments(OutputSegmentListRequest {
            request_identifier: RequestIdentifier::new("list-segments"),
            filter: OutputSegmentListFilter {
                output_reference: outputs.outputs[0].reference.clone(),
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("list output segments");
    assert_eq!(segments.segments.len(), 1);
    assert_eq!(
        segments.segments[0]
            .byte_range
            .as_ref()
            .map(|range| (range.start.into_u64(), range.end.into_u64())),
        Some((0, 9))
    );
    assert_eq!(
        segments.segments[0]
            .line_range
            .as_ref()
            .map(|range| (range.start.into_u64(), range.end.into_u64())),
        Some((1, 2))
    );

    let estimated = nexus
        .estimate_output(signal_aggregator::OutputEstimateRequest {
            request_identifier: RequestIdentifier::new("estimate-output"),
            output_reference: outputs.outputs[0].reference.clone(),
            range: OutputReadRange::Bytes(ByteRange {
                start: ByteCount::new(0),
                end: ByteCount::new(5),
            }),
        })
        .expect("estimate output");
    assert_eq!(
        estimated
            .size
            .byte_count
            .as_ref()
            .map(|count| count.into_u64()),
        Some(5)
    );

    let read = nexus
        .read_output(OutputReadRequest {
            request_identifier: RequestIdentifier::new("read-output"),
            output_reference: outputs.outputs[0].reference.clone(),
            range: OutputReadRange::Bytes(ByteRange {
                start: ByteCount::new(0),
                end: ByteCount::new(5),
            }),
            maximum_bytes: ByteLimit::new(5),
        })
        .expect("read output");
    assert_eq!(read.excerpt.text.as_str(), "alpha");
    assert!(read.excerpt.truncation.is_none());
}

#[test]
fn output_interface_paginates_enforces_limits_and_rejects_stale_references() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let transcript = root.path().join("claude/session.jsonl");
    fs::write(
        &transcript,
        concat!(
            "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"text\":\"first output\"}\n",
            "{\"timestamp\":\"2026-01-02T00:20:00Z\",\"text\":\"second output\"}\n",
        ),
    )
    .expect("write transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let first_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("first-page"),
            filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    sources: vec![SourceKind::Claude],
                }),
                session_reference: None,
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("first page");
    assert_eq!(first_page.outputs.len(), 1);
    let cursor = first_page.page.next_cursor.clone().expect("next cursor");
    let second_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("second-page"),
            filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    sources: vec![SourceKind::Claude],
                }),
                session_reference: None,
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: Some(cursor),
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("second page");
    assert_eq!(second_page.outputs.len(), 1);
    assert_ne!(
        first_page.outputs[0].reference,
        second_page.outputs[0].reference
    );

    let oversized_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("oversized-page"),
            filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                session_reference: None,
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(65),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("page limit must be enforced");
    assert_eq!(oversized_page.reason, OperationRejectionReason::Oversized);

    let missing = nexus
        .read_output(OutputReadRequest {
            request_identifier: RequestIdentifier::new("missing-output"),
            output_reference: FragileOutputReference::new("missing-output-reference"),
            range: OutputReadRange::EntireOutput,
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("unknown reference rejected");
    assert_eq!(missing.reason, OperationRejectionReason::Missing);

    let stale_reference = first_page.outputs[0].reference.clone();
    fs::write(
        &transcript,
        "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"text\":\"changed output with different bytes\"}\n",
    )
    .expect("rewrite transcript");
    let stale = nexus
        .read_output(OutputReadRequest {
            request_identifier: RequestIdentifier::new("stale-output"),
            output_reference: stale_reference,
            range: OutputReadRange::EntireOutput,
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("stale reference rejected");
    assert_eq!(
        stale.reason,
        OperationRejectionReason::FragileReferenceStale
    );
}

#[test]
fn output_interface_rejects_cursors_when_listing_shape_changes() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/session-a.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"subagent_name\":\"writer\",\"role\":\"assistant\",\"text\":\"agent output\"}\n",
            "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"subagent_name\":\"reviewer\",\"role\":\"user\",\"text\":\"human output\"}\n",
        ),
    )
    .expect("write first transcript");
    fs::write(
        root.path().join("claude/session-b.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:20:00Z\",\"text\":\"later output\"}\n",
    )
    .expect("write second transcript");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let first_sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: RequestIdentifier::new("first-sessions-shape"),
            filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
        })
        .expect("first sessions page");
    let sessions_cursor = first_sessions
        .page
        .next_cursor
        .clone()
        .expect("sessions cursor");
    let stale_sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: RequestIdentifier::new("stale-sessions-shape"),
            filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window: Some(TimeWindow::Since(Timestamp::new("2026-01-02T00:15:00Z"))),
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: Some(sessions_cursor),
                order: ListingOrder::OldestFirst,
            },
        })
        .expect_err("session cursor is bound to the original time filter");
    assert_eq!(
        stale_sessions.reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let session_listing = nexus
        .list_sessions(SessionListRequest {
            request_identifier: RequestIdentifier::new("all-sessions-for-shape"),
            filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
        })
        .expect("all sessions");
    let session_with_subagents = session_listing
        .sessions
        .iter()
        .find(|session| {
            session
                .output_count
                .as_ref()
                .is_some_and(|count| count.into_u64() == 2)
        })
        .expect("session with two outputs")
        .reference
        .clone();

    let first_subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: RequestIdentifier::new("first-subagents-shape"),
            filter: SubagentListFilter {
                session_reference: session_with_subagents.clone(),
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
        })
        .expect("first subagents page");
    let subagents_cursor = first_subagents
        .page
        .next_cursor
        .clone()
        .expect("subagents cursor");
    let stale_subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: RequestIdentifier::new("stale-subagents-shape"),
            filter: SubagentListFilter {
                session_reference: session_with_subagents.clone(),
                authored_status: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::HumanAuthored,
                ),
                task_identifier: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: Some(subagents_cursor),
                order: ListingOrder::OldestFirst,
            },
        })
        .expect_err("subagent cursor is bound to the original authorship filter");
    assert_eq!(
        stale_subagents.reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let first_outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("first-outputs-shape"),
            filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                session_reference: Some(session_with_subagents.clone()),
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("first outputs page");
    let outputs_cursor = first_outputs
        .page
        .next_cursor
        .clone()
        .expect("outputs cursor");
    let stale_outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("stale-outputs-shape"),
            filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                session_reference: Some(session_with_subagents.clone()),
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::HumanAuthored,
                ),
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: Some(outputs_cursor.clone()),
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("output cursor is bound to the original authorship filter");
    assert_eq!(
        stale_outputs.reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let stale_output_page_shape = nexus
        .list_outputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("stale-output-page-shape"),
            filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                session_reference: Some(session_with_subagents),
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(2),
                cursor: Some(outputs_cursor),
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("output cursor is bound to the original page limit");
    assert_eq!(
        stale_output_page_shape.reason,
        OperationRejectionReason::FragileReferenceStale
    );
}

#[test]
fn output_interface_accepts_legacy_roots_as_read_only_and_rejects_index_under_them() {
    let root = TempDir::new().expect("temporary root");
    let legacy_root = root.path().join("reports");
    fs::create_dir_all(&legacy_root).expect("legacy root");
    fs::write(legacy_root.join("report.md"), "legacy recovery text").expect("legacy report");
    let mut configuration = accepted_configuration(&root);
    configuration.output_interfaces.legacy_recovery_sources =
        vec![LegacyRecoverySource::LegacyReports(LegacyRecoveryRoot {
            path: FilesystemPath::new(legacy_root.display().to_string()),
            access: LegacyRecoveryAccess::ReadOnlyRecovery,
        })];
    let accepted = RuntimeConfiguration::validate_from_meta(&configuration);
    assert!(matches!(
        accepted,
        RuntimeConfigurationValidation::Accepted(_)
    ));

    let mut rejected_configuration = configuration.clone();
    rejected_configuration.store_path =
        FilesystemPath::new(legacy_root.join("store.sema").display().to_string());
    let rejected = RuntimeConfiguration::validate_from_meta(&rejected_configuration);
    let report = match rejected {
        RuntimeConfigurationValidation::Accepted(_) => panic!("index under legacy root accepted"),
        RuntimeConfigurationValidation::Rejected(report) => report,
    };
    assert!(report.issues.iter().any(|issue| issue.kind
        == meta_signal_aggregator::ConfigurationValidationIssueKind::InvalidFragileIndexConfiguration));
    assert_eq!(
        fs::read_to_string(legacy_root.join("report.md")).expect("legacy report unchanged"),
        "legacy recovery text"
    );
}

#[test]
fn configuration_store_migrates_legacy_zero_one_configuration_with_default_output_interfaces() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let legacy = LegacyAggregatorConfiguration {
        ordinary_socket_path: configuration.ordinary_socket_path.clone(),
        ordinary_socket_mode: configuration.ordinary_socket_mode,
        meta_socket_path: configuration.meta_socket_path.clone(),
        meta_socket_mode: configuration.meta_socket_mode,
        store_path: configuration.store_path.clone(),
        active_repositories: configuration.active_repositories.clone(),
        transcript_sources: configuration.transcript_sources.clone(),
        default_projection: configuration.default_projection.clone(),
        default_limit_policy: configuration.default_limit_policy.clone(),
    };
    let configuration_path = root.path().join("legacy-configuration.nota");
    fs::write(&configuration_path, legacy.to_nota()).expect("write legacy configuration");
    let migrated = ConfigurationStore::at_path(&configuration_path)
        .read_configuration()
        .expect("migrate legacy configuration");
    assert_eq!(
        migrated
            .output_interfaces
            .limits
            .maximum_page_items
            .into_u64(),
        64
    );
    assert!(
        migrated
            .output_interfaces
            .legacy_recovery_sources
            .is_empty()
    );
}

#[test]
fn daemon_cli_boundary_handles_collect_version_and_meta_configuration() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/project.jsonl"),
        concat!(
            "{\"timestamp\":\"2026-01-01T00:00:00Z\",\"text\":\"old\"}\n",
            "{\"text\":\"timestampless\"}\n",
            "{\"timestamp\":\"2026-01-02T00:30:00Z\",\"text\":\"recent\"}\n",
        ),
    )
    .expect("write transcript");
    let configuration_path = root.path().join("configuration.nota");
    run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator-write-configuration"),
        &configuration_path,
        &configuration.to_nota(),
    );
    let _daemon = DaemonGuard::start(&configuration_path, "2026-01-02T01:00:00Z");
    let ordinary_socket_path = std::path::Path::new(configuration.ordinary_socket_path.as_str());
    let meta_socket_path = std::path::Path::new(configuration.meta_socket_path.as_str());
    wait_for_socket(ordinary_socket_path);
    wait_for_socket(meta_socket_path);
    assert_socket_mode(ordinary_socket_path, 0o660);
    assert_socket_mode(meta_socket_path, 0o600);

    send_malformed_socket_bytes(ordinary_socket_path);

    let version_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator"),
        &configuration_path,
        &AggregatorRequest::Version(Version {
            client_name: Some(ContractName::new("boundary-test")),
        })
        .to_nota(),
    );
    let version_reply = NotaSource::new(&version_output)
        .parse::<AggregatorReply>()
        .expect("parse version reply");
    assert!(matches!(version_reply, AggregatorReply::VersionReported(_)));

    let observe_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaAggregatorRequest::ObserveConfiguration(ObserveConfiguration { observer: None })
            .to_nota(),
    );
    let observe_reply = NotaSource::new(&observe_output)
        .parse::<MetaAggregatorReply>()
        .expect("parse observe reply");
    assert!(matches!(
        observe_reply,
        MetaAggregatorReply::ConfigurationObserved(_)
    ));

    let validate_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaAggregatorRequest::ValidateConfiguration(ConfigurationCandidate {
            configuration: configuration.clone(),
        })
        .to_nota(),
    );
    let validate_reply = NotaSource::new(&validate_output)
        .parse::<MetaAggregatorReply>()
        .expect("parse validate reply");
    assert!(matches!(
        validate_reply,
        MetaAggregatorReply::ConfigurationValidated(_)
    ));

    let configure_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaAggregatorRequest::Configure(ConfigurationChange {
            configuration: configuration.clone(),
        })
        .to_nota(),
    );
    let configure_reply = NotaSource::new(&configure_output)
        .parse::<MetaAggregatorReply>()
        .expect("parse configure reply");
    assert!(matches!(
        configure_reply,
        MetaAggregatorReply::ConfigurationConfigured(_)
    ));

    let list_outputs_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator"),
        &configuration_path,
        &AggregatorRequest::ListOutputs(OutputListRequest {
            request_identifier: RequestIdentifier::new("daemon-list-outputs"),
            filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    sources: vec![SourceKind::Claude],
                }),
                session_reference: None,
                subagent_reference: None,
                task_identifier: None,
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window: Some(TimeWindow::Since(Timestamp::new("2026-01-02T00:00:00Z"))),
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .to_nota(),
    );
    let list_outputs_reply = NotaSource::new(&list_outputs_output)
        .parse::<AggregatorReply>()
        .expect("parse list outputs reply");
    assert!(matches!(
        list_outputs_reply,
        AggregatorReply::OutputsListed(listed) if listed.outputs.len() == 1
    ));

    let collect_request = EvidenceRequest {
        source_selection: SourceSelection::Only(SelectedSources {
            sources: vec![SourceKind::Claude],
        }),
        ..evidence_request()
    };
    let collect_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator"),
        &configuration_path,
        &AggregatorRequest::Collect(collect_request).to_nota(),
    );
    let collect_reply = NotaSource::new(&collect_output)
        .parse::<AggregatorReply>()
        .expect("parse collect reply");
    let package = match collect_reply {
        AggregatorReply::EvidenceCollected(package) => package,
        other => panic!("expected collected evidence, got {other:?}"),
    };
    assert_eq!(package.transcript_segments.len(), 1);
    assert_eq!(
        package.transcript_segments[0]
            .timestamp
            .as_ref()
            .map(|value| value.as_str()),
        Some("2026-01-02T00:30:00Z")
    );
    assert!(
        !package
            .read_failures
            .iter()
            .any(|failure| failure.reason == ReadFailureReason::UnsupportedFormat)
    );
}

#[test]
fn meta_configure_persists_to_startup_configuration_for_restart() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let configuration_path = root.path().join("configuration.nota");
    run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator-write-configuration"),
        &configuration_path,
        &configuration.to_nota(),
    );
    let daemon = DaemonGuard::start(&configuration_path, "2026-01-02T01:00:00Z");
    wait_for_socket(std::path::Path::new(
        configuration.meta_socket_path.as_str(),
    ));

    let mut updated_configuration = configuration.clone();
    updated_configuration.ordinary_socket_path = FilesystemPath::new(
        root.path()
            .join("ordinary-restarted.sock")
            .display()
            .to_string(),
    );
    updated_configuration.meta_socket_path = FilesystemPath::new(
        root.path()
            .join("meta-restarted.sock")
            .display()
            .to_string(),
    );
    updated_configuration.store_path =
        FilesystemPath::new(root.path().join("future-ledger.sema").display().to_string());

    let configure_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaAggregatorRequest::Configure(ConfigurationChange {
            configuration: updated_configuration.clone(),
        })
        .to_nota(),
    );
    let configure_reply = NotaSource::new(&configure_output)
        .parse::<MetaAggregatorReply>()
        .expect("parse configure reply");
    assert!(matches!(
        configure_reply,
        MetaAggregatorReply::ConfigurationConfigured(_)
    ));

    drop(daemon);
    let _restarted_daemon = DaemonGuard::start(&configuration_path, "2026-01-02T01:00:00Z");
    wait_for_socket(std::path::Path::new(
        updated_configuration.meta_socket_path.as_str(),
    ));

    let observe_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaAggregatorRequest::ObserveConfiguration(ObserveConfiguration { observer: None })
            .to_nota(),
    );
    let observe_reply = NotaSource::new(&observe_output)
        .parse::<MetaAggregatorReply>()
        .expect("parse observe reply");
    match observe_reply {
        MetaAggregatorReply::ConfigurationObserved(observed) => match observed.observation {
            ConfigurationObservation::Configured(observed_configuration) => {
                assert_eq!(observed_configuration, updated_configuration);
            }
            other => panic!("expected configured observation, got {other:?}"),
        },
        other => panic!("expected configuration observation, got {other:?}"),
    }
}

#[test]
fn request_byte_limit_truncation_reason_is_carried_into_text_excerpt() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("project.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"hello world\"}\n",
    )
    .expect("write fixture transcript");
    let adapter =
        ClaudeTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request_with_byte_limit(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::BoundedText(BoundedTextProjection {
            maximum_bytes: ByteLimit::new(10),
        }),
        8,
        4,
    ));

    assert_eq!(
        outcome.truncations[0].reason,
        TruncationReason::RequestLimit
    );
    match &outcome.transcript_segments[0].projection {
        SegmentProjection::Text(excerpt) => assert_eq!(
            excerpt
                .truncation
                .as_ref()
                .map(|truncation| truncation.reason),
            Some(TruncationReason::RequestLimit)
        ),
        other => panic!("expected text projection, got {other:?}"),
    }
}

#[test]
fn projection_byte_limit_truncation_reason_is_carried_into_text_excerpt() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("project.jsonl"),
        "{\"timestamp\":\"2026-01-02T00:00:00Z\",\"text\":\"hello world\"}\n",
    )
    .expect("write fixture transcript");
    let adapter =
        ClaudeTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request_with_byte_limit(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::BoundedText(BoundedTextProjection {
            maximum_bytes: ByteLimit::new(4),
        }),
        8,
        10,
    ));

    assert_eq!(
        outcome.truncations[0].reason,
        TruncationReason::ProjectionLimit
    );
    match &outcome.transcript_segments[0].projection {
        SegmentProjection::Text(excerpt) => assert_eq!(
            excerpt
                .truncation
                .as_ref()
                .map(|truncation| truncation.reason),
            Some(TruncationReason::ProjectionLimit)
        ),
        other => panic!("expected text projection, got {other:?}"),
    }
}

#[test]
fn codex_adapter_reads_session_index_and_tolerates_unknown_fields() {
    let root = TempDir::new().expect("temporary root");
    let sessions = root.path().join("sessions");
    fs::create_dir_all(&sessions).expect("sessions directory");
    fs::write(
        root.path().join("index.jsonl"),
        "{\"path\":\"sessions/one.jsonl\",\"extra\":42}\n",
    )
    .expect("write index");
    fs::write(
        sessions.join("one.jsonl"),
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"content\":\"codex answer\",\"ignored\":true}\n",
    )
    .expect("write session");
    let adapter =
        CodexTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.transcript_segments[0].source, SourceKind::Codex);
    assert!(outcome.read_failures.is_empty());
}

#[test]
fn codex_adapter_honors_configured_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    for index in 0..2 {
        fs::write(
            root.path().join(format!("{index}.jsonl")),
            format!(
                "{{\"timestamp\":\"2026-02-01T00:00:0{index}Z\",\"content\":\"codex {index}\"}}\n"
            ),
        )
        .expect("write codex fixture");
    }
    let adapter = CodexTranscriptAdapter::new(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("1.jsonl"))
    }));
}

#[test]
fn codex_adapter_honors_configured_index_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    let sessions = root.path().join("sessions");
    fs::create_dir_all(&sessions).expect("sessions directory");
    let mut index_text = String::new();
    for index in 0..2 {
        let session_name = format!("{index}.jsonl");
        fs::write(
            sessions.join(&session_name),
            format!(
                "{{\"timestamp\":\"2026-02-01T00:00:0{index}Z\",\"content\":\"codex {index}\"}}\n"
            ),
        )
        .expect("write indexed codex fixture");
        index_text.push_str(&format!("{{\"path\":\"sessions/{session_name}\"}}\n"));
    }
    fs::write(root.path().join("index.jsonl"), index_text).expect("write index");
    let adapter = CodexTranscriptAdapter::new(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("sessions/1.jsonl"))
    }));
}

#[test]
fn codex_health_observation_reports_configured_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    for index in 0..2 {
        fs::write(
            root.path().join(format!("{index}.jsonl")),
            format!(
                "{{\"timestamp\":\"2026-02-01T00:00:0{index}Z\",\"content\":\"codex {index}\"}}\n"
            ),
        )
        .expect("write codex fixture");
    }
    let source = TranscriptAdapterConfiguration::Codex(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let health = SourceHealthObserver::new(source).observe();

    assert_eq!(health.status, SourceHealthStatus::DiscoveryTruncated);
    assert_eq!(health.discovered_files.into_u64(), 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.kind == ScanLimitKind::DiscoveredFiles && limit.limit.into_u64() == 1
    }));
}

#[test]
fn codex_health_observation_reports_configured_index_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    let sessions = root.path().join("sessions");
    fs::create_dir_all(&sessions).expect("sessions directory");
    let mut index_text = String::new();
    for index in 0..2 {
        let session_name = format!("{index}.jsonl");
        fs::write(
            sessions.join(&session_name),
            format!(
                "{{\"timestamp\":\"2026-02-01T00:00:0{index}Z\",\"content\":\"codex {index}\"}}\n"
            ),
        )
        .expect("write indexed codex fixture");
        index_text.push_str(&format!("{{\"path\":\"sessions/{session_name}\"}}\n"));
    }
    fs::write(root.path().join("index.jsonl"), index_text).expect("write index");
    let source = TranscriptAdapterConfiguration::Codex(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let health = SourceHealthObserver::new(source).observe();

    assert_eq!(health.status, SourceHealthStatus::DiscoveryTruncated);
    assert_eq!(health.discovered_files.into_u64(), 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.kind == ScanLimitKind::DiscoveredFiles && limit.limit.into_u64() == 1
    }));
}

#[test]
fn codex_adapter_reports_index_paths_that_escape_root_as_read_failure() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("index.jsonl"),
        "{\"path\":\"/outside-configured-root/session.jsonl\"}\n",
    )
    .expect("write escaping index");
    let adapter =
        CodexTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::PermissionDenied
    );
    assert!(
        outcome.read_failures[0]
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
    );
    assert!(
        outcome.read_failures[0]
            .source_identifier
            .as_ref()
            .is_some_and(|identifier| identifier
                .as_str()
                .contains("locator:/outside-configured-root/session.jsonl"))
    );
    assert!(outcome.transcript_segments.is_empty());
}

#[test]
fn codex_adapter_reports_absolute_parent_traversal_missing_index_paths_with_context() {
    let root = TempDir::new().expect("temporary root");
    let first_escape = root.path().join("..").join("outside").join("missing.jsonl");
    let second_escape = root
        .path()
        .join("sessions")
        .join("..")
        .join("..")
        .join("outside")
        .join("missing.jsonl");
    let index_text = format!(
        "{{\"path\":{}}}\n{{\"path\":{}}}\n",
        serde_json::to_string(&first_escape.display().to_string()).expect("first path json"),
        serde_json::to_string(&second_escape.display().to_string()).expect("second path json"),
    );
    fs::write(root.path().join("index.jsonl"), index_text).expect("write escaping index");
    let adapter =
        CodexTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(
        outcome
            .read_failures
            .iter()
            .all(|failure| failure.reason == ReadFailureReason::PermissionDenied)
    );
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
            && failure
                .source_identifier
                .as_ref()
                .is_some_and(|identifier| {
                    identifier.as_str().contains("locator:")
                        && identifier.as_str().contains("../outside/missing.jsonl")
                })
    }));
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:2"))
            && failure
                .source_identifier
                .as_ref()
                .is_some_and(|identifier| {
                    identifier.as_str().contains("locator:")
                        && identifier
                            .as_str()
                            .contains("sessions/../../outside/missing.jsonl")
                })
    }));
    assert!(outcome.transcript_segments.is_empty());
}

#[test]
fn codex_adapter_reports_malformed_index_lines_with_index_line_context() {
    let root = TempDir::new().expect("temporary root");
    fs::write(root.path().join("index.jsonl"), "not-json\n{}").expect("write malformed index");
    let reader = CodexSessionRootReader::with_limits(
        root.path().to_path_buf(),
        TranscriptScanLimits::new(TranscriptScanLimitConfiguration::new(
            MaximumScanEntries::new(16),
            MaximumDiscoveredFiles::new(16),
            MaximumFileBytes::new(4096),
            MaximumLineBytes::new(128),
            MaximumReadFailures::new(8),
        )),
    );
    let outcome = reader.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
    }));
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:2"))
    }));
    assert!(outcome.transcript_segments.is_empty());
}

#[test]
fn codex_adapter_reports_symlinked_index_paths_that_escape_root_as_read_failure() {
    let root = TempDir::new().expect("temporary root");
    let outside_root = TempDir::new().expect("outside temporary root");
    let sessions = root.path().join("sessions");
    fs::create_dir_all(&sessions).expect("sessions directory");
    let outside_session = outside_root.path().join("session.jsonl");
    fs::write(
        &outside_session,
        "{\"timestamp\":\"2026-02-01T00:00:00Z\",\"content\":\"outside codex answer\"}\n",
    )
    .expect("write outside session");
    symlink(&outside_session, sessions.join("escape.jsonl")).expect("session symlink");
    fs::write(
        root.path().join("index.jsonl"),
        "{\"path\":\"sessions/escape.jsonl\"}\n",
    )
    .expect("write symlink index");
    let adapter =
        CodexTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].reason,
        ReadFailureReason::PermissionDenied
    );
    assert!(outcome.transcript_segments.is_empty());
}

#[test]
fn pi_adapter_reads_run_history_records() {
    let root = TempDir::new().expect("temporary root");
    fs::write(
        root.path().join("run-history.jsonl"),
        "{\"started_at\":\"2026-03-01T00:00:00Z\",\"output\":\"pi run output\",\"unknown\":{}}\n",
    )
    .expect("write run history");
    let adapter =
        PiTranscriptAdapter::new(TranscriptRootConfiguration::new(root.path().to_path_buf()));
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-02-01T00:00:00Z")),
        Projection::IdentifiersOnly,
        8,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.transcript_segments[0].source, SourceKind::Pi);
    assert!(matches!(
        outcome.transcript_segments[0].projection,
        SegmentProjection::IdentifiersOnly
    ));
}

#[test]
fn pi_adapter_honors_configured_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    for index in 0..2 {
        fs::write(
            root.path().join(format!("run-history-{index}.jsonl")),
            format!(
                "{{\"started_at\":\"2026-03-01T00:00:0{index}Z\",\"output\":\"pi {index}\"}}\n"
            ),
        )
        .expect("write pi fixture");
    }
    let adapter = PiTranscriptAdapter::new(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let outcome = adapter.collect(&read_request(
        TimeWindow::Since(Timestamp::new("2026-02-01T00:00:00Z")),
        Projection::IdentifiersOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .path
            .as_ref()
            .is_some_and(|path| path.as_str().contains("run-history-1.jsonl"))
    }));
}

#[test]
fn pi_health_observation_reports_configured_discovery_limit() {
    let root = TempDir::new().expect("temporary root");
    for index in 0..2 {
        fs::write(
            root.path().join(format!("run-history-{index}.jsonl")),
            format!(
                "{{\"started_at\":\"2026-03-01T00:00:0{index}Z\",\"output\":\"pi {index}\"}}\n"
            ),
        )
        .expect("write pi fixture");
    }
    let source = TranscriptAdapterConfiguration::Pi(
        TranscriptRootConfiguration::new(root.path().to_path_buf())
            .with_scan_limits(small_discovery_limits(1)),
    );
    let health = SourceHealthObserver::new(source).observe();

    assert_eq!(health.status, SourceHealthStatus::DiscoveryTruncated);
    assert_eq!(health.discovered_files.into_u64(), 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.kind == ScanLimitKind::DiscoveredFiles && limit.limit.into_u64() == 1
    }));
}

#[test]
fn session_inventory_reports_configured_indexed_codex_and_pi_discovery_limits() {
    let root = TempDir::new().expect("temporary root");
    let repository = root.path().join("repository");
    let codex = root.path().join("codex");
    let pi = root.path().join("pi");
    fs::create_dir_all(&repository).expect("repository directory");
    let codex_sessions = codex.join("sessions");
    fs::create_dir_all(&codex_sessions).expect("codex sessions directory");
    fs::create_dir_all(&pi).expect("pi directory");
    let mut codex_index_text = String::new();
    for index in 0..2 {
        let session_name = format!("{index}.jsonl");
        fs::write(
            codex_sessions.join(&session_name),
            format!(
                "{{\"timestamp\":\"2026-02-01T00:00:0{index}Z\",\"content\":\"codex {index}\"}}\n"
            ),
        )
        .expect("write indexed codex fixture");
        codex_index_text.push_str(&format!("{{\"path\":\"sessions/{session_name}\"}}\n"));
        fs::write(
            pi.join(format!("run-history-{index}.jsonl")),
            format!(
                "{{\"started_at\":\"2026-03-01T00:00:0{index}Z\",\"output\":\"pi {index}\"}}\n"
            ),
        )
        .expect("write pi fixture");
    }
    fs::write(codex.join("index.jsonl"), codex_index_text).expect("write codex index");
    let output_interfaces = OutputInterfaceConfiguration {
        limits: OutputInterfaceLimitPolicy {
            maximum_transcript_discovered_files: ItemCount::new(1),
            ..OutputInterfaceLimitPolicy::default()
        },
        ..OutputInterfaceConfiguration::default()
    };
    let configuration = AggregatorConfiguration {
        ordinary_socket_path: FilesystemPath::new(
            root.path().join("ordinary.sock").display().to_string(),
        ),
        ordinary_socket_mode: SocketMode::new(0o660),
        meta_socket_path: FilesystemPath::new(root.path().join("meta.sock").display().to_string()),
        meta_socket_mode: SocketMode::new(0o600),
        store_path: FilesystemPath::new(root.path().join("store.sema").display().to_string()),
        active_repositories: vec![ActiveRepository {
            name: RepositoryName::new("fixture-repository"),
            path: FilesystemPath::new(repository.display().to_string()),
        }],
        transcript_sources: vec![
            TranscriptSource::Codex(TranscriptRoot {
                path: FilesystemPath::new(codex.display().to_string()),
            }),
            TranscriptSource::Pi(TranscriptRoot {
                path: FilesystemPath::new(pi.display().to_string()),
            }),
        ],
        default_projection: Projection::MetadataOnly,
        default_limit_policy: LimitPolicy {
            maximum_segments: SegmentLimit::new(16),
            maximum_bytes: ByteLimit::new(4096),
        },
        output_interfaces,
    };
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-03-01T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let inventory = NexusPlane::with_runtime_configuration(runtime, clock)
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: RequestIdentifier::new("inventory-configured-limits"),
            source_selection: SourceSelection::AllConfigured,
            archive_path: None,
        })
        .expect("inventory sessions");

    assert_eq!(
        inventory.scan_report.completeness,
        SessionInventoryCompleteness::Truncated
    );
    for source in [SourceKind::Codex, SourceKind::Pi] {
        let report = inventory
            .scan_report
            .sources
            .iter()
            .find(|report| report.source == source)
            .expect("source report");
        assert_eq!(report.completeness, SessionInventoryCompleteness::Truncated);
        assert_eq!(report.discovered_files.into_u64(), 1);
        assert!(report.scan_limits.iter().any(|limit| {
            limit.kind == ScanLimitKind::DiscoveredFiles && limit.limit.into_u64() == 1
        }));
    }
}

#[test]
fn transcript_adapters_extract_observed_logical_block_kinds() {
    let root = TempDir::new().expect("temporary root");
    let claude = root.path().join("claude");
    let codex = root.path().join("codex");
    let pi = root.path().join("pi");
    fs::create_dir_all(&claude).expect("claude directory");
    fs::create_dir_all(&codex).expect("codex directory");
    fs::create_dir_all(&pi).expect("pi directory");
    fs::write(
        claude.join("session.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-01T00:00:00Z","message":{"role":"assistant","content":["#,
            r#"{"type":"thinking","thinking":"claude reasoning"},"#,
            r#"{"type":"text","text":"claude answer"},"#,
            r#"{"type":"tool_use","name":"Bash","input":{"command":"echo claude"}},"#,
            r#"{"type":"tool_result","content":"claude tool result"},"#,
            r#"{"type":"image","source":{"type":"base64"}}]}}
"#,
        ),
    )
    .expect("write claude blocks");
    fs::write(
        codex.join("session.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-01T00:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"codex prompt"}]}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:01Z","payload":{"type":"user_message","message":"codex observed user prompt","images":[],"local_images":[],"text_elements":[]}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:02Z","payload":{"type":"agent_message","message":"codex observed answer","phase":"final_answer","memory_citation":null}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:03Z","payload":{"type":"reasoning","summary":[{"text":"codex reasoning"}]}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:04Z","payload":{"type":"function_call","name":"shell","arguments":"{}"}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:05Z","payload":{"type":"custom_tool_call","call_id":"call-redacted","name":"shell","input":"{}","status":"completed"}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:06Z","payload":{"type":"tool_search_call","call_id":"search-redacted","status":"completed","execution":"approved","arguments":{"query":"redacted","limit":1}}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:07Z","payload":{"type":"function_call_output","output":"codex tool result"}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:08Z","payload":{"type":"custom_tool_call_output","call_id":"call-redacted","output":"codex custom tool result"}}
"#,
            r#"{"timestamp":"2026-04-01T00:00:09Z","payload":{"type":"tool_search_output","call_id":"search-redacted","status":"completed","execution":"approved","tools":[]}}
"#,
        ),
    )
    .expect("write codex blocks");
    fs::write(
        pi.join("run-history.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-01T00:00:00Z","message":{"role":"assistant","content":["#,
            r#"{"type":"thinking","text":"pi reasoning"},"#,
            r#"{"type":"text","text":"pi answer"},"#,
            r#"{"type":"toolCall","name":"shell","input":{"command":"echo pi"}},"#,
            r#"{"type":"toolResult","text":"pi tool result"}]}}
"#,
        ),
    )
    .expect("write pi blocks");

    let claude_records = ClaudeJsonlRootReader::new(claude).read_records().records;
    let codex_records = CodexSessionRootReader::new(codex).read_records().records;
    let pi_records = PiRunHistoryRootReader::new(pi).read_records().records;

    let claude_kinds = claude_records[0]
        .blocks
        .iter()
        .map(|block| block.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        claude_kinds,
        vec![
            TranscriptBlockKind::Inference,
            TranscriptBlockKind::AgentResponse,
            TranscriptBlockKind::ToolCall,
            TranscriptBlockKind::ToolResult,
            TranscriptBlockKind::Attachment,
        ]
    );
    assert_eq!(
        claude_records[0].blocks[4].text_availability,
        TranscriptBlockTextAvailability::UnavailableText
    );
    assert_eq!(
        codex_records
            .iter()
            .flat_map(|record| record.blocks.iter().map(|block| block.kind))
            .collect::<Vec<_>>(),
        vec![
            TranscriptBlockKind::UserPrompt,
            TranscriptBlockKind::UserPrompt,
            TranscriptBlockKind::AgentResponse,
            TranscriptBlockKind::Inference,
            TranscriptBlockKind::ToolCall,
            TranscriptBlockKind::ToolCall,
            TranscriptBlockKind::ToolCall,
            TranscriptBlockKind::ToolResult,
            TranscriptBlockKind::ToolResult,
            TranscriptBlockKind::ToolResult,
        ]
    );
    assert_eq!(
        pi_records[0]
            .blocks
            .iter()
            .map(|block| block.kind)
            .collect::<Vec<_>>(),
        vec![
            TranscriptBlockKind::Inference,
            TranscriptBlockKind::AgentResponse,
            TranscriptBlockKind::ToolCall,
            TranscriptBlockKind::ToolResult,
        ]
    );
}

#[test]
fn transcript_adapters_do_not_infer_agent_response_from_untyped_or_event_records() {
    let root = TempDir::new().expect("temporary root");
    let claude = root.path().join("claude");
    let codex = root.path().join("codex");
    let pi = root.path().join("pi");
    fs::create_dir_all(&claude).expect("claude directory");
    fs::create_dir_all(&codex).expect("codex directory");
    fs::create_dir_all(&pi).expect("pi directory");
    fs::write(
        claude.join("session.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-04T00:00:00Z","type":"queue-operation","operation":"enqueue","sessionId":"redacted-session","content":"claude queued event"}
"#,
            r#"{"timestamp":"2026-04-04T00:00:01Z","type":"attachment","attachment":{"type":"selected-files","addedNames":["redacted.rs"]}}
"#,
            r#"{"timestamp":"2026-04-04T00:00:02Z","text":"claude untyped text"}
"#,
        ),
    )
    .expect("write claude event blocks");
    fs::write(
        codex.join("session.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-04T00:00:00Z","payload":{"type":"message","content":"codex no role message"}}
"#,
            r#"{"timestamp":"2026-04-04T00:00:01Z","payload":{"type":"context_compacted"}}
"#,
        ),
    )
    .expect("write codex event blocks");
    fs::write(
        pi.join("run-history.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-04T00:00:00Z","type":"custom_message","customType":"agent-result","content":"pi custom event","display":true,"details":{"status":"redacted"}}
"#,
            r#"{"timestamp":"2026-04-04T00:00:01Z","output":"pi untyped output"}
"#,
        ),
    )
    .expect("write pi event blocks");

    let claude_outcome = ClaudeJsonlRootReader::new(claude).read_records();
    let codex_outcome = CodexSessionRootReader::new(codex).read_records();
    let pi_outcome = PiRunHistoryRootReader::new(pi).read_records();
    assert!(claude_outcome.read_failures.is_empty());
    assert!(codex_outcome.read_failures.is_empty());
    assert!(pi_outcome.read_failures.is_empty());

    let claude_kinds = claude_outcome
        .records
        .iter()
        .flat_map(|record| record.blocks.iter().map(|block| block.kind))
        .collect::<Vec<_>>();
    let codex_kinds = codex_outcome
        .records
        .iter()
        .flat_map(|record| record.blocks.iter().map(|block| block.kind))
        .collect::<Vec<_>>();
    let pi_kinds = pi_outcome
        .records
        .iter()
        .flat_map(|record| record.blocks.iter().map(|block| block.kind))
        .collect::<Vec<_>>();

    assert_eq!(
        claude_kinds,
        vec![
            TranscriptBlockKind::SessionEvent,
            TranscriptBlockKind::Attachment,
            TranscriptBlockKind::Unclassified,
        ]
    );
    assert_eq!(
        claude_outcome.records[1].blocks[0].text_availability,
        TranscriptBlockTextAvailability::UnavailableText
    );
    assert_eq!(
        codex_kinds,
        vec![
            TranscriptBlockKind::Unclassified,
            TranscriptBlockKind::SessionEvent,
        ]
    );
    assert_eq!(
        pi_kinds,
        vec![
            TranscriptBlockKind::SessionEvent,
            TranscriptBlockKind::Unclassified,
        ]
    );
    assert!(
        claude_kinds
            .iter()
            .chain(codex_kinds.iter())
            .chain(pi_kinds.iter())
            .all(|kind| *kind != TranscriptBlockKind::AgentResponse)
    );
}

#[test]
fn transcript_block_interface_filters_searches_reads_and_rejects_stale_cursors() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    fs::write(
        root.path().join("claude/session.jsonl"),
        concat!(
            r#"{"timestamp":"2026-04-02T00:00:00Z","message":{"role":"user","content":[{"type":"text","text":"please find exact phrase"}]}}
"#,
            r#"{"timestamp":"2026-04-02T00:00:01Z","message":{"role":"assistant","content":["#,
            r#"{"type":"thinking","thinking":"alpha beta omega hidden reasoning"},"#,
            r#"{"type":"text","text":"visible agent response"},"#,
            r#"{"type":"tool_use","name":"Bash","input":{"command":"echo alpha tool"}}]}}
"#,
        ),
    )
    .expect("write transcript blocks");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-04-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let all_blocks = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("list-blocks"),
            filter: all_transcript_block_filter(),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("list transcript blocks");
    assert_eq!(
        all_blocks
            .blocks
            .iter()
            .map(|block| block.kind)
            .collect::<Vec<_>>(),
        vec![
            TranscriptBlockKind::UserPrompt,
            TranscriptBlockKind::Inference,
            TranscriptBlockKind::AgentResponse,
            TranscriptBlockKind::ToolCall,
        ]
    );

    let tool_calls = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("list-tool-calls"),
            filter: only_transcript_block_filter(TranscriptBlockKind::ToolCall),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::BoundedPreview(BoundedTextProjection {
                maximum_bytes: ByteLimit::new(12),
            }),
        })
        .expect("list tool call blocks");
    assert_eq!(tool_calls.blocks.len(), 1);
    assert!(
        tool_calls.blocks[0]
            .preview
            .as_ref()
            .is_some_and(|preview| preview.byte_count.into_u64() <= 12)
    );

    let word_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-word"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::contains(QueryTerm::word("alpha"))),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("word search");
    assert_eq!(word_search.matches.len(), 2);

    let phrase_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-phrase"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::contains(QueryTerm::phrase(vec![
                "exact".to_string(),
                "phrase".to_string(),
            ]))),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("phrase search");
    assert_eq!(phrase_search.matches.len(), 1);
    assert_eq!(
        phrase_search.matches[0].card.kind,
        TranscriptBlockKind::UserPrompt
    );

    let near_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-near"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::near(
                QueryTerm::word("alpha"),
                QueryTerm::word("omega"),
                WordDistance::new(1),
            )),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("near search");
    assert_eq!(near_search.matches.len(), 1);
    assert_eq!(
        near_search.matches[0].card.kind,
        TranscriptBlockKind::Inference
    );

    let estimated = nexus
        .estimate_transcript_block(TranscriptBlockEstimateRequest {
            request_identifier: RequestIdentifier::new("estimate-block"),
            block_reference: near_search.matches[0].card.reference.clone(),
        })
        .expect("estimate block");
    assert!(
        estimated
            .size
            .byte_count
            .is_some_and(|count| count.into_u64() > 20)
    );

    let read = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("read-block"),
            block_reference: near_search.matches[0].card.reference.clone(),
            maximum_bytes: ByteLimit::new(8),
        })
        .expect("read bounded block");
    assert_eq!(read.excerpt.text.as_str(), "alpha be");
    assert_eq!(read.excerpt.byte_count.into_u64(), 8);
    assert!(read.excerpt.truncation.is_some());

    let first_page = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("block-cursor-first"),
            filter: all_transcript_block_filter(),
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("first block page");
    let stale_kind_cursor = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("block-cursor-stale-kind"),
            filter: only_transcript_block_filter(TranscriptBlockKind::AgentResponse),
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: first_page.page.next_cursor.clone(),
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("kind change must stale the block cursor");
    assert_eq!(
        stale_kind_cursor.reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let first_search_page = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-cursor-first"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::contains(QueryTerm::word("alpha"))),
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("first search page");
    let stale_query_cursor = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-cursor-stale-query"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::contains(QueryTerm::word("phrase"))),
            page: PageRequest {
                limit: PageLimit::new(1),
                cursor: first_search_page.page.next_cursor.clone(),
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("query change must stale the search cursor");
    assert_eq!(
        stale_query_cursor.reason,
        OperationRejectionReason::FragileReferenceStale
    );
}

#[test]
fn transcript_block_reads_reject_missing_broken_stale_unavailable_and_invalid_queries() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let transcript = root.path().join("claude/session.jsonl");
    fs::write(
        &transcript,
        concat!(
            r#"{"timestamp":"2026-04-03T00:00:00Z","message":{"role":"assistant","content":["#,
            r#"{"type":"text","text":"stable block text"},"#,
            r#"{"type":"image","source":{"type":"base64"}}]}}
"#,
        ),
    )
    .expect("write transcript blocks");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(Timestamp::new("2026-04-03T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let listed = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("list-for-rejections"),
            filter: all_transcript_block_filter(),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("list blocks");
    let readable_reference = listed.blocks[0].reference.clone();
    let attachment_reference = listed.blocks[1].reference.clone();

    let missing = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("missing-block"),
            block_reference: FragileTranscriptBlockReference::new("missing-block-reference"),
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("missing block reference rejected");
    assert_eq!(missing.reason, OperationRejectionReason::Missing);

    let unavailable = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("unavailable-block"),
            block_reference: attachment_reference,
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("unavailable attachment text rejected");
    assert_eq!(unavailable.reason, OperationRejectionReason::Unsupported);

    let invalid_query = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("invalid-query"),
            filter: all_transcript_block_filter(),
            query: TranscriptBlockTextQuery::new(TextQuery::contains(QueryTerm::word("!!!"))),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect_err("empty normalized query rejected");
    assert_eq!(invalid_query.reason, OperationRejectionReason::InvalidQuery);

    fs::write(
        &transcript,
        r#"{"timestamp":"2026-04-03T00:00:00Z","message":{"role":"assistant","content":[{"type":"text","text":"changed block text"}]}}
"#,
    )
    .expect("rewrite transcript for stale reference");
    let stale = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("stale-block"),
            block_reference: readable_reference.clone(),
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("changed backing file stales the block reference");
    assert_eq!(
        stale.reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let refreshed = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: RequestIdentifier::new("refresh-after-stale"),
            filter: all_transcript_block_filter(),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::OldestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("refresh changed block");
    fs::remove_file(&transcript).expect("delete transcript for broken reference");
    let broken = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("broken-block"),
            block_reference: refreshed.blocks[0].reference.clone(),
            maximum_bytes: ByteLimit::new(16),
        })
        .expect_err("deleted backing file breaks the block reference");
    assert_eq!(
        broken.reason,
        OperationRejectionReason::FragileReferenceBroken
    );
}

#[test]
fn repository_adapter_uses_fixture_or_reports_policy_unavailable() {
    let repository = RepositoryAdapterConfiguration::new(
        RepositoryName::new("fixture-repository"),
        std::env::temp_dir(),
    );
    let fixture = RepositoryEvidenceFixture::new(vec![
        RepositoryChangeFixture::new(
            RepositoryIdentifier::new("fixture-repository"),
            SignalFilesystemPath::new("/fixture/repository"),
            vec![RepositoryPath::new("src/lib.rs")],
            RepositoryWorktreeState::HasChanges,
        ),
        RepositoryChangeFixture::new(
            RepositoryIdentifier::new("other"),
            SignalFilesystemPath::new("/fixture/other"),
            vec![RepositoryPath::new("README.md")],
            RepositoryWorktreeState::Clean,
        ),
    ]);
    let fixture_outcome = RepositoryAdapter::fixture(vec![repository.clone()], fixture).collect();
    assert_eq!(fixture_outcome.repository_changes.len(), 1);
    let unavailable_outcome =
        RepositoryAdapter::command_policy(vec![repository], RepositoryCommandPolicy::unavailable())
            .collect();
    assert_eq!(unavailable_outcome.read_failures.len(), 1);
}

fn materialize_recovery_fixtures(root: &std::path::Path) -> (String, String, String, String) {
    let parent = root.join("claude-parent");
    let subagents = root.join("claude-subagents/claude-session-uuid");
    let empty = root.join("empty-root");
    let malformed = root.join("malformed-root");
    fs::create_dir_all(&parent).expect("parent fixture directory");
    fs::create_dir_all(&subagents).expect("subagent fixture directory");
    fs::create_dir_all(&empty).expect("empty fixture directory");
    fs::create_dir_all(&malformed).expect("malformed fixture directory");
    fs::write(
        parent.join("session-uuid.jsonl"),
        include_str!("fixtures/claude-parent/session-uuid.jsonl"),
    )
    .expect("parent fixture");
    fs::write(
        subagents.join("task-1.output"),
        include_str!("fixtures/claude-subagents/claude-session-uuid/task-1.output"),
    )
    .expect("subagent output fixture");
    fs::write(
        malformed.join("malformed.jsonl"),
        include_str!("fixtures/malformed-root/malformed.jsonl"),
    )
    .expect("malformed fixture");
    (
        parent.display().to_string(),
        root.join("claude-subagents").display().to_string(),
        empty.display().to_string(),
        malformed.display().to_string(),
    )
}

fn transcript_only_configuration(
    store_path: &std::path::Path,
    parent: String,
    subagents: String,
) -> AggregatorConfiguration {
    let mut configuration = ConfigurationFixture::minimal();
    configuration.store_path = FilesystemPath::new(store_path.display().to_string());
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![
        TranscriptSource::Claude(TranscriptRoot {
            path: FilesystemPath::new(parent),
        }),
        TranscriptSource::ClaudeSubagentOutput(TranscriptRoot {
            path: FilesystemPath::new(subagents),
        }),
    ];
    configuration
}

#[test]
fn runtime_configuration_accepts_transcript_only_configuration() {
    let temp = TempDir::new().expect("tempdir");
    let (parent, subagents, _, _) = materialize_recovery_fixtures(temp.path());
    let configuration =
        transcript_only_configuration(&temp.path().join("store"), parent, subagents);
    let validation = RuntimeConfiguration::validate_from_meta(&configuration);
    assert!(
        matches!(validation, RuntimeConfigurationValidation::Accepted(_)),
        "transcript-only configuration should be accepted: {validation:?}"
    );
}

#[test]
fn health_and_subagent_output_recovery_use_configured_fixture_roots() {
    let temp = TempDir::new().expect("tempdir");
    let (parent, subagents, _, _) = materialize_recovery_fixtures(temp.path());
    let configuration =
        transcript_only_configuration(&temp.path().join("store"), parent, subagents);
    let runtime_configuration = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
        other => panic!("expected accepted configuration, got {other:?}"),
    };
    let nexus = NexusPlane::with_runtime_configuration(
        runtime_configuration,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    );

    let health = nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: RequestIdentifier::new("health-fixture"),
        })
        .expect("health observed");
    assert!(
        health
            .sources
            .iter()
            .any(|source| source.source == SourceKind::ClaudeSubagentOutput
                && source.status == SourceHealthStatus::ReadableIndexed),
        "configured Claude subagent .output fixture should be indexed: {health:?}"
    );

    let sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: RequestIdentifier::new("sessions-fixture"),
            filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window: None,
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::NewestFirst,
            },
        })
        .expect("sessions");
    assert_eq!(
        sessions.sessions.len(),
        2,
        "equal producer identifiers from configured sources must remain source-scoped"
    );
    let subagent_session = sessions
        .sessions
        .iter()
        .find(|session| session.source == SourceKind::ClaudeSubagentOutput)
        .expect("subagent-output source retains its own session card");

    let subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: RequestIdentifier::new("subagents-fixture"),
            filter: SubagentListFilter {
                session_reference: subagent_session.reference.clone(),
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier: None,
            },
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::NewestFirst,
            },
        })
        .expect("subagents");
    assert_eq!(subagents.subagents[0].name.as_str(), "writer");
    assert_eq!(
        subagents.subagents[0]
            .task
            .as_ref()
            .expect("task metadata")
            .task_identifier
            .as_str(),
        "task-1"
    );

    let search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: RequestIdentifier::new("search-fixture"),
            filter: transcript_block_filter(TranscriptBlockKindSelection::AllTranscriptBlockKinds),
            query: TranscriptBlockTextQuery::new(TextQuery::Contains(QueryTerm::word("quota"))),
            page: PageRequest {
                limit: PageLimit::new(10),
                cursor: None,
                order: ListingOrder::NewestFirst,
            },
            projection: CardProjection::MetadataOnly,
        })
        .expect("search");
    assert_eq!(search.matches.len(), 1);
    let read = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: RequestIdentifier::new("read-fixture"),
            block_reference: search.matches[0].card.reference.clone(),
            maximum_bytes: ByteLimit::new(256),
        })
        .expect("read block");
    assert!(read.excerpt.text.as_str().contains("quota"));
}

#[test]
fn health_reports_unreadable_durable_index_store() {
    let temp = TempDir::new().expect("tempdir");
    let (_, _, empty, _) = materialize_recovery_fixtures(temp.path());
    let store_path = temp.path().join("store");
    let index_store = typed_index_store(&store_path);
    fs::write(index_store.pointer_path(), "not-json").expect("write unreadable index fixture");
    let mut configuration = ConfigurationFixture::minimal();
    configuration.store_path = FilesystemPath::new(store_path.display().to_string());
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![TranscriptSource::Claude(TranscriptRoot {
        path: FilesystemPath::new(empty),
    })];
    let runtime_configuration = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
        other => panic!("expected accepted configuration, got {other:?}"),
    };

    let health = NexusPlane::with_runtime_configuration(
        runtime_configuration,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    )
    .observe_health(RuntimeHealthRequest {
        request_identifier: RequestIdentifier::new("health-index-store-unreadable"),
    })
    .expect("health");

    assert_eq!(
        health.index.status,
        SourceHealthStatus::IndexStoreUnreadable
    );
}

#[test]
fn live_index_reconciles_current_evidence_idempotently_and_removes_stale_records() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let transcript = root.path().join("claude/session.jsonl");
    fs::write(
        &transcript,
        "{\"timestamp\":\"2026-07-09T10:00:00Z\",\"text\":\"first evidence\"}\n",
    )
    .expect("write first evidence");
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let index_store = typed_index_store(runtime.store_path());
    let nexus = NexusPlane::with_runtime_configuration(
        runtime,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let request = TranscriptBlockListRequest {
        request_identifier: RequestIdentifier::new("reconcile-current-evidence"),
        filter: all_transcript_block_filter(),
        page: PageRequest {
            limit: PageLimit::new(10),
            cursor: None,
            order: ListingOrder::OldestFirst,
        },
        projection: CardProjection::MetadataOnly,
    };

    let first = nexus
        .list_transcript_blocks(request.clone())
        .expect("first refresh");
    let first_bytes = fs::read(index_store.pointer_path()).expect("first index bytes");
    let repeated = nexus
        .list_transcript_blocks(request.clone())
        .expect("identical refresh");
    assert_eq!(repeated.blocks, first.blocks);
    assert_eq!(
        fs::read(index_store.pointer_path()).expect("repeated index bytes"),
        first_bytes
    );

    fs::write(
        &transcript,
        "{\"timestamp\":\"2026-07-09T10:00:00Z\",\"text\":\"replacement evidence\"}\n",
    )
    .expect("replace evidence");
    let replacement = nexus
        .list_transcript_blocks(request.clone())
        .expect("replacement refresh");
    assert_eq!(replacement.blocks.len(), 1);
    assert_ne!(replacement.blocks[0].reference, first.blocks[0].reference);
    let replacement_pointer = aggregator::output_index::store::IndexStore::new(
        index_store.pointer_path().to_path_buf(),
        aggregator::output_index::limits::IndexStoreLimits::default(),
    )
    .read_current_pointer()
    .expect("read v3 pointer")
    .expect("published v3 pointer");
    assert_eq!(replacement_pointer.format_version, 3);
    assert_eq!(
        IndexSnapshot::from_typed_store(&index_store)
            .expect("read typed replacement")
            .output_records()
            .count(),
        1
    );

    fs::remove_file(&transcript).expect("remove evidence");
    let removed = nexus
        .list_transcript_blocks(request)
        .expect("deletion refresh");
    assert!(removed.blocks.is_empty());
    let removed_bytes = fs::read(index_store.pointer_path()).expect("deletion pointer bytes");
    assert!(
        IndexSnapshot::from_typed_store(&index_store)
            .expect("read typed deletion")
            .output_records()
            .next()
            .is_none()
    );
    assert_ne!(removed_bytes, first_bytes);
}

#[test]
fn truncated_scan_preserves_last_complete_live_index_without_erasing_scope() {
    let root = TempDir::new().expect("temporary root");
    let mut configuration = accepted_configuration(&root);
    configuration.transcript_sources = vec![TranscriptSource::Claude(TranscriptRoot {
        path: FilesystemPath::new(root.path().join("claude").display().to_string()),
    })];
    for index in 0..2 {
        fs::write(
            root.path().join("claude").join(format!("{index}.jsonl")),
            format!(
                "{{\"timestamp\":\"2026-07-09T10:00:0{index}Z\",\"text\":\"evidence {index}\"}}\n"
            ),
        )
        .expect("write complete evidence");
    }
    let complete_runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("complete configuration")
        .clone();
    let index_store = typed_index_store(complete_runtime.store_path());
    let complete_nexus = NexusPlane::with_runtime_configuration(
        complete_runtime,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let list_request = SessionListRequest {
        request_identifier: RequestIdentifier::new("complete-coverage"),
        filter: SessionListFilter {
            source_selection: SourceSelection::AllConfigured,
            time_window: None,
        },
        page: PageRequest {
            limit: PageLimit::new(10),
            cursor: None,
            order: ListingOrder::OldestFirst,
        },
    };
    assert_eq!(
        complete_nexus
            .list_sessions(list_request.clone())
            .expect("complete refresh")
            .sessions
            .len(),
        2
    );
    let complete_bytes = fs::read(index_store.pointer_path()).expect("complete index bytes");

    let mut limited_configuration = configuration;
    limited_configuration
        .output_interfaces
        .limits
        .maximum_transcript_discovered_files = ItemCount::new(1);
    let limited_runtime = RuntimeConfiguration::validate_from_meta(&limited_configuration)
        .accepted_configuration()
        .expect("limited configuration")
        .clone();
    let limited_nexus = NexusPlane::with_runtime_configuration(
        limited_runtime,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let preserved = limited_nexus
        .list_sessions(list_request)
        .expect("truncated scan uses last complete index");
    assert_eq!(preserved.sessions.len(), 2);
    assert_eq!(
        fs::read(index_store.pointer_path()).expect("preserved index bytes"),
        complete_bytes
    );
    let health = limited_nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: RequestIdentifier::new("truncated-coverage-health"),
        })
        .expect("truncated health");
    assert!(
        health
            .sources
            .iter()
            .any(|source| source.status == SourceHealthStatus::DiscoveryTruncated)
    );
}

#[test]
fn health_distinguishes_empty_and_malformed_fixture_roots() {
    let temp = TempDir::new().expect("tempdir");
    let (_, _, empty, malformed) = materialize_recovery_fixtures(temp.path());
    let mut configuration = ConfigurationFixture::minimal();
    configuration.store_path = FilesystemPath::new(temp.path().join("store").display().to_string());
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![
        TranscriptSource::Claude(TranscriptRoot {
            path: FilesystemPath::new(empty),
        }),
        TranscriptSource::Claude(TranscriptRoot {
            path: FilesystemPath::new(malformed),
        }),
    ];
    let runtime_configuration = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
        other => panic!("expected accepted configuration, got {other:?}"),
    };
    let nexus = NexusPlane::with_runtime_configuration(
        runtime_configuration,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(Timestamp::new("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    );
    let health = nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: RequestIdentifier::new("health-empty-malformed"),
        })
        .expect("health");
    assert!(
        health
            .sources
            .iter()
            .any(|source| source.status == SourceHealthStatus::ReadableEmpty)
    );
    assert!(health.sources.iter().any(|source| source.status
        == SourceHealthStatus::MalformedRecords
        && source.discovered_files.into_u64() == 1
        && source.malformed_records.into_u64() > 0));

    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: RequestIdentifier::new("inventory-empty-malformed"),
            source_selection: SourceSelection::AllConfigured,
            archive_path: None,
        })
        .expect("inventory");
    assert_eq!(
        inventory.scan_report.completeness,
        SessionInventoryCompleteness::Resumable
    );
    assert!(inventory.scan_report.sources.iter().any(|source| {
        source.completeness == SessionInventoryCompleteness::Resumable
            && source.discovered_files.into_u64() == 1
    }));
}
