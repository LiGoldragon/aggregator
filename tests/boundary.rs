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
    AdapterKind, CollectionClock, ConfigurationFixture, ConfigurationStore, Error,
    FragileIndexStore, NexusPlane, ReferenceTime, RepositoryAdapterConfiguration,
    RuntimeConfiguration, RuntimeConfigurationValidation, SemaPlane, SignalPlane,
    TranscriptAdapterConfiguration, TranscriptRootConfiguration,
    adapter::{
        MaximumDiscoveredFiles, MaximumFileBytes, MaximumLineBytes, MaximumReadFailures,
        MaximumScanEntries, TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord,
        TranscriptScanLimitConfiguration, TranscriptScanLimits,
        claude::{ClaudeJsonlRootReader, ClaudeTranscriptAdapter},
        codex::{CodexSessionRootReader, CodexTranscriptAdapter},
        pi::PiTranscriptAdapter,
        repository::{
            RepositoryAdapter, RepositoryChangeFixture, RepositoryCommandPolicy,
            RepositoryEvidenceFixture,
        },
    },
    configuration::LegacyAggregatorConfiguration,
    daemon::{PrototypeDaemon, PrototypeSocket},
};
use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange,
    ConfigurationObservation, FilesystemPath, LegacyRecoveryAccess, LegacyRecoveryRoot,
    LegacyRecoverySource, MetaAggregatorReply, MetaAggregatorRequest, ObserveConfiguration,
    OutputInterfaceConfiguration, RepositoryName, SocketMode, TranscriptRoot, TranscriptSource,
};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::{
    AggregatorReply, AggregatorRequest, AuthoredStatus, AuthoredStatusFilter,
    BoundedTextProjection, ByteCount, ByteLimit, ByteRange, CardProjection, ContractName,
    DurationAmount, DurationUnit, EvidenceRequest, FilesystemPath as SignalFilesystemPath,
    FragileOutputReference, LimitPolicy, ListingOrder, OperationRejectionReason, OutputListFilter,
    OutputListRequest, OutputReadRange, OutputReadRequest, OutputSegmentListFilter,
    OutputSegmentListRequest, PageLimit, PageRequest, Projection, ReadFailureReason,
    RejectionReason, RelativeDuration, RepositoryIdentifier, RepositoryPath,
    RepositoryWorktreeState, RequestIdentifier, SegmentLimit, SegmentProjection, SelectedSources,
    SessionListFilter, SessionListRequest, SourceIdentifier, SourceKind, SourceSelection,
    SubagentListFilter, SubagentListRequest, TimeRange, TimeWindow, Timestamp, TruncationReason,
    Version,
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
    let index_store = FragileIndexStore::from_store_path(runtime.store_path());
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
    assert!(index_store.path().exists());

    let subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: RequestIdentifier::new("list-subagents"),
            filter: SubagentListFilter {
                session_reference: sessions.sessions[0].reference.clone(),
                authored_status: AuthoredStatusFilter::AnyAuthoredStatus,
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

    let all_sessions = nexus
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
    let session_with_subagents = all_sessions
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
