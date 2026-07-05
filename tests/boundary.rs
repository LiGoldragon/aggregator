use std::{
    fs,
    io::Write,
    net::Shutdown,
    os::unix::{fs::PermissionsExt, net::UnixStream},
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
        TranscriptReadOutcome, TranscriptReadRequest, TranscriptRecord,
        claude::ClaudeTranscriptAdapter,
        codex::CodexTranscriptAdapter,
        pi::PiTranscriptAdapter,
        repository::{
            RepositoryAdapter, RepositoryChangeFixture, RepositoryCommandPolicy,
            RepositoryEvidenceFixture,
        },
    },
    daemon::{PrototypeDaemon, PrototypeSocket},
};
use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange,
    ConfigurationObservation, FilesystemPath, MetaAggregatorReply, MetaAggregatorRequest,
    ObserveConfiguration, RepositoryName, SocketMode, TranscriptRoot, TranscriptSource,
};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::{
    AggregatorReply, AggregatorRequest, BoundedTextProjection, ByteLimit, ContractName,
    DurationAmount, DurationUnit, EvidenceRequest, FilesystemPath as SignalFilesystemPath,
    LimitPolicy, Projection, ReadFailureReason, RejectionReason, RelativeDuration,
    RepositoryIdentifier, RepositoryPath, RepositoryWorktreeState, RequestIdentifier, SegmentLimit,
    SegmentProjection, SelectedSources, SourceIdentifier, SourceKind, SourceSelection, TimeRange,
    TimeWindow, Timestamp, TruncationReason, Version,
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
