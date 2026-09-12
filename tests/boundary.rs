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
    AdapterKind, CollectionClock, ConfigurationFixture, ConfigurationStore,
    ContractQueryProjection, EngineQueryProjection, Error, NexusPlane, ReferenceTime,
    RepositoryAdapterConfiguration, RuntimeConfiguration, RuntimeConfigurationValidation,
    SemaPlane, SignalPlane, TranscriptAdapterConfiguration, TranscriptRootConfiguration,
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
    daemon::{PrototypeDaemon, PrototypeSocket},
    output_index::{
        PersistentIndex, SourceHealthObserver, limits::IndexStoreLimits, store::IndexStore,
    },
    wire::{DatomText, DatomTextual},
};
use dotos_text_query::{Query as TextQuery, QueryTerm, WordDistance};
use meta_signal_aggregator::{
    ActiveRepository, AggregatorConfiguration, ConfigurationCandidate, ConfigurationChange,
    ConfigurationObservation, ConfigurationObservationQuery, DefaultingPolicy,
    LegacyRecoveryAccess, LegacyRecoveryRoot, LegacyRecoverySource, OutputInterfaceConfiguration,
    OutputInterfaceLimitPolicy, Query as MetaQuery, Response as MetaResponse, TranscriptRoot,
    TranscriptSource,
};
use signal_aggregator::{
    ArchiveTextCompleteness, AuthoredStatus, AuthoredStatusFilter, BoundedTextProjection,
    ByteRange, CardProjection, DurationUnit, EvidenceRequest, LimitPolicy, ListingOrder,
    OperationRejectionReason, OutputListFilter, OutputListRequest, OutputReadRange,
    OutputReadRequest, OutputSegmentListFilter, OutputSegmentListRequest, PageRequest, Projection,
    Query, ReadFailureReason, RejectionReason, RelativeDuration, RepositoryWorktreeState, Response,
    RuntimeHealthRequest, ScanLimitKind, SegmentProjection, SelectedSources,
    SessionArchiveQueryRequest, SessionArchiveReadRequest, SessionArchiveRecordDraft,
    SessionArchiveStatus, SessionArchiveWriteRequest, SessionInventoryCompleteness,
    SessionInventoryRequest, SessionLifecycleStatus, SessionListFilter, SessionListRequest,
    SessionLookupRequest, SessionLookupSelector, SizeCertainty, SourceHealthStatus, SourceKind,
    SourceSelection, SubagentListFilter, SubagentListRequest, TimeRange, TimeWindow,
    TranscriptBlockEstimateRequest, TranscriptBlockFilter, TranscriptBlockKind,
    TranscriptBlockKindSelection, TranscriptBlockListRequest, TranscriptBlockReadRequest,
    TranscriptBlockSearchRequest, TranscriptBlockTextAvailability, TranscriptBlockTextQuery,
    TruncationReason, VersionQuery,
};
use tempfile::TempDir;

fn evidence_request() -> EvidenceRequest {
    EvidenceRequest {
        request_identifier: String::from("req-test"),
        time_window: TimeWindow::Recent(RelativeDuration {
            duration_amount: 1,
            duration_unit: DurationUnit::Hours,
        }),
        source_selection: SourceSelection::AllConfigured,
        projection: Projection::MetadataOnly,
        limit_policy: LimitPolicy {
            maximum_segments: 8,
            maximum_bytes: 1024,
        },
    }
}

#[test]
fn example_collect_query_carries_the_written_window_and_limits() {
    let query = DatomText::read::<Query>(
        "example collect",
        include_str!("../examples/collect.datom").trim(),
    )
    .expect("example collect must actualize");
    let Query::Collect(request) = query else {
        panic!("examples/collect.datom must carry a Collect query");
    };
    assert_eq!(request.request_identifier, "req-20260705");
    assert_eq!(
        request.time_window,
        TimeWindow::Recent(RelativeDuration {
            duration_amount: 6,
            duration_unit: DurationUnit::Hours,
        })
    );
    assert_eq!(request.limit_policy.maximum_segments, 32);
    assert_eq!(request.limit_policy.maximum_bytes, 4096);
}

#[test]
fn example_search_query_carries_a_flat_arena_the_engine_accepts() {
    let query = DatomText::read::<Query>(
        "example search",
        include_str!("../examples/transcript-block-search.datom").trim(),
    )
    .expect("example search must actualize");
    let Query::SearchTranscriptBlocks(request) = query else {
        panic!("examples/transcript-block-search.datom must carry a search query");
    };
    assert_eq!(
        request.transcript_block_text_query.text_query_nodes.len(),
        3
    );
    assert_eq!(
        ContractQueryProjection::new(&request.transcript_block_text_query)
            .project()
            .expect("the example arena must project"),
        TextQuery::all_of(vec![
            TextQuery::near(
                QueryTerm::word("quota"),
                QueryTerm::word("reset"),
                WordDistance::new(6),
            ),
            TextQuery::contains(QueryTerm::phrase(vec![
                String::from("rate"),
                String::from("limit"),
            ])),
        ])
    );
}

#[test]
fn example_response_carries_the_written_excerpt() {
    let response = DatomText::read::<Response>(
        "example response",
        include_str!("../examples/transcript-block-read.datom").trim(),
    )
    .expect("example response must actualize");
    let Response::TranscriptBlockRead(read) = response else {
        panic!("examples/transcript-block-read.datom must carry a TranscriptBlockRead");
    };
    assert_eq!(
        read.transcript_text_excerpt.transcript_text,
        "quota reset after cooldown"
    );
    assert_eq!(read.transcript_text_excerpt.byte_count, 26);
    assert_eq!(read.size_metadata.size_certainty, SizeCertainty::Exact);
}

#[test]
fn example_configuration_carries_the_written_sockets_and_sources() {
    let configuration = DatomText::read::<AggregatorConfiguration>(
        "example configuration",
        include_str!("../examples/configuration.datom").trim(),
    )
    .expect("example configuration must actualize");
    assert_eq!(
        configuration.ordinary_socket_path,
        "/run/aggregator/aggregator.sock"
    );
    assert_eq!(configuration.ordinary_socket_mode, 0o660);
    assert_eq!(configuration.meta_socket_mode, 0o600);
    assert_eq!(configuration.transcript_sources.len(), 2);
    assert_eq!(
        configuration
            .output_interface_configuration
            .output_interface_limit_policy
            .maximum_page_items,
        64
    );
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
            maximum_segments: aggregator::MeasuredCount::contract_count(maximum_segments),
            maximum_bytes: aggregator::MeasuredCount::contract_count(maximum_bytes),
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
        fragile_session_reference_option: None,
        fragile_subagent_reference_option: None,
        task_identifier_option: None,
        transcript_block_kind_selection: kind_selection,
        authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
        time_window_option: None,
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
        signal_aggregator::SelectedTranscriptBlockKinds {
            transcript_block_kinds: vec![kind],
        },
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
        ordinary_socket_path: root.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        meta_socket_path: root.path().join("meta.sock").display().to_string(),
        meta_socket_mode: 0o600,
        store_path: root.path().join("store.sema").display().to_string(),
        active_repositories: vec![ActiveRepository {
            repository_name: String::from("fixture-repository"),
            filesystem_path: repository.display().to_string(),
        }],
        transcript_sources: vec![
            TranscriptSource::Claude(TranscriptRoot {
                filesystem_path: claude.display().to_string(),
            }),
            TranscriptSource::Codex(TranscriptRoot {
                filesystem_path: codex.display().to_string(),
            }),
        ],
        default_projection: Projection::MetadataOnly,
        default_limit_policy: LimitPolicy {
            maximum_segments: 16,
            maximum_bytes: 4096,
        },
        output_interface_configuration: OutputInterfaceConfiguration::default_policy(),
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
        RepositoryAdapterConfiguration::new(String::from("primary"), std::env::temp_dir());
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
        String::from("req-test"),
        RejectionReason::CollectionUnavailable,
    );
    let text = DatomText::print(&reply);
    assert!(matches!(reply, Response::EvidenceRejected(_)));
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
fn configuration_fixture_round_trips_through_datom() {
    let configuration = ConfigurationFixture::minimal();
    let text = DatomText::print(&configuration);
    let decoded =
        DatomText::read::<meta_signal_aggregator::AggregatorConfiguration>("configuration", &text)
            .expect("decode configuration");
    assert_eq!(decoded, configuration);
}

#[test]
fn sema_scaffold_observes_configuration_and_rejects_configuration_without_store() {
    let root = TempDir::new().expect("temporary root");
    let mut sema = SemaPlane::empty();
    assert!(matches!(
        sema.observe_configuration(),
        MetaResponse::ConfigurationObserved(_)
    ));
    let configured = sema.configure(ConfigurationChange {
        aggregator_configuration: accepted_configuration(&root),
    });
    assert!(matches!(
        configured,
        MetaResponse::ConfigurationRejected(rejection)
            if rejection.configuration_rejection_reason == meta_signal_aggregator::ConfigurationRejectionReason::StoreUnavailable
    ));
}

#[test]
fn sema_configure_persists_typed_configuration_when_store_is_available() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let store = ConfigurationStore::at_path(root.path().join("configuration.datom"));
    let mut sema = SemaPlane::with_configuration_store(configuration.clone(), store.clone());

    let configured = sema.configure(ConfigurationChange {
        aggregator_configuration: configuration.clone(),
    });

    assert!(matches!(
        configured,
        MetaResponse::ConfigurationConfigured(_)
    ));
    assert_eq!(
        store.read_configuration().expect("persisted configuration"),
        configuration
    );
}

#[test]
fn configuration_store_round_trips_datom_file_storage() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let store = ConfigurationStore::at_path(root.path().join("configuration.datom"));
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

    let error = PrototypeSocket::new(socket_path.clone(), 0o660)
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
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
        source_kinds: vec![SourceKind::Claude, SourceKind::Repository],
    }));
    assert_eq!(selected.repositories.len(), 1);
    assert_eq!(selected.transcript_sources.len(), 1);
    assert!(matches!(
        selected.transcript_sources.first(),
        Some(TranscriptAdapterConfiguration::Claude(_))
    ));
}

#[test]
fn runtime_configuration_rejects_output_limits_above_runtime_ceilings() {
    let root = TempDir::new().expect("temporary root");
    let mut configuration = accepted_configuration(&root);
    let ceiling = OutputInterfaceLimitPolicy::default_policy();
    configuration
        .output_interface_configuration
        .output_interface_limit_policy = OutputInterfaceLimitPolicy {
        maximum_page_items: ceiling.maximum_page_items + 1,
        maximum_preview_bytes: ceiling.maximum_preview_bytes + 1,
        maximum_read_bytes: ceiling.maximum_read_bytes + 1,
        maximum_recovery_files_per_root: ceiling.maximum_recovery_files_per_root + 1,
        maximum_transcript_scan_entries: ceiling.maximum_transcript_scan_entries + 1,
        maximum_transcript_discovered_files: ceiling.maximum_transcript_discovered_files + 1,
        maximum_transcript_file_bytes: ceiling.maximum_transcript_file_bytes + 1,
        maximum_transcript_line_bytes: ceiling.maximum_transcript_line_bytes + 1,
        maximum_transcript_read_failures: ceiling.maximum_transcript_read_failures + 1,
    };

    let report = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(_) => panic!("oversized limits were accepted"),
        RuntimeConfigurationValidation::Rejected(report) => report,
    };
    let rejected_limit_count = report
        .configuration_validation_issues
        .iter()
        .filter(|issue| {
            issue.configuration_validation_issue_kind
                == meta_signal_aggregator::ConfigurationValidationIssueKind::InvalidOutputInterfaceLimit
        })
        .count();
    assert_eq!(
        rejected_limit_count, 9,
        "every output, scan, discovery, and recovery collection has a hard runtime ceiling"
    );
}

#[test]
fn runtime_configuration_reports_missing_paths_as_validation_issues() {
    let root = TempDir::new().expect("temporary root");
    let mut configuration = accepted_configuration(&root);
    configuration.transcript_sources = vec![TranscriptSource::Pi(TranscriptRoot {
        filesystem_path: root.path().join("missing-pi").display().to_string(),
    })];
    let validation = RuntimeConfiguration::validate_from_meta(&configuration);
    let report = match validation {
        RuntimeConfigurationValidation::Accepted(_) => panic!("missing path was accepted"),
        RuntimeConfigurationValidation::Rejected(report) => report,
    };
    assert!(
        report
            .configuration_validation_issues
            .iter()
            .any(|issue| issue.configuration_validation_issue_kind
                == meta_signal_aggregator::ConfigurationValidationIssueKind::UnreadablePath)
    );
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
            start_timestamp: String::from("2026-01-02T00:00:00Z"),
            end_timestamp: String::from("2026-01-02T23:59:59Z"),
        }),
        Projection::BoundedText(BoundedTextProjection { maximum_bytes: 6 }),
        1,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
        ReadFailureReason::Malformed
    );
    assert!(!outcome.truncations.is_empty());
    match &outcome.transcript_segments[0].segment_projection {
        SegmentProjection::Text(excerpt) => assert_eq!(excerpt.transcript_text.as_str(), "hello "),
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.truncations.len(), 1);
    let truncated_path = outcome.truncations[0].filesystem_path_option.as_deref();
    let expected_path = root.path().join("too-large.jsonl").display().to_string();
    assert_eq!(truncated_path, Some(expected_path.as_str()));
    assert!(
        outcome.truncations[0]
            .original_bytes
            .as_ref()
            .is_some_and(|count| *count > 32)
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
        ReadFailureReason::Malformed
    );
    assert!(outcome.truncations.len() >= 3);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("second.jsonl"))
    }));
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .filesystem_path_option
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
        limit.scan_limit_kind == ScanLimitKind::DiscoveredFiles(limit.scan_limit)
            && limit.scan_limit == 2
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(outcome.read_failures.iter().all(|failure| {
        failure.read_failure_reason == ReadFailureReason::PermissionDenied
            && failure
                .filesystem_path_option
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
            TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
            Projection::MetadataOnly,
            8,
        ));

    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(
        outcome.transcript_segments[0]
            .filesystem_path
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.read_failures.is_empty());
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(
        outcome.transcript_segments[0].source_kind,
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));
    assert!(ordinary.transcript_segments.is_empty());
    assert!(ordinary.read_failures.is_empty());

    let subagent =
        ClaudeJsonlRootReader::subagent_output(root.path().to_path_buf()).collect(&read_request(
            TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
            Projection::MetadataOnly,
            8,
        ));
    assert_eq!(subagent.transcript_segments.len(), 1);
    assert_eq!(
        subagent.transcript_segments[0].source_kind,
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
        ReadFailureReason::Malformed
    );
    assert!(
        outcome.read_failures[0]
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("project.jsonl:1"))
    );
}

#[test]
fn signal_plane_rejects_non_canonical_time_windows() {
    let request = EvidenceRequest {
        time_window: TimeWindow::Since(String::from("2026-01-02T01:00:00+01:00")),
        ..evidence_request()
    };

    let rejection = SignalPlane
        .collect_rejection(&request)
        .expect("offset window must be rejected");
    assert!(
        matches!(rejection, Response::EvidenceRejected(rejection) if rejection.rejection_reason == RejectionReason::InvalidTimeWindow)
    );
}

#[test]
fn recent_time_window_does_not_accept_old_or_timestampless_records() {
    let source_identifier = String::from("fixture-source");
    let request = read_request(
        TimeWindow::Recent(RelativeDuration {
            duration_amount: 1,
            duration_unit: DurationUnit::Hours,
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
                Some(String::from("2000-01-01T00:00:00Z")),
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
            duration_amount: 1,
            duration_unit: DurationUnit::Hours,
        }),
        Projection::MetadataOnly,
        8,
    ));

    assert!(outcome.transcript_segments.is_empty());
    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let request = EvidenceRequest {
        source_selection: SourceSelection::Only(SelectedSources {
            source_kinds: vec![SourceKind::Claude],
        }),
        ..evidence_request()
    };
    let package = NexusPlane::with_runtime_configuration(runtime, clock)
        .collect(request)
        .expect("collect through nexus");

    assert_eq!(package.transcript_segments.len(), 1);
    assert_eq!(
        package.transcript_segments[0].timestamp_option.as_deref(),
        Some("2026-01-02T00:30:00Z")
    );
    assert!(
        !package
            .read_failures
            .iter()
            .any(|failure| failure.read_failure_reason == ReadFailureReason::UnsupportedFormat)
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);
    let archive_path = String::from("archive.rkyv");

    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: String::from("inventory-sessions"),
            source_selection: SourceSelection::Only(SelectedSources {
                source_kinds: vec![SourceKind::Claude],
            }),
            archive_path_option: Some(archive_path.clone()),
        })
        .expect("inventory sessions");
    assert_eq!(inventory.session_inventory_cards.len(), 1);
    assert_eq!(
        inventory
            .session_inventory_scan_report
            .session_inventory_completeness,
        SessionInventoryCompleteness::Complete
    );
    assert_eq!(inventory.session_inventory_cards[0].file_count, 1);
    assert_eq!(
        inventory.session_inventory_cards[0].session_archive_status,
        SessionArchiveStatus::ArchiveUnknown
    );
    assert_eq!(
        inventory.session_inventory_cards[0].session_lifecycle_status,
        SessionLifecycleStatus::Current
    );

    let looked_up = nexus
        .lookup_session(SessionLookupRequest {
            request_identifier: String::from("lookup-session"),
            session_lookup_selector: SessionLookupSelector::ByReference(
                inventory.session_inventory_cards[0]
                    .fragile_session_reference
                    .clone(),
            ),
            archive_path_option: None,
        })
        .expect("lookup session");
    assert_eq!(looked_up.session_inventory_cards.len(), 1);

    let written = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: String::from("write-archive"),
            archive_path: archive_path.clone(),
            session_archive_record_draft: SessionArchiveRecordDraft {
                session_inventory_card: inventory.session_inventory_cards[0].clone(),
                archive_summary_text: String::from("summary may include a direct quote"),
                archive_provenance_text: String::from("bounded transcript read references"),
                created_at: String::from("2026-01-02T01:10:00Z"),
            },
        })
        .expect("write archive");

    let duplicate_written = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: String::from("write-archive-duplicate"),
            archive_path: archive_path.clone(),
            session_archive_record_draft: SessionArchiveRecordDraft {
                session_inventory_card: inventory.session_inventory_cards[0].clone(),
                archive_summary_text: String::from("summary may include a direct quote"),
                archive_provenance_text: String::from("bounded transcript read references"),
                created_at: String::from("2026-01-02T01:10:00Z"),
            },
        })
        .expect("write duplicate archive record");
    assert_ne!(
        written
            .session_archive_record_card
            .archive_record_identifier,
        duplicate_written
            .session_archive_record_card
            .archive_record_identifier
    );

    let queried = nexus
        .query_session_archive(SessionArchiveQueryRequest {
            request_identifier: String::from("query-archive"),
            archive_path: archive_path.clone(),
            fragile_session_reference_option: Some(
                inventory.session_inventory_cards[0]
                    .fragile_session_reference
                    .clone(),
            ),
        })
        .expect("query archive");
    assert_eq!(
        queried.session_archive_record_cards,
        vec![
            written.session_archive_record_card.clone(),
            duplicate_written.session_archive_record_card.clone()
        ]
    );

    let read = nexus
        .read_session_archive(SessionArchiveReadRequest {
            request_identifier: String::from("read-archive"),
            archive_path,
            archive_record_identifier: written
                .session_archive_record_card
                .archive_record_identifier
                .clone(),
            maximum_summary_bytes: 12,
            maximum_provenance_bytes: 64,
        })
        .expect("read archive");
    assert_eq!(
        read.session_archive_record_projection
            .session_archive_record_card
            .archive_record_identifier,
        written
            .session_archive_record_card
            .archive_record_identifier
    );
    assert_eq!(
        read.session_archive_record_projection
            .session_archive_text_projection
            .archive_summary_text
            .as_str(),
        "summary may "
    );
    assert_eq!(
        read.session_archive_record_projection
            .session_archive_text_projection
            .archive_text_completeness,
        ArchiveTextCompleteness::Truncated
    );
    assert_eq!(
        read.session_archive_record_projection
            .session_archive_provenance_projection
            .archive_provenance_text
            .as_str(),
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);
    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: String::from("inventory-for-archive-rejection"),
            source_selection: SourceSelection::Only(SelectedSources {
                source_kinds: vec![SourceKind::Claude],
            }),
            archive_path_option: None,
        })
        .expect("inventory sessions");
    let rejected = nexus
        .write_session_archive(SessionArchiveWriteRequest {
            request_identifier: String::from("write-outside-archive-root"),
            archive_path: root.path().join("outside.rkyv").display().to_string(),
            session_archive_record_draft: SessionArchiveRecordDraft {
                session_inventory_card: inventory.session_inventory_cards[0].clone(),
                archive_summary_text: String::from("summary"),
                archive_provenance_text: String::from("provenance"),
                created_at: String::from("2026-01-02T01:10:00Z"),
            },
        })
        .expect_err("outside archive path must be rejected");
    assert_eq!(
        rejected.operation_rejection_reason,
        OperationRejectionReason::Unauthorized
    );

    let archive_root = root.path().join("session-archive");
    fs::create_dir_all(&archive_root).expect("archive root");
    let outside = root.path().join("outside-symlink-target.rkyv");
    fs::write(&outside, "not an archive").expect("outside target");
    symlink(&outside, archive_root.join("linked.rkyv")).expect("archive symlink");
    let symlink_rejected = nexus
        .query_session_archive(SessionArchiveQueryRequest {
            request_identifier: String::from("query-symlink-archive"),
            archive_path: String::from("linked.rkyv"),
            fragile_session_reference_option: None,
        })
        .expect_err("archive symlink must be rejected");
    assert_eq!(
        symlink_rejected.operation_rejection_reason,
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: String::from("list-sessions"),
            session_list_filter: SessionListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    source_kinds: vec![SourceKind::Claude],
                }),
                time_window_option: Some(TimeWindow::Since(String::from("2026-01-01T00:00:00Z"))),
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect("list sessions");
    assert_eq!(sessions.session_cards.len(), 1);
    assert_eq!(
        sessions.session_cards[0].output_count.as_ref(),
        Some(2).as_ref()
    );
    assert_eq!(
        sessions.session_cards[0].last_observed_at.as_deref(),
        Some("2026-01-02T00:20:00Z")
    );
    assert!(index_store.pointer_path().exists());

    let subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: String::from("list-subagents"),
            subagent_list_filter: SubagentListFilter {
                fragile_session_reference: sessions.session_cards[0]
                    .fragile_session_reference
                    .clone(),
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier_option: None,
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect("list subagents");
    assert_eq!(subagents.subagent_cards.len(), 1);
    assert_eq!(subagents.subagent_cards[0].subagent_name.as_str(), "writer");
    assert_eq!(
        subagents.subagent_cards[0].authored_status,
        AuthoredStatus::AgentAuthored
    );

    let outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("list-outputs"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    source_kinds: vec![SourceKind::Claude],
                }),
                fragile_session_reference_option: Some(
                    sessions.session_cards[0].fragile_session_reference.clone(),
                ),
                fragile_subagent_reference_option: Some(
                    subagents.subagent_cards[0]
                        .fragile_subagent_reference
                        .clone(),
                ),
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::AgentAuthored,
                ),
                time_window_option: Some(TimeWindow::Since(String::from("2026-01-01T00:00:00Z"))),
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::BoundedPreview(BoundedTextProjection {
                maximum_bytes: 5,
            }),
        })
        .expect("list outputs");
    assert_eq!(outputs.output_cards.len(), 2);
    assert_eq!(
        outputs.output_cards[0]
            .output_text_excerpt_option
            .as_ref()
            .map(|preview| preview.output_text.as_str()),
        Some("alpha")
    );
    assert_eq!(
        outputs.output_cards[0]
            .size_metadata
            .byte_count_option
            .as_ref(),
        Some(9).as_ref()
    );

    let segments = nexus
        .list_output_segments(OutputSegmentListRequest {
            request_identifier: String::from("list-segments"),
            output_segment_list_filter: OutputSegmentListFilter {
                fragile_output_reference: outputs.output_cards[0].fragile_output_reference.clone(),
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("list output segments");
    assert_eq!(segments.output_segment_cards.len(), 1);
    assert_eq!(
        segments.output_segment_cards[0]
            .byte_range_option
            .as_ref()
            .map(|range| (range.start_byte_count, range.end_byte_count)),
        Some((0, 9))
    );
    assert_eq!(
        segments.output_segment_cards[0]
            .line_range_option
            .as_ref()
            .map(|range| (range.start_line_number, range.end_line_number)),
        Some((1, 2))
    );

    let estimated = nexus
        .estimate_output(signal_aggregator::OutputEstimateRequest {
            request_identifier: String::from("estimate-output"),
            fragile_output_reference: outputs.output_cards[0].fragile_output_reference.clone(),
            output_read_range: OutputReadRange::Bytes(ByteRange {
                start_byte_count: 0,
                end_byte_count: 5,
            }),
        })
        .expect("estimate output");
    assert_eq!(
        estimated.size_metadata.byte_count_option.as_ref(),
        Some(5).as_ref()
    );

    let read = nexus
        .read_output(OutputReadRequest {
            request_identifier: String::from("read-output"),
            fragile_output_reference: outputs.output_cards[0].fragile_output_reference.clone(),
            output_read_range: OutputReadRange::Bytes(ByteRange {
                start_byte_count: 0,
                end_byte_count: 5,
            }),
            maximum_bytes: 5,
        })
        .expect("read output");
    assert_eq!(read.output_text_excerpt.output_text.as_str(), "alpha");
    assert!(read.output_text_excerpt.truncation_option.is_none());
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let first_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("first-page"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    source_kinds: vec![SourceKind::Claude],
                }),
                fragile_session_reference_option: None,
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("first page");
    assert_eq!(first_page.output_cards.len(), 1);
    let cursor = first_page
        .page_metadata
        .next_page_cursor
        .clone()
        .expect("next cursor");
    let second_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("second-page"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    source_kinds: vec![SourceKind::Claude],
                }),
                fragile_session_reference_option: None,
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: Some(cursor),
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("second page");
    assert_eq!(second_page.output_cards.len(), 1);
    assert_ne!(
        first_page.output_cards[0].fragile_output_reference,
        second_page.output_cards[0].fragile_output_reference
    );

    let oversized_page = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("oversized-page"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                fragile_session_reference_option: None,
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 65,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("page limit must be enforced");
    assert_eq!(
        oversized_page.operation_rejection_reason,
        OperationRejectionReason::Oversized
    );

    let missing = nexus
        .read_output(OutputReadRequest {
            request_identifier: String::from("missing-output"),
            fragile_output_reference: String::from("missing-output-reference"),
            output_read_range: OutputReadRange::EntireOutput,
            maximum_bytes: 16,
        })
        .expect_err("unknown reference rejected");
    assert_eq!(
        missing.operation_rejection_reason,
        OperationRejectionReason::Missing
    );

    let stale_reference = first_page.output_cards[0].fragile_output_reference.clone();
    fs::write(
        &transcript,
        "{\"timestamp\":\"2026-01-02T00:10:00Z\",\"text\":\"changed output with different bytes\"}\n",
    )
    .expect("rewrite transcript");
    let stale = nexus
        .read_output(OutputReadRequest {
            request_identifier: String::from("stale-output"),
            fragile_output_reference: stale_reference,
            output_read_range: OutputReadRange::EntireOutput,
            maximum_bytes: 16,
        })
        .expect_err("stale reference rejected");
    assert_eq!(
        stale.operation_rejection_reason,
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
        ReferenceTime::from_timestamp(String::from("2026-01-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let first_sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: String::from("first-sessions-shape"),
            session_list_filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect("first sessions page");
    let sessions_cursor = first_sessions
        .page_metadata
        .next_page_cursor
        .clone()
        .expect("sessions cursor");
    let stale_sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: String::from("stale-sessions-shape"),
            session_list_filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window_option: Some(TimeWindow::Since(String::from("2026-01-02T00:15:00Z"))),
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: Some(sessions_cursor),
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect_err("session cursor is bound to the original time filter");
    assert_eq!(
        stale_sessions.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let session_listing = nexus
        .list_sessions(SessionListRequest {
            request_identifier: String::from("all-sessions-for-shape"),
            session_list_filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect("all sessions");
    let session_with_subagents = session_listing
        .session_cards
        .iter()
        .find(|session| {
            session
                .output_count
                .as_ref()
                .is_some_and(|count| *count == 2)
        })
        .expect("session with two outputs")
        .fragile_session_reference
        .clone();

    let first_subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: String::from("first-subagents-shape"),
            subagent_list_filter: SubagentListFilter {
                fragile_session_reference: session_with_subagents.clone(),
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect("first subagents page");
    let subagents_cursor = first_subagents
        .page_metadata
        .next_page_cursor
        .clone()
        .expect("subagents cursor");
    let stale_subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: String::from("stale-subagents-shape"),
            subagent_list_filter: SubagentListFilter {
                fragile_session_reference: session_with_subagents.clone(),
                authored_status_filter: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::HumanAuthored,
                ),
                task_identifier_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: Some(subagents_cursor),
                listing_order: ListingOrder::OldestFirst,
            },
        })
        .expect_err("subagent cursor is bound to the original authorship filter");
    assert_eq!(
        stale_subagents.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let first_outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("first-outputs-shape"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                fragile_session_reference_option: Some(session_with_subagents.clone()),
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("first outputs page");
    let outputs_cursor = first_outputs
        .page_metadata
        .next_page_cursor
        .clone()
        .expect("outputs cursor");
    let stale_outputs = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("stale-outputs-shape"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                fragile_session_reference_option: Some(session_with_subagents.clone()),
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::OnlyAuthoredStatus(
                    AuthoredStatus::HumanAuthored,
                ),
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: Some(outputs_cursor.clone()),
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("output cursor is bound to the original authorship filter");
    assert_eq!(
        stale_outputs.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let stale_output_page_shape = nexus
        .list_outputs(OutputListRequest {
            request_identifier: String::from("stale-output-page-shape"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::AllConfigured,
                fragile_session_reference_option: Some(session_with_subagents),
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 2,
                page_cursor: Some(outputs_cursor),
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("output cursor is bound to the original page limit");
    assert_eq!(
        stale_output_page_shape.operation_rejection_reason,
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
    configuration
        .output_interface_configuration
        .legacy_recovery_sources = vec![LegacyRecoverySource::LegacyReports(LegacyRecoveryRoot {
        filesystem_path: legacy_root.display().to_string(),
        legacy_recovery_access: LegacyRecoveryAccess::ReadOnlyRecovery,
    })];
    let accepted = RuntimeConfiguration::validate_from_meta(&configuration);
    assert!(matches!(
        accepted,
        RuntimeConfigurationValidation::Accepted(_)
    ));

    let mut rejected_configuration = configuration.clone();
    rejected_configuration.store_path = legacy_root.join("store.sema").display().to_string();
    let rejected = RuntimeConfiguration::validate_from_meta(&rejected_configuration);
    let report = match rejected {
        RuntimeConfigurationValidation::Accepted(_) => panic!("index under legacy root accepted"),
        RuntimeConfigurationValidation::Rejected(report) => report,
    };
    assert!(report.configuration_validation_issues.iter().any(|issue| issue.configuration_validation_issue_kind
        == meta_signal_aggregator::ConfigurationValidationIssueKind::InvalidFragileIndexConfiguration));
    assert_eq!(
        fs::read_to_string(legacy_root.join("report.md")).expect("legacy report unchanged"),
        "legacy recovery text"
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
    let configuration_path = root.path().join("configuration.datom");
    run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator-write-configuration"),
        &configuration_path,
        &DatomText::print(&configuration),
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
        &Query::Version(VersionQuery {
            client_name: Some(String::from("boundary-test")),
        })
        .datom_text(),
    );
    let version_reply =
        DatomText::read::<Response>("response", &version_output).expect("parse version reply");
    assert!(matches!(version_reply, Response::VersionReported(_)));

    let observe_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaQuery::ObserveConfiguration(ConfigurationObservationQuery {
            configuration_observer_option: None,
        })
        .datom_text(),
    );
    let observe_reply =
        DatomText::read::<MetaResponse>("reply", &observe_output).expect("parse observe reply");
    assert!(matches!(
        observe_reply,
        MetaResponse::ConfigurationObserved(_)
    ));

    let validate_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaQuery::ValidateConfiguration(ConfigurationCandidate {
            aggregator_configuration: configuration.clone(),
        })
        .datom_text(),
    );
    let validate_reply =
        DatomText::read::<MetaResponse>("reply", &validate_output).expect("parse validate reply");
    assert!(matches!(
        validate_reply,
        MetaResponse::ConfigurationValidated(_)
    ));

    let configure_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaQuery::Configure(ConfigurationChange {
            aggregator_configuration: configuration.clone(),
        })
        .datom_text(),
    );
    let configure_reply =
        DatomText::read::<MetaResponse>("reply", &configure_output).expect("parse configure reply");
    assert!(matches!(
        configure_reply,
        MetaResponse::ConfigurationConfigured(_)
    ));

    let list_outputs_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator"),
        &configuration_path,
        &Query::ListOutputs(OutputListRequest {
            request_identifier: String::from("daemon-list-outputs"),
            output_list_filter: OutputListFilter {
                source_selection: SourceSelection::Only(SelectedSources {
                    source_kinds: vec![SourceKind::Claude],
                }),
                fragile_session_reference_option: None,
                fragile_subagent_reference_option: None,
                task_identifier_option: None,
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                time_window_option: Some(TimeWindow::Since(String::from("2026-01-02T00:00:00Z"))),
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .datom_text(),
    );
    let list_outputs_reply = DatomText::read::<Response>("reply", &list_outputs_output)
        .expect("parse list outputs reply");
    assert!(matches!(
        list_outputs_reply,
        Response::OutputsListed(listed) if listed.output_cards.len() == 1
    ));

    let collect_request = EvidenceRequest {
        source_selection: SourceSelection::Only(SelectedSources {
            source_kinds: vec![SourceKind::Claude],
        }),
        ..evidence_request()
    };
    let collect_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator"),
        &configuration_path,
        &DatomText::print(&Query::Collect(collect_request)),
    );
    let collect_reply =
        DatomText::read::<Response>("reply", &collect_output).expect("parse collect reply");
    let package = match collect_reply {
        Response::EvidenceCollected(package) => package,
        other => panic!("expected collected evidence, got {other:?}"),
    };
    assert_eq!(package.transcript_segments.len(), 1);
    assert_eq!(
        package.transcript_segments[0].timestamp_option.as_deref(),
        Some("2026-01-02T00:30:00Z")
    );
    assert!(
        !package
            .read_failures
            .iter()
            .any(|failure| failure.read_failure_reason == ReadFailureReason::UnsupportedFormat)
    );
}

#[test]
fn meta_configure_persists_to_startup_configuration_for_restart() {
    let root = TempDir::new().expect("temporary root");
    let configuration = accepted_configuration(&root);
    let configuration_path = root.path().join("configuration.datom");
    run_binary_with_input(
        env!("CARGO_BIN_EXE_aggregator-write-configuration"),
        &configuration_path,
        &DatomText::print(&configuration),
    );
    let daemon = DaemonGuard::start(&configuration_path, "2026-01-02T01:00:00Z");
    wait_for_socket(std::path::Path::new(
        configuration.meta_socket_path.as_str(),
    ));

    let mut updated_configuration = configuration.clone();
    updated_configuration.ordinary_socket_path = root
        .path()
        .join("ordinary-restarted.sock")
        .display()
        .to_string();
    updated_configuration.meta_socket_path = root
        .path()
        .join("meta-restarted.sock")
        .display()
        .to_string();
    updated_configuration.store_path = root.path().join("future-ledger.sema").display().to_string();

    let configure_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaQuery::Configure(ConfigurationChange {
            aggregator_configuration: updated_configuration.clone(),
        })
        .datom_text(),
    );
    let configure_reply =
        DatomText::read::<MetaResponse>("reply", &configure_output).expect("parse configure reply");
    assert!(matches!(
        configure_reply,
        MetaResponse::ConfigurationConfigured(_)
    ));

    drop(daemon);
    let _restarted_daemon = DaemonGuard::start(&configuration_path, "2026-01-02T01:00:00Z");
    wait_for_socket(std::path::Path::new(
        updated_configuration.meta_socket_path.as_str(),
    ));

    let observe_output = run_binary_with_input(
        env!("CARGO_BIN_EXE_meta-aggregator"),
        &configuration_path,
        &MetaQuery::ObserveConfiguration(ConfigurationObservationQuery {
            configuration_observer_option: None,
        })
        .datom_text(),
    );
    let observe_reply =
        DatomText::read::<MetaResponse>("reply", &observe_output).expect("parse observe reply");
    match observe_reply {
        MetaResponse::ConfigurationObserved(observed) => match observed.configuration_observation {
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::BoundedText(BoundedTextProjection { maximum_bytes: 10 }),
        8,
        4,
    ));

    assert_eq!(
        outcome.truncations[0].truncation_reason,
        TruncationReason::RequestLimit
    );
    match &outcome.transcript_segments[0].segment_projection {
        SegmentProjection::Text(excerpt) => assert_eq!(
            excerpt
                .truncation_option
                .as_ref()
                .map(|truncation| truncation.truncation_reason.clone()),
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::BoundedText(BoundedTextProjection { maximum_bytes: 4 }),
        8,
        10,
    ));

    assert_eq!(
        outcome.truncations[0].truncation_reason,
        TruncationReason::ProjectionLimit
    );
    match &outcome.transcript_segments[0].segment_projection {
        SegmentProjection::Text(excerpt) => assert_eq!(
            excerpt
                .truncation_option
                .as_ref()
                .map(|truncation| truncation.truncation_reason.clone()),
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(
        outcome.transcript_segments[0].source_kind,
        SourceKind::Codex
    );
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .filesystem_path_option
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .filesystem_path_option
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

    assert_eq!(
        health.source_health_status,
        SourceHealthStatus::DiscoveryTruncated
    );
    assert_eq!(health.discovered_files, 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.scan_limit_kind == ScanLimitKind::DiscoveredFiles(limit.scan_limit)
            && limit.scan_limit == 1
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

    assert_eq!(
        health.source_health_status,
        SourceHealthStatus::DiscoveryTruncated
    );
    assert_eq!(health.discovered_files, 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.scan_limit_kind == ScanLimitKind::DiscoveredFiles(limit.scan_limit)
            && limit.scan_limit == 1
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
        ReadFailureReason::PermissionDenied
    );
    assert!(
        outcome.read_failures[0]
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
    );
    assert!(
        outcome.read_failures[0]
            .source_identifier_option
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(
        outcome
            .read_failures
            .iter()
            .all(|failure| failure.read_failure_reason == ReadFailureReason::PermissionDenied)
    );
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
            && failure
                .source_identifier_option
                .as_ref()
                .is_some_and(|identifier| {
                    identifier.as_str().contains("locator:")
                        && identifier.as_str().contains("../outside/missing.jsonl")
                })
    }));
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:2"))
            && failure
                .source_identifier_option
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 2);
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .filesystem_path_option
            .as_ref()
            .is_some_and(|path| path.as_str().contains("index.jsonl:1"))
    }));
    assert!(outcome.read_failures.iter().any(|failure| {
        failure
            .filesystem_path_option
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
        TimeWindow::Since(String::from("2026-01-01T00:00:00Z")),
        Projection::MetadataOnly,
        8,
    ));

    assert_eq!(outcome.read_failures.len(), 1);
    assert_eq!(
        outcome.read_failures[0].read_failure_reason,
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
        TimeWindow::Since(String::from("2026-02-01T00:00:00Z")),
        Projection::IdentifiersOnly,
        8,
    ));
    assert_eq!(outcome.transcript_segments.len(), 1);
    assert_eq!(outcome.transcript_segments[0].source_kind, SourceKind::Pi);
    assert!(matches!(
        outcome.transcript_segments[0].segment_projection,
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
        TimeWindow::Since(String::from("2026-02-01T00:00:00Z")),
        Projection::IdentifiersOnly,
        8,
    ));

    assert_eq!(outcome.transcript_segments.len(), 1);
    assert!(outcome.truncations.iter().any(|truncation| {
        truncation
            .filesystem_path_option
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

    assert_eq!(
        health.source_health_status,
        SourceHealthStatus::DiscoveryTruncated
    );
    assert_eq!(health.discovered_files, 1);
    assert!(health.scan_limits.iter().any(|limit| {
        limit.scan_limit_kind == ScanLimitKind::DiscoveredFiles(limit.scan_limit)
            && limit.scan_limit == 1
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
        output_interface_limit_policy: OutputInterfaceLimitPolicy {
            maximum_transcript_discovered_files: 1,
            ..OutputInterfaceLimitPolicy::default_policy()
        },
        ..OutputInterfaceConfiguration::default_policy()
    };
    let configuration = AggregatorConfiguration {
        ordinary_socket_path: root.path().join("ordinary.sock").display().to_string(),
        ordinary_socket_mode: 0o660,
        meta_socket_path: root.path().join("meta.sock").display().to_string(),
        meta_socket_mode: 0o600,
        store_path: root.path().join("store.sema").display().to_string(),
        active_repositories: vec![ActiveRepository {
            repository_name: String::from("fixture-repository"),
            filesystem_path: repository.display().to_string(),
        }],
        transcript_sources: vec![
            TranscriptSource::Codex(TranscriptRoot {
                filesystem_path: codex.display().to_string(),
            }),
            TranscriptSource::Pi(TranscriptRoot {
                filesystem_path: pi.display().to_string(),
            }),
        ],
        default_projection: Projection::MetadataOnly,
        default_limit_policy: LimitPolicy {
            maximum_segments: 16,
            maximum_bytes: 4096,
        },
        output_interface_configuration: output_interfaces,
    };
    let runtime = RuntimeConfiguration::validate_from_meta(&configuration)
        .accepted_configuration()
        .expect("accepted configuration")
        .clone();
    let clock = CollectionClock::fixed(
        ReferenceTime::from_timestamp(String::from("2026-03-01T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let inventory = NexusPlane::with_runtime_configuration(runtime, clock)
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: String::from("inventory-configured-limits"),
            source_selection: SourceSelection::AllConfigured,
            archive_path_option: None,
        })
        .expect("inventory sessions");

    assert_eq!(
        inventory
            .session_inventory_scan_report
            .session_inventory_completeness,
        SessionInventoryCompleteness::Truncated
    );
    for source in [SourceKind::Codex, SourceKind::Pi] {
        let report = inventory
            .session_inventory_scan_report
            .session_inventory_source_reports
            .iter()
            .find(|report| report.source_kind == source)
            .expect("source report");
        assert_eq!(
            report.session_inventory_completeness,
            SessionInventoryCompleteness::Truncated
        );
        assert_eq!(report.discovered_files, 1);
        assert!(report.scan_limits.iter().any(|limit| {
            limit.scan_limit_kind == ScanLimitKind::DiscoveredFiles(limit.scan_limit)
                && limit.scan_limit == 1
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
        .map(|block| block.kind.clone())
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
            .flat_map(|record| record.blocks.iter().map(|block| block.kind.clone()))
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
            .map(|block| block.kind.clone())
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
        .flat_map(|record| record.blocks.iter().map(|block| block.kind.clone()))
        .collect::<Vec<_>>();
    let codex_kinds = codex_outcome
        .records
        .iter()
        .flat_map(|record| record.blocks.iter().map(|block| block.kind.clone()))
        .collect::<Vec<_>>();
    let pi_kinds = pi_outcome
        .records
        .iter()
        .flat_map(|record| record.blocks.iter().map(|block| block.kind.clone()))
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
        ReferenceTime::from_timestamp(String::from("2026-04-02T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let all_blocks = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: String::from("list-blocks"),
            transcript_block_filter: all_transcript_block_filter(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("list transcript blocks");
    assert_eq!(
        all_blocks
            .transcript_block_cards
            .iter()
            .map(|block| block.transcript_block_kind.clone())
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
            request_identifier: String::from("list-tool-calls"),
            transcript_block_filter: only_transcript_block_filter(TranscriptBlockKind::ToolCall),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::BoundedPreview(BoundedTextProjection {
                maximum_bytes: 12,
            }),
        })
        .expect("list tool call blocks");
    assert_eq!(tool_calls.transcript_block_cards.len(), 1);
    assert!(
        tool_calls.transcript_block_cards[0]
            .transcript_text_excerpt_option
            .as_ref()
            .is_some_and(|preview| preview.byte_count <= 12)
    );

    let word_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-word"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::contains(
                QueryTerm::word("alpha"),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("word search");
    assert_eq!(word_search.transcript_block_search_matches.len(), 2);

    let phrase_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-phrase"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::contains(
                QueryTerm::phrase(vec!["exact".to_string(), "phrase".to_string()]),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("phrase search");
    assert_eq!(phrase_search.transcript_block_search_matches.len(), 1);
    assert_eq!(
        phrase_search.transcript_block_search_matches[0]
            .transcript_block_card
            .transcript_block_kind,
        TranscriptBlockKind::UserPrompt
    );

    let near_search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-near"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::near(
                QueryTerm::word("alpha"),
                QueryTerm::word("omega"),
                WordDistance::new(1),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("near search");
    assert_eq!(near_search.transcript_block_search_matches.len(), 1);
    assert_eq!(
        near_search.transcript_block_search_matches[0]
            .transcript_block_card
            .transcript_block_kind,
        TranscriptBlockKind::Inference
    );

    let estimated = nexus
        .estimate_transcript_block(TranscriptBlockEstimateRequest {
            request_identifier: String::from("estimate-block"),
            fragile_transcript_block_reference: near_search.transcript_block_search_matches[0]
                .transcript_block_card
                .fragile_transcript_block_reference
                .clone(),
        })
        .expect("estimate block");
    assert!(
        estimated
            .size_metadata
            .byte_count_option
            .is_some_and(|count| count > 20)
    );

    let read = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("read-block"),
            fragile_transcript_block_reference: near_search.transcript_block_search_matches[0]
                .transcript_block_card
                .fragile_transcript_block_reference
                .clone(),
            maximum_bytes: 8,
        })
        .expect("read bounded block");
    assert_eq!(
        read.transcript_text_excerpt.transcript_text.as_str(),
        "alpha be"
    );
    assert_eq!(read.transcript_text_excerpt.byte_count, 8);
    assert!(read.transcript_text_excerpt.truncation_option.is_some());

    let first_page = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: String::from("block-cursor-first"),
            transcript_block_filter: all_transcript_block_filter(),
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("first block page");
    let stale_kind_cursor = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: String::from("block-cursor-stale-kind"),
            transcript_block_filter: only_transcript_block_filter(
                TranscriptBlockKind::AgentResponse,
            ),
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: first_page.page_metadata.next_page_cursor.clone(),
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("kind change must stale the block cursor");
    assert_eq!(
        stale_kind_cursor.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let first_search_page = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-cursor-first"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::contains(
                QueryTerm::word("alpha"),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("first search page");
    let stale_query_cursor = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-cursor-stale-query"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::contains(
                QueryTerm::word("phrase"),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 1,
                page_cursor: first_search_page.page_metadata.next_page_cursor.clone(),
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("query change must stale the search cursor");
    assert_eq!(
        stale_query_cursor.operation_rejection_reason,
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
        ReferenceTime::from_timestamp(String::from("2026-04-03T01:00:00Z"))
            .expect("reference timestamp"),
    );
    let nexus = NexusPlane::with_runtime_configuration(runtime, clock);

    let listed = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: String::from("list-for-rejections"),
            transcript_block_filter: all_transcript_block_filter(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("list blocks");
    let readable_reference = listed.transcript_block_cards[0]
        .fragile_transcript_block_reference
        .clone();
    let attachment_reference = listed.transcript_block_cards[1]
        .fragile_transcript_block_reference
        .clone();

    let missing = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("missing-block"),
            fragile_transcript_block_reference: String::from("missing-block-reference"),
            maximum_bytes: 16,
        })
        .expect_err("missing block reference rejected");
    assert_eq!(
        missing.operation_rejection_reason,
        OperationRejectionReason::Missing
    );

    let unavailable = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("unavailable-block"),
            fragile_transcript_block_reference: attachment_reference,
            maximum_bytes: 16,
        })
        .expect_err("unavailable attachment text rejected");
    assert_eq!(
        unavailable.operation_rejection_reason,
        OperationRejectionReason::Unsupported
    );

    let invalid_query = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("invalid-query"),
            transcript_block_filter: all_transcript_block_filter(),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::contains(
                QueryTerm::word("!!!"),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect_err("empty normalized query rejected");
    assert_eq!(
        invalid_query.operation_rejection_reason,
        OperationRejectionReason::InvalidQuery
    );

    for broken_arena in [
        TranscriptBlockTextQuery {
            text_query_nodes: vec![signal_aggregator::TextQueryNode::AllOf(vec![9])],
            text_query_root: 0,
        },
        TranscriptBlockTextQuery {
            text_query_nodes: vec![
                signal_aggregator::TextQueryNode::Not(1),
                signal_aggregator::TextQueryNode::Not(0),
            ],
            text_query_root: 0,
        },
    ] {
        let rejected = nexus
            .search_transcript_blocks(TranscriptBlockSearchRequest {
                request_identifier: String::from("broken-arena"),
                transcript_block_filter: all_transcript_block_filter(),
                transcript_block_text_query: broken_arena,
                page_request: PageRequest {
                    page_limit: 10,
                    page_cursor: None,
                    listing_order: ListingOrder::OldestFirst,
                },
                card_projection: CardProjection::MetadataOnly,
            })
            .expect_err("an arena that is not a finite tree is rejected");
        assert_eq!(
            rejected.operation_rejection_reason,
            OperationRejectionReason::InvalidQuery
        );
    }

    fs::write(
        &transcript,
        r#"{"timestamp":"2026-04-03T00:00:00Z","message":{"role":"assistant","content":[{"type":"text","text":"changed block text"}]}}
"#,
    )
    .expect("rewrite transcript for stale reference");
    let stale = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("stale-block"),
            fragile_transcript_block_reference: readable_reference.clone(),
            maximum_bytes: 16,
        })
        .expect_err("changed backing file stales the block reference");
    assert_eq!(
        stale.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceStale
    );

    let refreshed = nexus
        .list_transcript_blocks(TranscriptBlockListRequest {
            request_identifier: String::from("refresh-after-stale"),
            transcript_block_filter: all_transcript_block_filter(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::OldestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("refresh changed block");
    fs::remove_file(&transcript).expect("delete transcript for broken reference");
    let broken = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("broken-block"),
            fragile_transcript_block_reference: refreshed.transcript_block_cards[0]
                .fragile_transcript_block_reference
                .clone(),
            maximum_bytes: 16,
        })
        .expect_err("deleted backing file breaks the block reference");
    assert_eq!(
        broken.operation_rejection_reason,
        OperationRejectionReason::FragileReferenceBroken
    );
}

#[test]
fn repository_adapter_uses_fixture_or_reports_policy_unavailable() {
    let repository = RepositoryAdapterConfiguration::new(
        String::from("fixture-repository"),
        std::env::temp_dir(),
    );
    let fixture = RepositoryEvidenceFixture::new(vec![
        RepositoryChangeFixture::new(
            String::from("fixture-repository"),
            String::from("/fixture/repository"),
            vec![String::from("src/lib.rs")],
            RepositoryWorktreeState::HasChanges,
        ),
        RepositoryChangeFixture::new(
            String::from("other"),
            String::from("/fixture/other"),
            vec![String::from("README.md")],
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
    configuration.store_path = store_path.display().to_string();
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![
        TranscriptSource::Claude(TranscriptRoot {
            filesystem_path: parent,
        }),
        TranscriptSource::ClaudeSubagentOutput(TranscriptRoot {
            filesystem_path: subagents,
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
            ReferenceTime::from_timestamp(String::from("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    );

    let health = nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: String::from("health-fixture"),
        })
        .expect("health observed");
    assert!(
        health
            .source_health_cards
            .iter()
            .any(
                |source| source.source_kind == SourceKind::ClaudeSubagentOutput
                    && source.source_health_status == SourceHealthStatus::ReadableIndexed
            ),
        "configured Claude subagent .output fixture should be indexed: {health:?}"
    );

    let sessions = nexus
        .list_sessions(SessionListRequest {
            request_identifier: String::from("sessions-fixture"),
            session_list_filter: SessionListFilter {
                source_selection: SourceSelection::AllConfigured,
                time_window_option: None,
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::NewestFirst,
            },
        })
        .expect("sessions");
    assert_eq!(
        sessions.session_cards.len(),
        2,
        "equal producer identifiers from configured sources must remain source-scoped"
    );
    let subagent_session = sessions
        .session_cards
        .iter()
        .find(|session| session.source_kind == SourceKind::ClaudeSubagentOutput)
        .expect("subagent-output source retains its own session card");

    let subagents = nexus
        .list_subagents(SubagentListRequest {
            request_identifier: String::from("subagents-fixture"),
            subagent_list_filter: SubagentListFilter {
                fragile_session_reference: subagent_session.fragile_session_reference.clone(),
                authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
                task_identifier_option: None,
            },
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::NewestFirst,
            },
        })
        .expect("subagents");
    assert_eq!(subagents.subagent_cards[0].subagent_name.as_str(), "writer");
    assert_eq!(
        subagents.subagent_cards[0]
            .subagent_task_metadata_option
            .as_ref()
            .expect("task metadata")
            .task_identifier
            .as_str(),
        "task-1"
    );

    let search = nexus
        .search_transcript_blocks(TranscriptBlockSearchRequest {
            request_identifier: String::from("search-fixture"),
            transcript_block_filter: transcript_block_filter(
                TranscriptBlockKindSelection::AllTranscriptBlockKinds,
            ),
            transcript_block_text_query: EngineQueryProjection::new(&TextQuery::Contains(
                QueryTerm::word("quota"),
            ))
            .project(),
            page_request: PageRequest {
                page_limit: 10,
                page_cursor: None,
                listing_order: ListingOrder::NewestFirst,
            },
            card_projection: CardProjection::MetadataOnly,
        })
        .expect("search");
    assert_eq!(search.transcript_block_search_matches.len(), 1);
    let read = nexus
        .read_transcript_block(TranscriptBlockReadRequest {
            request_identifier: String::from("read-fixture"),
            fragile_transcript_block_reference: search.transcript_block_search_matches[0]
                .transcript_block_card
                .fragile_transcript_block_reference
                .clone(),
            maximum_bytes: 256,
        })
        .expect("read block");
    assert!(
        read.transcript_text_excerpt
            .transcript_text
            .as_str()
            .contains("quota")
    );
}

#[test]
fn health_reports_unreadable_durable_index_store() {
    let temp = TempDir::new().expect("tempdir");
    let (_, _, empty, _) = materialize_recovery_fixtures(temp.path());
    let store_path = temp.path().join("store");
    let index_store = typed_index_store(&store_path);
    fs::write(index_store.pointer_path(), "not-json").expect("write unreadable index fixture");
    let mut configuration = ConfigurationFixture::minimal();
    configuration.store_path = store_path.display().to_string();
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![TranscriptSource::Claude(TranscriptRoot {
        filesystem_path: empty,
    })];
    let runtime_configuration = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
        other => panic!("expected accepted configuration, got {other:?}"),
    };

    let health = NexusPlane::with_runtime_configuration(
        runtime_configuration,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(String::from("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    )
    .observe_health(RuntimeHealthRequest {
        request_identifier: String::from("health-index-store-unreadable"),
    })
    .expect("health");

    assert_eq!(
        health.index_health.source_health_status,
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
            ReferenceTime::from_timestamp(String::from("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let request = TranscriptBlockListRequest {
        request_identifier: String::from("reconcile-current-evidence"),
        transcript_block_filter: all_transcript_block_filter(),
        page_request: PageRequest {
            page_limit: 10,
            page_cursor: None,
            listing_order: ListingOrder::OldestFirst,
        },
        card_projection: CardProjection::MetadataOnly,
    };

    let first = nexus
        .list_transcript_blocks(request.clone())
        .expect("first refresh");
    let first_bytes = fs::read(index_store.pointer_path()).expect("first index bytes");
    let repeated = nexus
        .list_transcript_blocks(request.clone())
        .expect("identical refresh");
    assert_eq!(
        repeated.transcript_block_cards,
        first.transcript_block_cards
    );
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
    assert_eq!(replacement.transcript_block_cards.len(), 1);
    assert_ne!(
        replacement.transcript_block_cards[0].fragile_transcript_block_reference,
        first.transcript_block_cards[0].fragile_transcript_block_reference
    );
    let replacement_pointer = aggregator::output_index::store::IndexStore::new(
        index_store.pointer_path().to_path_buf(),
        aggregator::output_index::limits::IndexStoreLimits::default(),
    )
    .read_current_pointer()
    .expect("read v3 pointer")
    .expect("published v3 pointer");
    assert_eq!(replacement_pointer.format_version, 3);
    assert_eq!(
        PersistentIndex::from_typed_store(&index_store)
            .expect("read typed replacement")
            .output_records()
            .count(),
        1
    );

    fs::remove_file(&transcript).expect("remove evidence");
    let removed = nexus
        .list_transcript_blocks(request)
        .expect("deletion refresh");
    assert!(removed.transcript_block_cards.is_empty());
    let removed_bytes = fs::read(index_store.pointer_path()).expect("deletion pointer bytes");
    assert!(
        PersistentIndex::from_typed_store(&index_store)
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
        filesystem_path: root.path().join("claude").display().to_string(),
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
            ReferenceTime::from_timestamp(String::from("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let list_request = SessionListRequest {
        request_identifier: String::from("complete-coverage"),
        session_list_filter: SessionListFilter {
            source_selection: SourceSelection::AllConfigured,
            time_window_option: None,
        },
        page_request: PageRequest {
            page_limit: 10,
            page_cursor: None,
            listing_order: ListingOrder::OldestFirst,
        },
    };
    assert_eq!(
        complete_nexus
            .list_sessions(list_request.clone())
            .expect("complete refresh")
            .session_cards
            .len(),
        2
    );
    let complete_bytes = fs::read(index_store.pointer_path()).expect("complete index bytes");

    let mut limited_configuration = configuration;
    limited_configuration
        .output_interface_configuration
        .output_interface_limit_policy
        .maximum_transcript_discovered_files = 1;
    let limited_runtime = RuntimeConfiguration::validate_from_meta(&limited_configuration)
        .accepted_configuration()
        .expect("limited configuration")
        .clone();
    let limited_nexus = NexusPlane::with_runtime_configuration(
        limited_runtime,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(String::from("2026-07-09T11:00:00Z"))
                .expect("reference time"),
        ),
    );
    let preserved = limited_nexus
        .list_sessions(list_request)
        .expect("truncated scan uses last complete index");
    assert_eq!(preserved.session_cards.len(), 2);
    assert_eq!(
        fs::read(index_store.pointer_path()).expect("preserved index bytes"),
        complete_bytes
    );
    let health = limited_nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: String::from("truncated-coverage-health"),
        })
        .expect("truncated health");
    assert!(
        health
            .source_health_cards
            .iter()
            .any(|source| source.source_health_status == SourceHealthStatus::DiscoveryTruncated)
    );
}

#[test]
fn health_distinguishes_empty_and_malformed_fixture_roots() {
    let temp = TempDir::new().expect("tempdir");
    let (_, _, empty, malformed) = materialize_recovery_fixtures(temp.path());
    let mut configuration = ConfigurationFixture::minimal();
    configuration.store_path = temp.path().join("store").display().to_string();
    configuration.active_repositories = Vec::new();
    configuration.transcript_sources = vec![
        TranscriptSource::Claude(TranscriptRoot {
            filesystem_path: empty,
        }),
        TranscriptSource::Claude(TranscriptRoot {
            filesystem_path: malformed,
        }),
    ];
    let runtime_configuration = match RuntimeConfiguration::validate_from_meta(&configuration) {
        RuntimeConfigurationValidation::Accepted(configuration) => configuration,
        other => panic!("expected accepted configuration, got {other:?}"),
    };
    let nexus = NexusPlane::with_runtime_configuration(
        runtime_configuration,
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(String::from("2026-07-05T13:00:00Z"))
                .expect("reference time"),
        ),
    );
    let health = nexus
        .observe_health(RuntimeHealthRequest {
            request_identifier: String::from("health-empty-malformed"),
        })
        .expect("health");
    assert!(
        health
            .source_health_cards
            .iter()
            .any(|source| source.source_health_status == SourceHealthStatus::ReadableEmpty)
    );
    assert!(health.source_health_cards.iter().any(|source| {
        matches!(
            source.source_health_status,
            SourceHealthStatus::MalformedRecords(_)
        ) && source.discovered_files == 1
            && source.malformed_records > 0
    }));

    let inventory = nexus
        .inventory_sessions(SessionInventoryRequest {
            request_identifier: String::from("inventory-empty-malformed"),
            source_selection: SourceSelection::AllConfigured,
            archive_path_option: None,
        })
        .expect("inventory");
    assert_eq!(
        inventory
            .session_inventory_scan_report
            .session_inventory_completeness,
        SessionInventoryCompleteness::Resumable
    );
    assert!(
        inventory
            .session_inventory_scan_report
            .session_inventory_source_reports
            .iter()
            .any(|source| {
                source.session_inventory_completeness == SessionInventoryCompleteness::Resumable
                    && source.discovered_files == 1
            })
    );
}
