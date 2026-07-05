use aggregator::{
    AdapterKind, ConfigurationFixture, ConfigurationStore, NexusPlane, SemaPlane, SignalPlane,
    adapter::{
        claude::ClaudeTranscriptAdapter, codex::CodexTranscriptAdapter, pi::PiTranscriptAdapter,
        repository::RepositoryAdapter,
    },
};
use meta_signal_aggregator::{ConfigurationChange, MetaAggregatorReply};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::{
    AggregatorReply, ByteLimit, DurationAmount, DurationUnit, EvidenceRequest, LimitPolicy,
    Projection, RejectionReason, RelativeDuration, RequestIdentifier, SegmentLimit,
    SourceSelection, TimeWindow,
};

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

#[test]
fn adapter_skeletons_name_the_approved_sources() {
    assert_eq!(
        ClaudeTranscriptAdapter.kind(),
        AdapterKind::ClaudeTranscript
    );
    assert_eq!(CodexTranscriptAdapter.kind(), AdapterKind::CodexTranscript);
    assert_eq!(PiTranscriptAdapter.kind(), AdapterKind::PiTranscript);
    assert_eq!(RepositoryAdapter.kind(), AdapterKind::Repository);
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
        .expect_err("scaffold stops before reading");
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
fn sema_scaffold_observes_and_configures_typed_configuration() {
    let mut sema = SemaPlane::empty();
    assert!(matches!(
        sema.observe_configuration(),
        MetaAggregatorReply::ConfigurationObserved(_)
    ));
    let configured = sema.configure(ConfigurationChange {
        configuration: ConfigurationFixture::minimal(),
    });
    assert!(matches!(
        configured,
        MetaAggregatorReply::ConfigurationConfigured(_)
    ));
}

#[test]
fn configuration_store_names_missing_storage() {
    let store = ConfigurationStore::in_memory();
    let error = store
        .read_configuration()
        .expect_err("storage is intentionally skeletal");
    assert!(error.to_string().contains("not implemented"));
}
