//! A received frame's nesting is bounded before it is decoded.
//!
//! rkyv validation recurses once per nested pointer, so a received frame is
//! validated within `MAXIMUM_FRAME_NESTING`, and every text-query and evidence
//! tree in it is then measured, still archived, against the projection bounds
//! (depth 32, 256 nodes). Trees are built at the bound and one past it; every
//! leaf is a phrase of words too long to be stored inline, so each leaf spends
//! the most pointer nesting a leaf can.

use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{Arc, Mutex},
    thread,
};

use aggregator::{
    CollectionClock, ReferenceTime, SemaPlane, TextQueryProjectionFault,
    daemon::OrdinarySocketService,
    wire::{FrameRefusal, MAXIMUM_FRAME_NESTING, ReceivedFrame, SignalFrame},
};
use signal::Signalizable;
use signal_aggregator::{
    AuthoredStatus, AuthoredStatusFilter, CardProjection, ContainsEvidence, ListingOrder,
    MatchEvidence, Occurrence, OperationKind, OperationRejectionReason, PageMetadata, PageRequest,
    Query, Response, SearchPhrase, SizeCertainty, SizeMetadata, SourceKind, SourceSelection,
    TextQuery, TextQueryTerm, TranscriptBlockCard, TranscriptBlockFilter, TranscriptBlockKind,
    TranscriptBlockKindSelection, TranscriptBlockProvenance, TranscriptBlockSearchMatch,
    TranscriptBlockSearchRequest, TranscriptBlockTextAvailability, TranscriptBlocksSearched,
};

const REQUEST: &str = "bounded-frame-request";

fn long_phrase() -> TextQueryTerm {
    TextQueryTerm::Phrase(SearchPhrase {
        search_words: vec![
            String::from("quota-reset-window"),
            String::from("rate-limit-exceeded"),
        ],
    })
}

fn query_leaf() -> TextQuery {
    TextQuery::Contains(long_phrase())
}

/// A chain `depth` nodes deep: the leaf under `depth - 1` single-child `AllOf`s.
fn query_of_depth(depth: usize) -> TextQuery {
    (1..depth).fold(query_leaf(), |child, _| TextQuery::AllOf(vec![child]))
}

/// One `AnyOf` over `leaves` leaves: `leaves + 1` nodes.
fn query_of_nodes(nodes: usize) -> TextQuery {
    TextQuery::AnyOf(vec![query_leaf(); nodes - 1])
}

fn evidence_leaf() -> MatchEvidence {
    MatchEvidence::Contains(ContainsEvidence {
        text_query_term: long_phrase(),
        occurrences: vec![Occurrence {
            start_word_position: 3,
            end_word_position: 4,
        }],
    })
}

fn evidence_of_depth(depth: usize) -> MatchEvidence {
    (1..depth).fold(evidence_leaf(), |child, _| {
        MatchEvidence::AllOf(vec![child])
    })
}

fn evidence_of_nodes(nodes: usize) -> MatchEvidence {
    MatchEvidence::AnyOf(vec![evidence_leaf(); nodes - 1])
}

fn search(tree: TextQuery) -> Query {
    Query::SearchTranscriptBlocks(TranscriptBlockSearchRequest {
        request_identifier: String::from(REQUEST),
        transcript_block_filter: TranscriptBlockFilter {
            source_selection: SourceSelection::AllConfigured,
            fragile_session_reference_option: None,
            fragile_subagent_reference_option: None,
            task_identifier_option: None,
            transcript_block_kind_selection: TranscriptBlockKindSelection::AllTranscriptBlockKinds,
            authored_status_filter: AuthoredStatusFilter::AnyAuthoredStatus,
            time_window_option: None,
        },
        transcript_block_text_query: tree,
        page_request: PageRequest {
            page_limit: 10,
            page_cursor: None,
            listing_order: ListingOrder::OldestFirst,
        },
        card_projection: CardProjection::MetadataOnly,
    })
}

fn searched(evidence: MatchEvidence) -> Response {
    Response::TranscriptBlocksSearched(TranscriptBlocksSearched {
        request_identifier: String::from(REQUEST),
        transcript_block_search_matches: vec![TranscriptBlockSearchMatch {
            transcript_block_card: TranscriptBlockCard {
                fragile_transcript_block_reference: String::from("block-reference-0001"),
                fragile_session_reference: String::from("session-reference-0001"),
                fragile_subagent_reference_option: None,
                subagent_task_metadata_option: None,
                transcript_block_kind: TranscriptBlockKind::AgentResponse,
                transcript_block_index: 0,
                transcript_block_provenance: TranscriptBlockProvenance {
                    source_kind: SourceKind::Claude,
                    source_identifier: String::from("claude-transcript-source"),
                    authored_status: AuthoredStatus::AgentAuthored,
                    observed_at: Some(String::from("2026-09-25T00:00:00Z")),
                },
                line_range_option: None,
                byte_range_option: None,
                size_metadata: SizeMetadata {
                    byte_count_option: Some(64),
                    line_count_option: Some(1),
                    segment_count: Some(1),
                    size_certainty: SizeCertainty::Exact,
                },
                transcript_block_text_availability: TranscriptBlockTextAvailability::ReadableText,
                transcript_text_excerpt_option: None,
            },
            transcript_block_search_evidence: evidence,
        }],
        page_metadata: PageMetadata {
            page_limit: 10,
            returned_items: 1,
            total_items: Some(1),
            next_page_cursor: None,
            listing_order: ListingOrder::OldestFirst,
        },
    })
}

fn frame<T: Signalizable>(value: &T) -> ReceivedFrame {
    ReceivedFrame::new(signal::ByteViewable::bytes(&value.signalize().expect("signalize")).to_vec())
}

fn refused(fault: TextQueryProjectionFault) -> FrameRefusal {
    FrameRefusal::TreeOutsideBound {
        request_identifier: String::from(REQUEST),
        fault,
    }
}

#[test]
fn a_query_frame_at_depth_32_is_accepted() {
    let query = search(query_of_depth(32));
    assert_eq!(frame(&query).restore::<Query>(), Ok(query));
}

#[test]
fn a_query_frame_at_depth_33_is_refused_before_decoding() {
    assert_eq!(
        frame(&search(query_of_depth(33))).restore::<Query>(),
        Err(refused(TextQueryProjectionFault::TooDeep))
    );
}

#[test]
fn a_query_frame_of_256_nodes_is_accepted() {
    let query = search(query_of_nodes(256));
    assert_eq!(frame(&query).restore::<Query>(), Ok(query));
}

#[test]
fn a_query_frame_of_257_nodes_is_refused_before_decoding() {
    assert_eq!(
        frame(&search(query_of_nodes(257))).restore::<Query>(),
        Err(refused(TextQueryProjectionFault::TooLarge))
    );
}

#[test]
fn an_evidence_frame_at_depth_32_is_accepted() {
    let response = searched(evidence_of_depth(32));
    assert_eq!(frame(&response).restore::<Response>(), Ok(response));
}

#[test]
fn an_evidence_frame_at_depth_33_is_refused_before_decoding() {
    assert_eq!(
        frame(&searched(evidence_of_depth(33))).restore::<Response>(),
        Err(refused(TextQueryProjectionFault::TooDeep))
    );
}

#[test]
fn an_evidence_frame_of_256_nodes_is_accepted() {
    let response = searched(evidence_of_nodes(256));
    assert_eq!(frame(&response).restore::<Response>(), Ok(response));
}

#[test]
fn an_evidence_frame_of_257_nodes_is_refused_before_decoding() {
    assert_eq!(
        frame(&searched(evidence_of_nodes(257))).restore::<Response>(),
        Err(refused(TextQueryProjectionFault::TooLarge))
    );
}

/// A tree far past the validation ceiling is refused by validation itself, on
/// a stack far too small for an unbounded walk of it.
#[test]
fn a_frame_nested_far_past_the_ceiling_is_refused_on_a_small_stack() {
    const DEPTH: usize = 20_000;
    let bytes = thread::Builder::new()
        .stack_size(256 << 20)
        .spawn(|| {
            let query = search(query_of_depth(DEPTH));
            let bytes = frame(&query);
            // The deep tree is dropped here, on the large stack.
            drop(query);
            bytes
        })
        .expect("spawn builder")
        .join()
        .expect("build deep frame");
    let refusal = thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || bytes.restore::<Query>())
        .expect("spawn receiver")
        .join()
        .expect("the receiver's stack holds")
        .expect_err("a frame past the ceiling is refused");
    assert!(
        matches!(refusal, FrameRefusal::Unvalidated { .. }),
        "{refusal:?}"
    );
    const { assert!(DEPTH > MAXIMUM_FRAME_NESTING) };
}

/// The ordinary socket answers a validated frame whose tree passes its bound
/// with the typed rejection, naming the request.
#[test]
fn the_ordinary_socket_answers_a_tree_past_its_bound_with_invalid_query() {
    let (mut client, server) = UnixStream::pair().expect("socket pair");
    let service = OrdinarySocketService::new(
        std::env::temp_dir().join("aggregator-received-frame-unused.sock"),
        0o600,
        Arc::new(Mutex::new(SemaPlane::empty())),
        CollectionClock::fixed(
            ReferenceTime::from_timestamp(String::from("2026-09-25T00:00:00Z"))
                .expect("reference timestamp"),
        ),
    );
    let serving = thread::spawn(move || service.handle_stream(server));
    SignalFrame::write("query", &mut client, &search(query_of_depth(33))).expect("write query");
    client.flush().expect("flush");
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown write");
    let response: Response = SignalFrame::read("response", &mut client).expect("typed reply");
    serving.join().expect("serve").expect("handled");
    let Response::OperationRejected(rejected) = response else {
        panic!("expected OperationRejected, got {response:?}");
    };
    assert_eq!(rejected.request_identifier, REQUEST);
    assert_eq!(
        rejected.operation_kind,
        OperationKind::SearchTranscriptBlocks
    );
    assert_eq!(
        rejected.operation_rejection_reason,
        OperationRejectionReason::InvalidQuery
    );
}
