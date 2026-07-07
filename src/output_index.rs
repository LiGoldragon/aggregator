use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use nota_text_query::{Query, QueryTerm, SearchText};
use serde_json::{Value, json};
use signal_aggregator::{
    AuthoredStatus, AuthoredStatusFilter, ByteCount, ByteLimit, ByteRange, CardProjection,
    DurationUnit, FilesystemPath, FragileOutputReference, FragileOutputSegmentReference,
    FragilePageCursor, FragileSessionReference, FragileSubagentReference,
    FragileTranscriptBlockReference, ItemCount, LineCount, LineNumber, LineRange, ListingOrder,
    OperationKind, OperationRejected, OperationRejectionReason, OutputCard, OutputEstimateRequest,
    OutputEstimated, OutputListFilter, OutputListRequest, OutputProvenance, OutputRead,
    OutputReadRange, OutputReadRequest, OutputSegmentCard, OutputSegmentListFilter,
    OutputSegmentListRequest, OutputSegmentsListed, OutputText, OutputTextExcerpt, OutputsListed,
    PageLimit, PageMetadata, PageRequest, RejectedFragileReference, RequestIdentifier,
    SegmentIndex, SessionCard, SessionListFilter, SessionListRequest, SessionsListed,
    SizeCertainty, SizeMetadata, SourceKind, SourceSelection, SubagentCard, SubagentListFilter,
    SubagentListRequest, SubagentsListed, TimeWindow, Timestamp, TranscriptBlockCard,
    TranscriptBlockEstimateRequest, TranscriptBlockEstimated, TranscriptBlockFilter,
    TranscriptBlockKind, TranscriptBlockKindSelection, TranscriptBlockListRequest,
    TranscriptBlockProvenance, TranscriptBlockRead, TranscriptBlockReadRequest,
    TranscriptBlockSearchEvidence, TranscriptBlockSearchMatch, TranscriptBlockSearchRequest,
    TranscriptBlockTextAvailability, TranscriptBlockTextQuery, TranscriptBlocksListed,
    TranscriptBlocksSearched, TranscriptText, TranscriptTextExcerpt, Truncation, TruncationReason,
};

use crate::{
    CollectionClock, Error, Result, RuntimeConfiguration, TranscriptAdapterConfiguration,
    adapter::{
        OutputLineCounter, TimeWindowAcceptance, TimeWindowMatcher, TranscriptBlockRecord,
        TranscriptRawReadOutcome, TranscriptRecord, claude::ClaudeJsonlRecord,
        codex::CodexJsonlRecord, pi::PiJsonlRecord,
    },
    configuration::RuntimeStorePath,
};

pub type OutputOperationResult<T> = std::result::Result<T, OperationRejected>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputInterfaceRuntime {
    configuration: RuntimeConfiguration,
    clock: CollectionClock,
}

impl OutputInterfaceRuntime {
    pub fn new(configuration: RuntimeConfiguration, clock: CollectionClock) -> Self {
        Self {
            configuration,
            clock,
        }
    }

    pub fn list_sessions(
        &self,
        request: SessionListRequest,
    ) -> OutputOperationResult<SessionsListed> {
        let index =
            self.refreshed_index(&request.request_identifier, OperationKind::ListSessions)?;
        let validator = PageRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListSessions,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
        );
        validator.validate(&request.page)?;
        let lowered_time_window = self.lower_optional_time_window(
            request.filter.time_window.as_ref(),
            &request.request_identifier,
            OperationKind::ListSessions,
        )?;
        let mut sessions = index
            .current_sessions()
            .into_iter()
            .filter(|session| {
                SourceSelectionFilter::new(&request.filter.source_selection).accepts(session.source)
            })
            .filter(|session| {
                OptionalTimeWindowFilter::new(lowered_time_window.as_ref())
                    .accepts(session.chronology_timestamp())
            })
            .collect::<Vec<_>>();
        IndexedSessionSorter::new(request.page.order).sort(&mut sessions);
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::ListSessions,
            PageCollectionKind::Sessions,
            request.page.clone(),
            PaginationQueryShape::sessions(&request.filter, lowered_time_window.as_ref()),
        )
        .select(&sessions)?;
        Ok(SessionsListed {
            request_identifier: request.request_identifier,
            sessions: page.items.iter().map(IndexedSession::card).collect(),
            page: page.metadata,
        })
    }

    pub fn list_subagents(
        &self,
        request: SubagentListRequest,
    ) -> OutputOperationResult<SubagentsListed> {
        let index =
            self.refreshed_index(&request.request_identifier, OperationKind::ListSubagents)?;
        let validator = PageRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListSubagents,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
        );
        validator.validate(&request.page)?;
        ReferenceResolver::new(&index).resolve_session(
            &request.filter.session_reference,
            &request.request_identifier,
            OperationKind::ListSubagents,
        )?;
        let mut subagents = index
            .current_subagents()
            .into_iter()
            .filter(|subagent| subagent.session_reference == request.filter.session_reference)
            .filter(|subagent| {
                AuthoredStatusFilterMatcher::new(&request.filter.authored_status)
                    .accepts(subagent.authored_status)
            })
            .collect::<Vec<_>>();
        IndexedSubagentSorter::new(request.page.order).sort(&mut subagents);
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::ListSubagents,
            PageCollectionKind::Subagents,
            request.page.clone(),
            PaginationQueryShape::subagents(&request.filter),
        )
        .select(&subagents)?;
        Ok(SubagentsListed {
            request_identifier: request.request_identifier,
            subagents: page.items.iter().map(IndexedSubagent::card).collect(),
            page: page.metadata,
        })
    }

    pub fn list_outputs(&self, request: OutputListRequest) -> OutputOperationResult<OutputsListed> {
        let index =
            self.refreshed_index(&request.request_identifier, OperationKind::ListOutputs)?;
        let validator = PageRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputs,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
        );
        validator.validate(&request.page)?;
        ProjectionRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputs,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_preview_bytes,
        )
        .validate(&request.projection)?;
        if let Some(reference) = &request.filter.session_reference {
            ReferenceResolver::new(&index).resolve_session(
                reference,
                &request.request_identifier,
                OperationKind::ListOutputs,
            )?;
        }
        if let Some(reference) = &request.filter.subagent_reference {
            ReferenceResolver::new(&index).resolve_subagent(
                reference,
                &request.request_identifier,
                OperationKind::ListOutputs,
            )?;
        }
        let lowered_time_window = self.lower_optional_time_window(
            request.filter.time_window.as_ref(),
            &request.request_identifier,
            OperationKind::ListOutputs,
        )?;
        let mut outputs = index
            .current_outputs()
            .into_iter()
            .filter(|output| {
                SourceSelectionFilter::new(&request.filter.source_selection)
                    .accepts(output.provenance.source)
            })
            .filter(|output| {
                request
                    .filter
                    .session_reference
                    .as_ref()
                    .is_none_or(|reference| output.session_reference == *reference)
            })
            .filter(|output| {
                request
                    .filter
                    .subagent_reference
                    .as_ref()
                    .is_none_or(|reference| output.subagent_reference.as_ref() == Some(reference))
            })
            .filter(|output| {
                AuthoredStatusFilterMatcher::new(&request.filter.authored_status)
                    .accepts(output.provenance.authored_status)
            })
            .filter(|output| {
                OptionalTimeWindowFilter::new(lowered_time_window.as_ref())
                    .accepts(output.provenance.produced_at.as_ref())
            })
            .collect::<Vec<_>>();
        IndexedOutputSorter::new(request.page.order).sort(&mut outputs);
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputs,
            PageCollectionKind::Outputs,
            request.page.clone(),
            PaginationQueryShape::outputs(&request.filter, lowered_time_window.as_ref()),
        )
        .select(&outputs)?;
        Ok(OutputsListed {
            request_identifier: request.request_identifier,
            outputs: page
                .items
                .iter()
                .map(|output| output.card(&request.projection))
                .collect(),
            page: page.metadata,
        })
    }

    pub fn list_output_segments(
        &self,
        request: OutputSegmentListRequest,
    ) -> OutputOperationResult<OutputSegmentsListed> {
        let index = self.refreshed_index(
            &request.request_identifier,
            OperationKind::ListOutputSegments,
        )?;
        let validator = PageRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputSegments,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
        );
        validator.validate(&request.page)?;
        ProjectionRequestValidator::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputSegments,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_preview_bytes,
        )
        .validate(&request.projection)?;
        ReferenceResolver::new(&index).resolve_output(
            &request.filter.output_reference,
            &request.request_identifier,
            OperationKind::ListOutputSegments,
        )?;
        let mut segments = index
            .current_segments()
            .into_iter()
            .filter(|segment| segment.output_reference == request.filter.output_reference)
            .collect::<Vec<_>>();
        IndexedSegmentSorter::new(request.page.order).sort(&mut segments);
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::ListOutputSegments,
            PageCollectionKind::Segments,
            request.page.clone(),
            PaginationQueryShape::segments(&request.filter),
        )
        .select(&segments)?;
        Ok(OutputSegmentsListed {
            request_identifier: request.request_identifier,
            segments: page
                .items
                .iter()
                .map(|segment| segment.card(&request.projection))
                .collect(),
            page: page.metadata,
        })
    }

    pub fn estimate_output(
        &self,
        request: OutputEstimateRequest,
    ) -> OutputOperationResult<OutputEstimated> {
        let index = self.index_for_reference_operation(
            &request.request_identifier,
            OperationKind::EstimateOutput,
        )?;
        let output = ReferenceResolver::new(&index).resolve_output(
            &request.output_reference,
            &request.request_identifier,
            OperationKind::EstimateOutput,
        )?;
        let size = OutputRangeEstimator::new(&index, &output).estimate(
            &request.range,
            &request.request_identifier,
            OperationKind::EstimateOutput,
        )?;
        Ok(OutputEstimated {
            request_identifier: request.request_identifier,
            output_reference: request.output_reference,
            range: request.range,
            size,
        })
    }

    pub fn read_output(&self, request: OutputReadRequest) -> OutputOperationResult<OutputRead> {
        let index = self.index_for_reference_operation(
            &request.request_identifier,
            OperationKind::ReadOutput,
        )?;
        ReadLimitValidator::new(
            request.request_identifier.clone(),
            OperationKind::ReadOutput,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_read_bytes,
        )
        .validate(request.maximum_bytes)?;
        let output = ReferenceResolver::new(&index).resolve_output(
            &request.output_reference,
            &request.request_identifier,
            OperationKind::ReadOutput,
        )?;
        let text = OutputBackingReader::new(
            output.clone(),
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_read_bytes,
        )
        .read_text(&request.request_identifier, OperationKind::ReadOutput)?;
        let selected = OutputRangeReader::new(&index, output.clone(), text).read(
            &request.range,
            request.maximum_bytes,
            &request.request_identifier,
            OperationKind::ReadOutput,
        )?;
        Ok(OutputRead {
            request_identifier: request.request_identifier,
            output_reference: request.output_reference,
            range: request.range,
            size: selected.size,
            excerpt: selected.excerpt,
        })
    }

    pub fn list_transcript_blocks(
        &self,
        request: TranscriptBlockListRequest,
    ) -> OutputOperationResult<TranscriptBlocksListed> {
        let index = self.refreshed_index(
            &request.request_identifier,
            OperationKind::ListTranscriptBlocks,
        )?;
        TranscriptBlockRequestValidator::new(
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_preview_bytes,
        )
        .validate_listing(
            &request.request_identifier,
            OperationKind::ListTranscriptBlocks,
            &request.page,
            &request.projection,
        )?;
        TranscriptBlockReferenceFilterResolver::new(&index).resolve_filter_references(
            &request.filter,
            &request.request_identifier,
            OperationKind::ListTranscriptBlocks,
        )?;
        let lowered_time_window = self.lower_optional_time_window(
            request.filter.time_window.as_ref(),
            &request.request_identifier,
            OperationKind::ListTranscriptBlocks,
        )?;
        let mut blocks =
            TranscriptBlockFilterMatcher::new(&request.filter, lowered_time_window.as_ref())
                .matching_blocks(index.current_transcript_blocks());
        IndexedTranscriptBlockSorter::new(request.page.order).sort(&mut blocks);
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::ListTranscriptBlocks,
            PageCollectionKind::TranscriptBlocks,
            request.page.clone(),
            PaginationQueryShape::transcript_blocks(&request.filter, lowered_time_window.as_ref()),
        )
        .select(&blocks)?;
        Ok(TranscriptBlocksListed {
            request_identifier: request.request_identifier,
            blocks: page
                .items
                .iter()
                .map(|block| block.card(&request.projection))
                .collect(),
            page: page.metadata,
        })
    }

    pub fn search_transcript_blocks(
        &self,
        request: TranscriptBlockSearchRequest,
    ) -> OutputOperationResult<TranscriptBlocksSearched> {
        let index = self.refreshed_index(
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
        )?;
        TranscriptBlockRequestValidator::new(
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_page_items,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_preview_bytes,
        )
        .validate_listing(
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
            &request.page,
            &request.projection,
        )?;
        TranscriptBlockQueryValidator::new(&request.query).validate(
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
        )?;
        TranscriptBlockReferenceFilterResolver::new(&index).resolve_filter_references(
            &request.filter,
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
        )?;
        let lowered_time_window = self.lower_optional_time_window(
            request.filter.time_window.as_ref(),
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
        )?;
        let mut blocks =
            TranscriptBlockFilterMatcher::new(&request.filter, lowered_time_window.as_ref())
                .matching_blocks(index.current_transcript_blocks());
        IndexedTranscriptBlockSorter::new(request.page.order).sort(&mut blocks);
        let matches = TranscriptBlockSearcher::new(
            request.query.clone(),
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_read_bytes,
        )
        .search(
            blocks,
            &request.request_identifier,
            OperationKind::SearchTranscriptBlocks,
        )?;
        let page = PaginationWindow::new(
            request.request_identifier.clone(),
            OperationKind::SearchTranscriptBlocks,
            PageCollectionKind::TranscriptBlocks,
            request.page.clone(),
            PaginationQueryShape::transcript_block_search(
                &request.filter,
                lowered_time_window.as_ref(),
                &request.query,
            ),
        )
        .select(&matches)?;
        Ok(TranscriptBlocksSearched {
            request_identifier: request.request_identifier,
            matches: page
                .items
                .iter()
                .map(|match_record| match_record.reply_match(&request.projection))
                .collect(),
            page: page.metadata,
        })
    }

    pub fn estimate_transcript_block(
        &self,
        request: TranscriptBlockEstimateRequest,
    ) -> OutputOperationResult<TranscriptBlockEstimated> {
        let index = self.index_for_reference_operation(
            &request.request_identifier,
            OperationKind::EstimateTranscriptBlock,
        )?;
        let block = ReferenceResolver::new(&index).resolve_transcript_block(
            &request.block_reference,
            &request.request_identifier,
            OperationKind::EstimateTranscriptBlock,
        )?;
        Ok(TranscriptBlockEstimated {
            request_identifier: request.request_identifier,
            block_reference: request.block_reference,
            size: block.size,
        })
    }

    pub fn read_transcript_block(
        &self,
        request: TranscriptBlockReadRequest,
    ) -> OutputOperationResult<TranscriptBlockRead> {
        let index = self.index_for_reference_operation(
            &request.request_identifier,
            OperationKind::ReadTranscriptBlock,
        )?;
        ReadLimitValidator::new(
            request.request_identifier.clone(),
            OperationKind::ReadTranscriptBlock,
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_read_bytes,
        )
        .validate(request.maximum_bytes)?;
        let block = ReferenceResolver::new(&index).resolve_transcript_block(
            &request.block_reference,
            &request.request_identifier,
            OperationKind::ReadTranscriptBlock,
        )?;
        let text = TranscriptBlockBackingReader::new(
            block.clone(),
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_read_bytes,
        )
        .read_text(
            &request.request_identifier,
            OperationKind::ReadTranscriptBlock,
        )?;
        let selected = SelectedTranscriptBlockText::new(
            text,
            block.provenance.source,
            block.path.clone(),
            request.maximum_bytes,
        );
        Ok(TranscriptBlockRead {
            request_identifier: request.request_identifier,
            block_reference: request.block_reference,
            size: selected.size,
            excerpt: selected.excerpt,
        })
    }

    pub fn refreshed_index(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<DurableFragileIndex> {
        let store = FragileIndexStore::from_store_path(self.configuration.store_path());
        let mut durable = store.read_or_empty().map_err(|_| {
            OperationRejectedFactory::new(request_identifier.clone(), operation).unsupported()
        })?;
        let current = CurrentIndexBuilder::new(self.configuration.clone()).build();
        durable.merge_current(current);
        store.write(&durable).map_err(|_| {
            OperationRejectedFactory::new(request_identifier.clone(), operation).unsupported()
        })?;
        Ok(durable)
    }

    pub fn index_for_reference_operation(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<DurableFragileIndex> {
        let store = FragileIndexStore::from_store_path(self.configuration.store_path());
        let durable = store.read_or_empty().map_err(|_| {
            OperationRejectedFactory::new(request_identifier.clone(), operation).unsupported()
        })?;
        if durable.is_empty() {
            self.refreshed_index(request_identifier, operation)
        } else {
            Ok(durable)
        }
    }

    pub fn lower_optional_time_window(
        &self,
        time_window: Option<&TimeWindow>,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<Option<TimeWindow>> {
        time_window
            .map(|time_window| {
                self.clock.lower_time_window(time_window).map_err(|_| {
                    OperationRejectedFactory::new(request_identifier.clone(), operation)
                        .invalid_request()
                })
            })
            .transpose()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableFragileIndex {
    sessions: Vec<IndexedSession>,
    subagents: Vec<IndexedSubagent>,
    outputs: Vec<IndexedOutput>,
    segments: Vec<IndexedOutputSegment>,
    transcript_blocks: Vec<IndexedTranscriptBlock>,
    active_sessions: Vec<FragileSessionReference>,
    active_subagents: Vec<FragileSubagentReference>,
    active_outputs: Vec<FragileOutputReference>,
    active_segments: Vec<FragileOutputSegmentReference>,
    active_transcript_blocks: Vec<FragileTranscriptBlockReference>,
}

impl DurableFragileIndex {
    pub fn empty() -> Self {
        Self {
            sessions: Vec::new(),
            subagents: Vec::new(),
            outputs: Vec::new(),
            segments: Vec::new(),
            transcript_blocks: Vec::new(),
            active_sessions: Vec::new(),
            active_subagents: Vec::new(),
            active_outputs: Vec::new(),
            active_segments: Vec::new(),
            active_transcript_blocks: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty() && self.outputs.is_empty() && self.transcript_blocks.is_empty()
    }

    pub fn merge_current(&mut self, current: CurrentFragileIndex) {
        self.sessions =
            IndexedSessionMerger::new(self.sessions.clone(), current.sessions.clone()).merge();
        self.subagents =
            IndexedSubagentMerger::new(self.subagents.clone(), current.subagents.clone()).merge();
        self.outputs =
            IndexedOutputMerger::new(self.outputs.clone(), current.outputs.clone()).merge();
        self.segments =
            IndexedSegmentMerger::new(self.segments.clone(), current.segments.clone()).merge();
        self.transcript_blocks = IndexedTranscriptBlockMerger::new(
            self.transcript_blocks.clone(),
            current.transcript_blocks.clone(),
        )
        .merge();
        self.active_sessions = current
            .sessions
            .into_iter()
            .map(|session| session.reference)
            .collect();
        self.active_subagents = current
            .subagents
            .into_iter()
            .map(|subagent| subagent.reference)
            .collect();
        self.active_outputs = current
            .outputs
            .into_iter()
            .map(|output| output.reference)
            .collect();
        self.active_segments = current
            .segments
            .into_iter()
            .map(|segment| segment.reference)
            .collect();
        self.active_transcript_blocks = current
            .transcript_blocks
            .into_iter()
            .map(|block| block.reference)
            .collect();
    }

    pub fn current_sessions(&self) -> Vec<IndexedSession> {
        let active = self
            .active_sessions
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<BTreeSet<_>>();
        self.sessions
            .iter()
            .filter(|session| active.contains(session.reference.as_str()))
            .cloned()
            .collect()
    }

    pub fn current_subagents(&self) -> Vec<IndexedSubagent> {
        let active = self
            .active_subagents
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<BTreeSet<_>>();
        self.subagents
            .iter()
            .filter(|subagent| active.contains(subagent.reference.as_str()))
            .cloned()
            .collect()
    }

    pub fn current_outputs(&self) -> Vec<IndexedOutput> {
        let active = self
            .active_outputs
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<BTreeSet<_>>();
        self.outputs
            .iter()
            .filter(|output| active.contains(output.reference.as_str()))
            .cloned()
            .collect()
    }

    pub fn current_segments(&self) -> Vec<IndexedOutputSegment> {
        let active = self
            .active_segments
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<BTreeSet<_>>();
        self.segments
            .iter()
            .filter(|segment| active.contains(segment.reference.as_str()))
            .cloned()
            .collect()
    }

    pub fn current_transcript_blocks(&self) -> Vec<IndexedTranscriptBlock> {
        let active = self
            .active_transcript_blocks
            .iter()
            .map(|reference| reference.as_str().to_string())
            .collect::<BTreeSet<_>>();
        self.transcript_blocks
            .iter()
            .filter(|block| active.contains(block.reference.as_str()))
            .cloned()
            .collect()
    }

    pub fn session(&self, reference: &FragileSessionReference) -> Option<IndexedSession> {
        self.sessions
            .iter()
            .find(|session| &session.reference == reference)
            .cloned()
    }

    pub fn subagent(&self, reference: &FragileSubagentReference) -> Option<IndexedSubagent> {
        self.subagents
            .iter()
            .find(|subagent| &subagent.reference == reference)
            .cloned()
    }

    pub fn output(&self, reference: &FragileOutputReference) -> Option<IndexedOutput> {
        self.outputs
            .iter()
            .find(|output| &output.reference == reference)
            .cloned()
    }

    pub fn segment(
        &self,
        reference: &FragileOutputSegmentReference,
    ) -> Option<IndexedOutputSegment> {
        self.segments
            .iter()
            .find(|segment| &segment.reference == reference)
            .cloned()
    }

    pub fn transcript_block(
        &self,
        reference: &FragileTranscriptBlockReference,
    ) -> Option<IndexedTranscriptBlock> {
        self.transcript_blocks
            .iter()
            .find(|block| &block.reference == reference)
            .cloned()
    }

    pub fn collection_signature(&self) -> String {
        StableHash::new(
            self.active_sessions
                .iter()
                .map(|reference| reference.as_str())
                .chain(
                    self.active_subagents
                        .iter()
                        .map(|reference| reference.as_str()),
                )
                .chain(
                    self.active_outputs
                        .iter()
                        .map(|reference| reference.as_str()),
                )
                .chain(
                    self.active_segments
                        .iter()
                        .map(|reference| reference.as_str()),
                )
                .chain(
                    self.active_transcript_blocks
                        .iter()
                        .map(|reference| reference.as_str()),
                )
                .collect::<Vec<_>>()
                .join("|"),
        )
        .hex()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "version": 1,
            "active_sessions": self.active_sessions.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "active_subagents": self.active_subagents.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "active_outputs": self.active_outputs.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "active_segments": self.active_segments.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "active_transcript_blocks": self.active_transcript_blocks.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "sessions": self.sessions.iter().map(IndexedSession::to_json).collect::<Vec<_>>(),
            "subagents": self.subagents.iter().map(IndexedSubagent::to_json).collect::<Vec<_>>(),
            "outputs": self.outputs.iter().map(IndexedOutput::to_json).collect::<Vec<_>>(),
            "segments": self.segments.iter().map(IndexedOutputSegment::to_json).collect::<Vec<_>>(),
            "transcript_blocks": self.transcript_blocks.iter().map(IndexedTranscriptBlock::to_json).collect::<Vec<_>>(),
        })
    }

    pub fn from_json(value: &Value) -> Self {
        let reader = JsonReader::new(value);
        Self {
            active_sessions: reader
                .strings("active_sessions")
                .into_iter()
                .map(FragileSessionReference::new)
                .collect(),
            active_subagents: reader
                .strings("active_subagents")
                .into_iter()
                .map(FragileSubagentReference::new)
                .collect(),
            active_outputs: reader
                .strings("active_outputs")
                .into_iter()
                .map(FragileOutputReference::new)
                .collect(),
            active_segments: reader
                .strings("active_segments")
                .into_iter()
                .map(FragileOutputSegmentReference::new)
                .collect(),
            active_transcript_blocks: reader
                .strings("active_transcript_blocks")
                .into_iter()
                .map(FragileTranscriptBlockReference::new)
                .collect(),
            sessions: reader
                .array("sessions")
                .into_iter()
                .filter_map(IndexedSession::from_json)
                .collect(),
            subagents: reader
                .array("subagents")
                .into_iter()
                .filter_map(IndexedSubagent::from_json)
                .collect(),
            outputs: reader
                .array("outputs")
                .into_iter()
                .filter_map(IndexedOutput::from_json)
                .collect(),
            segments: reader
                .array("segments")
                .into_iter()
                .filter_map(IndexedOutputSegment::from_json)
                .collect(),
            transcript_blocks: reader
                .array("transcript_blocks")
                .into_iter()
                .filter_map(IndexedTranscriptBlock::from_json)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentFragileIndex {
    sessions: Vec<IndexedSession>,
    subagents: Vec<IndexedSubagent>,
    outputs: Vec<IndexedOutput>,
    segments: Vec<IndexedOutputSegment>,
    transcript_blocks: Vec<IndexedTranscriptBlock>,
}

impl CurrentFragileIndex {
    pub fn new(
        sessions: Vec<IndexedSession>,
        subagents: Vec<IndexedSubagent>,
        outputs: Vec<IndexedOutput>,
        segments: Vec<IndexedOutputSegment>,
        transcript_blocks: Vec<IndexedTranscriptBlock>,
    ) -> Self {
        Self {
            sessions,
            subagents,
            outputs,
            segments,
            transcript_blocks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentIndexBuilder {
    configuration: RuntimeConfiguration,
}

impl CurrentIndexBuilder {
    pub fn new(configuration: RuntimeConfiguration) -> Self {
        Self { configuration }
    }

    pub fn build(&self) -> CurrentFragileIndex {
        let mut accumulator = CurrentIndexAccumulator::new(
            self.configuration
                .output_interfaces()
                .limits()
                .maximum_preview_bytes,
        );
        for source in self.configuration.transcript_sources() {
            accumulator.merge(self.read_source(source));
        }
        accumulator.finish()
    }

    pub fn read_source(&self, source: &TranscriptAdapterConfiguration) -> TranscriptRawReadOutcome {
        match source {
            TranscriptAdapterConfiguration::Claude(root) => {
                crate::adapter::claude::ClaudeJsonlRootReader::new(root.path().to_path_buf())
                    .read_records()
            }
            TranscriptAdapterConfiguration::Codex(root) => {
                crate::adapter::codex::CodexSessionRootReader::new(root.path().to_path_buf())
                    .read_records()
            }
            TranscriptAdapterConfiguration::Pi(root) => {
                crate::adapter::pi::PiRunHistoryRootReader::new(root.path().to_path_buf())
                    .read_records()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentIndexAccumulator {
    sessions: BTreeMap<String, SessionAccumulator>,
    preview_limit: ByteLimit,
}

impl CurrentIndexAccumulator {
    pub fn new(preview_limit: ByteLimit) -> Self {
        Self {
            sessions: BTreeMap::new(),
            preview_limit,
        }
    }

    pub fn merge(&mut self, outcome: TranscriptRawReadOutcome) {
        for record in outcome.records {
            self.observe(record);
        }
    }

    pub fn observe(&mut self, record: TranscriptRecord) {
        let session_key = record.path.display().to_string();
        self.sessions
            .entry(session_key)
            .or_insert_with(|| {
                SessionAccumulator::new(
                    record.source,
                    record.source_identifier.clone(),
                    record.path.clone(),
                    self.preview_limit,
                )
            })
            .observe(record);
    }

    pub fn finish(self) -> CurrentFragileIndex {
        let mut sessions = Vec::new();
        let mut subagents = Vec::new();
        let mut outputs = Vec::new();
        let mut segments = Vec::new();
        let mut transcript_blocks = Vec::new();
        for accumulator in self.sessions.into_values() {
            let indexed = accumulator.finish();
            sessions.push(indexed.session);
            subagents.extend(indexed.subagents);
            outputs.extend(indexed.outputs);
            segments.extend(indexed.segments);
            transcript_blocks.extend(indexed.transcript_blocks);
        }
        sessions.sort_by(|left, right| left.reference.as_str().cmp(right.reference.as_str()));
        subagents.sort_by(|left, right| left.reference.as_str().cmp(right.reference.as_str()));
        outputs.sort_by(|left, right| left.reference.as_str().cmp(right.reference.as_str()));
        segments.sort_by(|left, right| left.reference.as_str().cmp(right.reference.as_str()));
        transcript_blocks
            .sort_by(|left, right| left.reference.as_str().cmp(right.reference.as_str()));
        CurrentFragileIndex::new(sessions, subagents, outputs, segments, transcript_blocks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAccumulator {
    source: SourceKind,
    source_identifier: signal_aggregator::SourceIdentifier,
    path: PathBuf,
    fingerprint: SourceFingerprint,
    preview_limit: ByteLimit,
    outputs: Vec<IndexedOutput>,
    segments: Vec<IndexedOutputSegment>,
    transcript_blocks: Vec<IndexedTranscriptBlock>,
    subagents: BTreeMap<String, SubagentAccumulator>,
    size: SizeAccumulator,
    started_at: Option<Timestamp>,
    last_observed_at: Option<Timestamp>,
}

impl SessionAccumulator {
    pub fn new(
        source: SourceKind,
        source_identifier: signal_aggregator::SourceIdentifier,
        path: PathBuf,
        preview_limit: ByteLimit,
    ) -> Self {
        let fingerprint =
            SourceFingerprint::from_path(&path).unwrap_or_else(|_| SourceFingerprint::missing());
        Self {
            source,
            source_identifier,
            path,
            fingerprint,
            preview_limit,
            outputs: Vec::new(),
            segments: Vec::new(),
            transcript_blocks: Vec::new(),
            subagents: BTreeMap::new(),
            size: SizeAccumulator::new(),
            started_at: None,
            last_observed_at: None,
        }
    }

    pub fn observe(&mut self, record: TranscriptRecord) {
        self.size.observe_text(&record.text);
        self.observe_timestamp(record.timestamp.clone());
        let session_reference = self.session_reference();
        let subagent_reference = record
            .subagent_name
            .as_ref()
            .map(|name| self.subagent_reference(name));
        let output = IndexedOutput::from_record(
            record.clone(),
            session_reference.clone(),
            subagent_reference.clone(),
            self.fingerprint.clone(),
            self.preview_limit,
        );
        let segment = IndexedOutputSegment::from_output(&output);
        if let Some(name) = record.subagent_name.clone() {
            let key = name.as_str().to_string();
            let subagent_reference = self.subagent_reference(&name);
            self.subagents
                .entry(key)
                .or_insert_with(|| {
                    SubagentAccumulator::new(session_reference.clone(), subagent_reference, name)
                })
                .observe(&output);
        }
        for block_record in record.transcript_blocks() {
            let block_subagent_reference = block_record
                .subagent_name
                .as_ref()
                .map(|name| self.subagent_reference(name));
            self.transcript_blocks
                .push(IndexedTranscriptBlock::from_record(
                    block_record,
                    session_reference.clone(),
                    block_subagent_reference,
                    self.fingerprint.clone(),
                    self.preview_limit,
                ));
        }
        self.segments.push(segment);
        self.outputs.push(output);
    }

    pub fn observe_timestamp(&mut self, timestamp: Option<Timestamp>) {
        let Some(timestamp) = timestamp else {
            return;
        };
        if TimestampOrdering::new(&timestamp).is_before_optional(self.started_at.as_ref()) {
            self.started_at = Some(timestamp.clone());
        }
        if TimestampOrdering::new(&timestamp).is_after_optional(self.last_observed_at.as_ref()) {
            self.last_observed_at = Some(timestamp);
        }
    }

    pub fn session_reference(&self) -> FragileSessionReference {
        FragileSessionReference::new(
            StableReference::new("session", self.session_reference_material()).as_string(),
        )
    }

    pub fn session_reference_material(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            SourceKindName::new(self.source).as_str(),
            self.source_identifier.as_str(),
            self.path.display(),
            self.fingerprint.material()
        )
    }

    pub fn subagent_reference(
        &self,
        name: &signal_aggregator::SubagentName,
    ) -> FragileSubagentReference {
        FragileSubagentReference::new(
            StableReference::new(
                "subagent",
                format!("{}|{}", self.session_reference().as_str(), name.as_str()),
            )
            .as_string(),
        )
    }

    pub fn finish(self) -> IndexedSessionBundle {
        let session_reference = self.session_reference();
        let subagents = self
            .subagents
            .into_values()
            .map(SubagentAccumulator::finish)
            .collect::<Vec<_>>();
        IndexedSessionBundle {
            session: IndexedSession {
                reference: session_reference,
                source: self.source,
                source_identifier: self.source_identifier,
                path: self.path,
                fingerprint: self.fingerprint,
                started_at: self.started_at,
                last_observed_at: self.last_observed_at,
                subagent_references: subagents
                    .iter()
                    .map(|subagent| subagent.reference.clone())
                    .collect(),
                output_references: self
                    .outputs
                    .iter()
                    .map(|output| output.reference.clone())
                    .collect(),
                size: self.size.finish(),
            },
            subagents,
            outputs: self.outputs,
            segments: self.segments,
            transcript_blocks: self.transcript_blocks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSessionBundle {
    session: IndexedSession,
    subagents: Vec<IndexedSubagent>,
    outputs: Vec<IndexedOutput>,
    segments: Vec<IndexedOutputSegment>,
    transcript_blocks: Vec<IndexedTranscriptBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentAccumulator {
    session_reference: FragileSessionReference,
    reference: FragileSubagentReference,
    name: signal_aggregator::SubagentName,
    authored_status: AuthoredStatusAccumulator,
    output_references: Vec<FragileOutputReference>,
    size: SizeAccumulator,
    first_observed_at: Option<Timestamp>,
    last_observed_at: Option<Timestamp>,
}

impl SubagentAccumulator {
    pub fn new(
        session_reference: FragileSessionReference,
        reference: FragileSubagentReference,
        name: signal_aggregator::SubagentName,
    ) -> Self {
        Self {
            session_reference,
            reference,
            name,
            authored_status: AuthoredStatusAccumulator::new(),
            output_references: Vec::new(),
            size: SizeAccumulator::new(),
            first_observed_at: None,
            last_observed_at: None,
        }
    }

    pub fn observe(&mut self, output: &IndexedOutput) {
        self.output_references.push(output.reference.clone());
        self.size.observe_size(&output.size);
        self.authored_status
            .observe(output.provenance.authored_status);
        if let Some(timestamp) = &output.provenance.produced_at {
            if TimestampOrdering::new(timestamp).is_before_optional(self.first_observed_at.as_ref())
            {
                self.first_observed_at = Some(timestamp.clone());
            }
            if TimestampOrdering::new(timestamp).is_after_optional(self.last_observed_at.as_ref()) {
                self.last_observed_at = Some(timestamp.clone());
            }
        }
    }

    pub fn finish(self) -> IndexedSubagent {
        IndexedSubagent {
            reference: self.reference,
            session_reference: self.session_reference,
            name: self.name,
            authored_status: self.authored_status.finish(),
            output_references: self.output_references,
            size: self.size.finish(),
            first_observed_at: self.first_observed_at,
            last_observed_at: self.last_observed_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSession {
    reference: FragileSessionReference,
    source: SourceKind,
    source_identifier: signal_aggregator::SourceIdentifier,
    path: PathBuf,
    fingerprint: SourceFingerprint,
    started_at: Option<Timestamp>,
    last_observed_at: Option<Timestamp>,
    subagent_references: Vec<FragileSubagentReference>,
    output_references: Vec<FragileOutputReference>,
    size: SizeMetadata,
}

impl IndexedSession {
    pub fn card(&self) -> SessionCard {
        SessionCard {
            reference: self.reference.clone(),
            source: self.source,
            source_identifier: self.source_identifier.clone(),
            started_at: self.started_at.clone(),
            last_observed_at: self.last_observed_at.clone(),
            subagent_count: Some(ItemCount::new(self.subagent_references.len() as u64)),
            output_count: Some(ItemCount::new(self.output_references.len() as u64)),
            size: self.size.clone(),
        }
    }

    pub fn chronology_timestamp(&self) -> Option<&Timestamp> {
        self.last_observed_at.as_ref().or(self.started_at.as_ref())
    }

    pub fn to_json(&self) -> Value {
        json!({
            "reference": self.reference.as_str(),
            "source": SourceKindName::new(self.source).as_str(),
            "source_identifier": self.source_identifier.as_str(),
            "path": self.path.display().to_string(),
            "fingerprint": self.fingerprint.to_json(),
            "started_at": self.started_at.as_ref().map(|value| value.as_str()),
            "last_observed_at": self.last_observed_at.as_ref().map(|value| value.as_str()),
            "subagent_references": self.subagent_references.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "output_references": self.output_references.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "size": SizeMetadataJson::new(&self.size).to_json(),
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        Some(Self {
            reference: FragileSessionReference::new(reader.string("reference")?),
            source: SourceKindName::parse(&reader.string("source")?)?,
            source_identifier: signal_aggregator::SourceIdentifier::new(
                reader.string("source_identifier")?,
            ),
            path: PathBuf::from(reader.string("path")?),
            fingerprint: SourceFingerprint::from_json(reader.value("fingerprint")?)?,
            started_at: reader.optional_string("started_at").map(Timestamp::new),
            last_observed_at: reader
                .optional_string("last_observed_at")
                .map(Timestamp::new),
            subagent_references: reader
                .strings("subagent_references")
                .into_iter()
                .map(FragileSubagentReference::new)
                .collect(),
            output_references: reader
                .strings("output_references")
                .into_iter()
                .map(FragileOutputReference::new)
                .collect(),
            size: SizeMetadataJson::from_json(reader.value("size")?)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSubagent {
    reference: FragileSubagentReference,
    session_reference: FragileSessionReference,
    name: signal_aggregator::SubagentName,
    authored_status: AuthoredStatus,
    output_references: Vec<FragileOutputReference>,
    size: SizeMetadata,
    first_observed_at: Option<Timestamp>,
    last_observed_at: Option<Timestamp>,
}

impl IndexedSubagent {
    pub fn card(&self) -> SubagentCard {
        SubagentCard {
            reference: self.reference.clone(),
            session_reference: self.session_reference.clone(),
            name: self.name.clone(),
            authored_status: self.authored_status,
            output_count: Some(ItemCount::new(self.output_references.len() as u64)),
            size: self.size.clone(),
            first_observed_at: self.first_observed_at.clone(),
            last_observed_at: self.last_observed_at.clone(),
        }
    }

    pub fn chronology_timestamp(&self) -> Option<&Timestamp> {
        self.last_observed_at
            .as_ref()
            .or(self.first_observed_at.as_ref())
    }

    pub fn to_json(&self) -> Value {
        json!({
            "reference": self.reference.as_str(),
            "session_reference": self.session_reference.as_str(),
            "name": self.name.as_str(),
            "authored_status": AuthoredStatusName::new(self.authored_status).as_str(),
            "output_references": self.output_references.iter().map(|reference| reference.as_str().to_string()).collect::<Vec<_>>(),
            "size": SizeMetadataJson::new(&self.size).to_json(),
            "first_observed_at": self.first_observed_at.as_ref().map(|value| value.as_str()),
            "last_observed_at": self.last_observed_at.as_ref().map(|value| value.as_str()),
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        Some(Self {
            reference: FragileSubagentReference::new(reader.string("reference")?),
            session_reference: FragileSessionReference::new(reader.string("session_reference")?),
            name: signal_aggregator::SubagentName::new(reader.string("name")?),
            authored_status: AuthoredStatusName::parse(&reader.string("authored_status")?)?,
            output_references: reader
                .strings("output_references")
                .into_iter()
                .map(FragileOutputReference::new)
                .collect(),
            size: SizeMetadataJson::from_json(reader.value("size")?)?,
            first_observed_at: reader
                .optional_string("first_observed_at")
                .map(Timestamp::new),
            last_observed_at: reader
                .optional_string("last_observed_at")
                .map(Timestamp::new),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedOutput {
    reference: FragileOutputReference,
    session_reference: FragileSessionReference,
    subagent_reference: Option<FragileSubagentReference>,
    title: Option<signal_aggregator::OutputTitle>,
    provenance: OutputProvenance,
    path: PathBuf,
    fingerprint: SourceFingerprint,
    source_line_number: u64,
    text_hash: String,
    size: SizeMetadata,
    preview_text: String,
    preview_original_bytes: u64,
}

impl IndexedOutput {
    pub fn from_record(
        record: TranscriptRecord,
        session_reference: FragileSessionReference,
        subagent_reference: Option<FragileSubagentReference>,
        fingerprint: SourceFingerprint,
        preview_limit: ByteLimit,
    ) -> Self {
        let text_hash = StableHash::new(&record.text).hex();
        let reference = FragileOutputReference::new(
            StableReference::new(
                "output",
                format!(
                    "{}|{}|{}|{}|{}|{}",
                    SourceKindName::new(record.source).as_str(),
                    record.source_identifier.as_str(),
                    record.path.display(),
                    record.line_number,
                    fingerprint.material(),
                    text_hash
                ),
            )
            .as_string(),
        );
        let size = SizeMetadataFactory::from_text(&record.text, Some(1)).exact();
        let preview_text = Utf8Prefix::new(&record.text, preview_limit.into_u64()).into_string();
        let preview_original_bytes = record.byte_count();
        Self {
            reference,
            session_reference,
            subagent_reference,
            title: record.title,
            provenance: OutputProvenance {
                source: record.source,
                source_identifier: record.source_identifier,
                authored_status: record.authored_status,
                produced_at: record.timestamp,
            },
            path: record.path,
            fingerprint,
            source_line_number: record.line_number,
            text_hash,
            size,
            preview_text,
            preview_original_bytes,
        }
    }

    pub fn card(&self, projection: &CardProjection) -> OutputCard {
        OutputCard {
            reference: self.reference.clone(),
            session_reference: self.session_reference.clone(),
            subagent_reference: self.subagent_reference.clone(),
            title: self.title.clone(),
            provenance: self.provenance.clone(),
            size: self.size.clone(),
            preview: PreviewProjector::new(
                self.preview_text.clone(),
                self.preview_original_bytes,
                self.provenance.source,
                self.path.clone(),
            )
            .project(projection),
        }
    }

    pub fn chronology_timestamp(&self) -> Option<&Timestamp> {
        self.provenance.produced_at.as_ref()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "reference": self.reference.as_str(),
            "session_reference": self.session_reference.as_str(),
            "subagent_reference": self.subagent_reference.as_ref().map(|value| value.as_str()),
            "title": self.title.as_ref().map(|value| value.as_str()),
            "source": SourceKindName::new(self.provenance.source).as_str(),
            "source_identifier": self.provenance.source_identifier.as_str(),
            "authored_status": AuthoredStatusName::new(self.provenance.authored_status).as_str(),
            "produced_at": self.provenance.produced_at.as_ref().map(|value| value.as_str()),
            "path": self.path.display().to_string(),
            "fingerprint": self.fingerprint.to_json(),
            "source_line_number": self.source_line_number,
            "text_hash": self.text_hash,
            "size": SizeMetadataJson::new(&self.size).to_json(),
            "preview_text": self.preview_text,
            "preview_original_bytes": self.preview_original_bytes,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        let source = SourceKindName::parse(&reader.string("source")?)?;
        Some(Self {
            reference: FragileOutputReference::new(reader.string("reference")?),
            session_reference: FragileSessionReference::new(reader.string("session_reference")?),
            subagent_reference: reader
                .optional_string("subagent_reference")
                .map(FragileSubagentReference::new),
            title: reader
                .optional_string("title")
                .map(signal_aggregator::OutputTitle::new),
            provenance: OutputProvenance {
                source,
                source_identifier: signal_aggregator::SourceIdentifier::new(
                    reader.string("source_identifier")?,
                ),
                authored_status: AuthoredStatusName::parse(&reader.string("authored_status")?)?,
                produced_at: reader.optional_string("produced_at").map(Timestamp::new),
            },
            path: PathBuf::from(reader.string("path")?),
            fingerprint: SourceFingerprint::from_json(reader.value("fingerprint")?)?,
            source_line_number: reader.u64("source_line_number")?,
            text_hash: reader.string("text_hash")?,
            size: SizeMetadataJson::from_json(reader.value("size")?)?,
            preview_text: reader.string("preview_text")?,
            preview_original_bytes: reader.u64("preview_original_bytes")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedOutputSegment {
    reference: FragileOutputSegmentReference,
    output_reference: FragileOutputReference,
    segment_index: SegmentIndex,
    byte_range: Option<ByteRange>,
    line_range: Option<LineRange>,
    size: SizeMetadata,
    preview_text: String,
    preview_original_bytes: u64,
    source: SourceKind,
    path: PathBuf,
}

impl IndexedOutputSegment {
    pub fn from_output(output: &IndexedOutput) -> Self {
        Self {
            reference: FragileOutputSegmentReference::new(
                StableReference::new("segment", format!("{}|0", output.reference.as_str()))
                    .as_string(),
            ),
            output_reference: output.reference.clone(),
            segment_index: SegmentIndex::new(0),
            byte_range: Some(ByteRange {
                start: ByteCount::new(0),
                end: ByteCount::new(output.size.byte_count.map_or(0, ByteCount::into_u64)),
            }),
            line_range: Some(LineRange {
                start: LineNumber::new(1),
                end: LineNumber::new(
                    output
                        .size
                        .line_count
                        .map_or(1, |count| count.into_u64() + 1),
                ),
            }),
            size: output.size.clone(),
            preview_text: output.preview_text.clone(),
            preview_original_bytes: output.preview_original_bytes,
            source: output.provenance.source,
            path: output.path.clone(),
        }
    }

    pub fn card(&self, projection: &CardProjection) -> OutputSegmentCard {
        OutputSegmentCard {
            reference: self.reference.clone(),
            output_reference: self.output_reference.clone(),
            segment_index: self.segment_index,
            byte_range: self.byte_range.clone(),
            line_range: self.line_range.clone(),
            size: self.size.clone(),
            preview: PreviewProjector::new(
                self.preview_text.clone(),
                self.preview_original_bytes,
                self.source,
                self.path.clone(),
            )
            .project(projection),
        }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "reference": self.reference.as_str(),
            "output_reference": self.output_reference.as_str(),
            "segment_index": self.segment_index.into_u64(),
            "byte_start": self.byte_range.as_ref().map(|range| range.start.into_u64()),
            "byte_end": self.byte_range.as_ref().map(|range| range.end.into_u64()),
            "line_start": self.line_range.as_ref().map(|range| range.start.into_u64()),
            "line_end": self.line_range.as_ref().map(|range| range.end.into_u64()),
            "size": SizeMetadataJson::new(&self.size).to_json(),
            "preview_text": self.preview_text,
            "preview_original_bytes": self.preview_original_bytes,
            "source": SourceKindName::new(self.source).as_str(),
            "path": self.path.display().to_string(),
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        let byte_start = reader.optional_u64("byte_start");
        let byte_end = reader.optional_u64("byte_end");
        let line_start = reader.optional_u64("line_start");
        let line_end = reader.optional_u64("line_end");
        Some(Self {
            reference: FragileOutputSegmentReference::new(reader.string("reference")?),
            output_reference: FragileOutputReference::new(reader.string("output_reference")?),
            segment_index: SegmentIndex::new(reader.u64("segment_index")?),
            byte_range: byte_start.zip(byte_end).map(|(start, end)| ByteRange {
                start: ByteCount::new(start),
                end: ByteCount::new(end),
            }),
            line_range: line_start.zip(line_end).map(|(start, end)| LineRange {
                start: LineNumber::new(start),
                end: LineNumber::new(end),
            }),
            size: SizeMetadataJson::from_json(reader.value("size")?)?,
            preview_text: reader.string("preview_text")?,
            preview_original_bytes: reader.u64("preview_original_bytes")?,
            source: SourceKindName::parse(&reader.string("source")?)?,
            path: PathBuf::from(reader.string("path")?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedTranscriptBlock {
    reference: FragileTranscriptBlockReference,
    session_reference: FragileSessionReference,
    subagent_reference: Option<FragileSubagentReference>,
    kind: TranscriptBlockKind,
    block_index: signal_aggregator::TranscriptBlockIndex,
    provenance: TranscriptBlockProvenance,
    path: PathBuf,
    fingerprint: SourceFingerprint,
    source_line_number: u64,
    text_hash: String,
    size: SizeMetadata,
    text_availability: TranscriptBlockTextAvailability,
    preview_text: String,
    preview_original_bytes: u64,
}

impl IndexedTranscriptBlock {
    pub fn from_record(
        record: TranscriptBlockRecord,
        session_reference: FragileSessionReference,
        subagent_reference: Option<FragileSubagentReference>,
        fingerprint: SourceFingerprint,
        preview_limit: ByteLimit,
    ) -> Self {
        let text_hash = record
            .readable_text()
            .map(StableHash::new)
            .map(|hash| hash.hex())
            .unwrap_or_else(|| StableHash::new("unavailable").hex());
        let reference = FragileTranscriptBlockReference::new(
            StableReference::new(
                "transcript-block",
                format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}",
                    SourceKindName::new(record.source).as_str(),
                    record.source_identifier.as_str(),
                    record.path.display(),
                    record.line_number,
                    record.block_index,
                    TranscriptBlockKindName::new(record.kind).as_str(),
                    fingerprint.material(),
                    text_hash
                ),
            )
            .as_string(),
        );
        let size = record
            .readable_text()
            .map(|text| SizeMetadataFactory::from_text(text, None).exact())
            .unwrap_or_else(SizeMetadataFactory::unknown);
        let preview_text = record
            .readable_text()
            .map(|text| Utf8Prefix::new(text, preview_limit.into_u64()).into_string())
            .unwrap_or_default();
        let preview_original_bytes = record.byte_count().unwrap_or(0);
        Self {
            reference,
            session_reference,
            subagent_reference,
            kind: record.kind,
            block_index: signal_aggregator::TranscriptBlockIndex::new(record.block_index),
            provenance: TranscriptBlockProvenance {
                source: record.source,
                source_identifier: record.source_identifier,
                authored_status: record.authored_status,
                observed_at: record.timestamp,
            },
            path: record.path,
            fingerprint,
            source_line_number: record.line_number,
            text_hash,
            size,
            text_availability: record.text_availability,
            preview_text,
            preview_original_bytes,
        }
    }

    pub fn card(&self, projection: &CardProjection) -> TranscriptBlockCard {
        TranscriptBlockCard {
            reference: self.reference.clone(),
            session_reference: self.session_reference.clone(),
            subagent_reference: self.subagent_reference.clone(),
            kind: self.kind,
            block_index: self.block_index,
            provenance: self.provenance.clone(),
            line_range: Some(LineRange {
                start: LineNumber::new(self.source_line_number),
                end: LineNumber::new(self.source_line_number + 1),
            }),
            byte_range: None,
            size: self.size.clone(),
            text_availability: self.text_availability,
            preview: TranscriptBlockPreviewProjector::new(
                self.preview_text.clone(),
                self.preview_original_bytes,
                self.provenance.source,
                self.path.clone(),
                self.text_availability,
            )
            .project(projection),
        }
    }

    pub fn chronology_timestamp(&self) -> Option<&Timestamp> {
        self.provenance.observed_at.as_ref()
    }

    pub fn source_sort_material(&self) -> String {
        StableSignatureMaterial::new("transcript-block-source-sort")
            .field(
                "source",
                SourceKindName::new(self.provenance.source).as_str(),
            )
            .field(
                "source_identifier",
                self.provenance.source_identifier.as_str(),
            )
            .field("path", self.path.display().to_string())
            .finish()
    }

    pub fn size_byte_count(&self) -> u64 {
        self.size.byte_count.map_or(0, ByteCount::into_u64)
    }

    pub fn to_json(&self) -> Value {
        json!({
            "reference": self.reference.as_str(),
            "session_reference": self.session_reference.as_str(),
            "subagent_reference": self.subagent_reference.as_ref().map(|value| value.as_str()),
            "kind": TranscriptBlockKindName::new(self.kind).as_str(),
            "block_index": self.block_index.into_u64(),
            "source": SourceKindName::new(self.provenance.source).as_str(),
            "source_identifier": self.provenance.source_identifier.as_str(),
            "authored_status": AuthoredStatusName::new(self.provenance.authored_status).as_str(),
            "observed_at": self.provenance.observed_at.as_ref().map(|value| value.as_str()),
            "path": self.path.display().to_string(),
            "fingerprint": self.fingerprint.to_json(),
            "source_line_number": self.source_line_number,
            "text_hash": self.text_hash,
            "size": SizeMetadataJson::new(&self.size).to_json(),
            "text_availability": TranscriptBlockTextAvailabilityName::new(self.text_availability).as_str(),
            "preview_text": self.preview_text,
            "preview_original_bytes": self.preview_original_bytes,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        let source = SourceKindName::parse(&reader.string("source")?)?;
        Some(Self {
            reference: FragileTranscriptBlockReference::new(reader.string("reference")?),
            session_reference: FragileSessionReference::new(reader.string("session_reference")?),
            subagent_reference: reader
                .optional_string("subagent_reference")
                .map(FragileSubagentReference::new),
            kind: TranscriptBlockKindName::parse(&reader.string("kind")?)?,
            block_index: signal_aggregator::TranscriptBlockIndex::new(reader.u64("block_index")?),
            provenance: TranscriptBlockProvenance {
                source,
                source_identifier: signal_aggregator::SourceIdentifier::new(
                    reader.string("source_identifier")?,
                ),
                authored_status: AuthoredStatusName::parse(&reader.string("authored_status")?)?,
                observed_at: reader.optional_string("observed_at").map(Timestamp::new),
            },
            path: PathBuf::from(reader.string("path")?),
            fingerprint: SourceFingerprint::from_json(reader.value("fingerprint")?)?,
            source_line_number: reader.u64("source_line_number")?,
            text_hash: reader.string("text_hash")?,
            size: SizeMetadataJson::from_json(reader.value("size")?)?,
            text_availability: TranscriptBlockTextAvailabilityName::parse(
                &reader.string("text_availability")?,
            )?,
            preview_text: reader.string("preview_text")?,
            preview_original_bytes: reader.u64("preview_original_bytes")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragileIndexStore {
    path: PathBuf,
}

impl FragileIndexStore {
    pub fn from_store_path(store_path: &Path) -> Self {
        Self {
            path: RuntimeStorePath::new(store_path.to_path_buf()).fragile_index_path(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_or_empty(&self) -> Result<DurableFragileIndex> {
        if !self.path.exists() {
            return Ok(DurableFragileIndex::empty());
        }
        let text = fs::read_to_string(&self.path)
            .map_err(|error| Error::io("reading fragile output index", error))?;
        let value = serde_json::from_str::<Value>(&text)
            .map_err(|error| Error::protocol("fragile output index decode", error.to_string()))?;
        Ok(DurableFragileIndex::from_json(&value))
    }

    pub fn write(&self, index: &DurableFragileIndex) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| Error::io("creating fragile output index directory", error))?;
        }
        let temporary_path = self.temporary_path();
        let text = serde_json::to_string_pretty(&index.to_json())
            .map_err(|error| Error::protocol("fragile output index encode", error.to_string()))?;
        fs::write(&temporary_path, text)
            .map_err(|error| Error::io("writing fragile output index", error))?;
        fs::rename(&temporary_path, &self.path)
            .map_err(|error| Error::io("committing fragile output index", error))
    }

    pub fn temporary_path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("output-index.json");
        self.path
            .with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceResolver<'a> {
    index: &'a DurableFragileIndex,
}

impl<'a> ReferenceResolver<'a> {
    pub fn new(index: &'a DurableFragileIndex) -> Self {
        Self { index }
    }

    pub fn resolve_session(
        &self,
        reference: &FragileSessionReference,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<IndexedSession> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let Some(session) = self.index.session(reference) else {
            return Err(factory.missing(Some(RejectedFragileReference::Session(reference.clone()))));
        };
        BackingFileState::new(session.path.clone(), session.fingerprint.clone()).ensure_available(
            &factory,
            Some(RejectedFragileReference::Session(reference.clone())),
        )?;
        Ok(session)
    }

    pub fn resolve_subagent(
        &self,
        reference: &FragileSubagentReference,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<IndexedSubagent> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let Some(subagent) = self.index.subagent(reference) else {
            return Err(
                factory.missing(Some(RejectedFragileReference::Subagent(reference.clone())))
            );
        };
        self.resolve_session(&subagent.session_reference, request_identifier, operation)?;
        Ok(subagent)
    }

    pub fn resolve_output(
        &self,
        reference: &FragileOutputReference,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<IndexedOutput> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let Some(output) = self.index.output(reference) else {
            return Err(factory.missing(Some(RejectedFragileReference::Output(reference.clone()))));
        };
        BackingFileState::new(output.path.clone(), output.fingerprint.clone()).ensure_available(
            &factory,
            Some(RejectedFragileReference::Output(reference.clone())),
        )?;
        Ok(output)
    }

    pub fn resolve_segment(
        &self,
        reference: &FragileOutputSegmentReference,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<IndexedOutputSegment> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let Some(segment) = self.index.segment(reference) else {
            return Err(
                factory.missing(Some(RejectedFragileReference::OutputSegment(
                    reference.clone(),
                ))),
            );
        };
        self.resolve_output(&segment.output_reference, request_identifier, operation)?;
        Ok(segment)
    }

    pub fn resolve_transcript_block(
        &self,
        reference: &FragileTranscriptBlockReference,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<IndexedTranscriptBlock> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let Some(block) = self.index.transcript_block(reference) else {
            return Err(
                factory.missing(Some(RejectedFragileReference::TranscriptBlock(
                    reference.clone(),
                ))),
            );
        };
        BackingFileState::new(block.path.clone(), block.fingerprint.clone()).ensure_available(
            &factory,
            Some(RejectedFragileReference::TranscriptBlock(reference.clone())),
        )?;
        Ok(block)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBackingReader {
    output: IndexedOutput,
    maximum_line_bytes: ByteLimit,
}

impl OutputBackingReader {
    pub fn new(output: IndexedOutput, maximum_line_bytes: ByteLimit) -> Self {
        Self {
            output,
            maximum_line_bytes,
        }
    }

    pub fn read_text(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<String> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let line = BoundedLineReader::new(
            self.output.path.clone(),
            self.output.source_line_number,
            self.maximum_line_bytes.into_u64().max(4096),
        )
        .read_line()
        .map_err(|failure| failure.rejection(&factory, self.output.reference.clone()))?;
        let record = TranscriptLineParser::new(
            self.output.provenance.source,
            self.output.provenance.source_identifier.clone(),
            self.output.path.clone(),
            self.output.source_line_number,
            line,
        )
        .parse()
        .ok_or_else(|| {
            factory.stale(Some(RejectedFragileReference::Output(
                self.output.reference.clone(),
            )))
        })?;
        let hash = StableHash::new(&record.text).hex();
        if hash != self.output.text_hash {
            return Err(factory.stale(Some(RejectedFragileReference::Output(
                self.output.reference.clone(),
            ))));
        }
        Ok(record.text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlockBackingReader {
    block: IndexedTranscriptBlock,
    maximum_line_bytes: ByteLimit,
}

impl TranscriptBlockBackingReader {
    pub fn new(block: IndexedTranscriptBlock, maximum_line_bytes: ByteLimit) -> Self {
        Self {
            block,
            maximum_line_bytes,
        }
    }

    pub fn read_text(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<String> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let reference = Some(RejectedFragileReference::TranscriptBlock(
            self.block.reference.clone(),
        ));
        match self.block.text_availability {
            TranscriptBlockTextAvailability::ReadableText => {}
            TranscriptBlockTextAvailability::UnavailableText => {
                return Err(factory.unsupported_reference(reference));
            }
            TranscriptBlockTextAvailability::EncryptedText => {
                return Err(factory.unauthorized(reference));
            }
        }
        let line_limit = self
            .maximum_line_bytes
            .into_u64()
            .max(self.block.size_byte_count().saturating_add(4096))
            .max(4096);
        let line = BoundedLineReader::new(
            self.block.path.clone(),
            self.block.source_line_number,
            line_limit,
        )
        .read_line()
        .map_err(|failure| {
            failure.transcript_block_rejection(&factory, self.block.reference.clone())
        })?;
        let record = TranscriptLineParser::new(
            self.block.provenance.source,
            self.block.provenance.source_identifier.clone(),
            self.block.path.clone(),
            self.block.source_line_number,
            line,
        )
        .parse()
        .ok_or_else(|| factory.stale(reference.clone()))?;
        let Some(block) = record
            .transcript_blocks()
            .into_iter()
            .find(|candidate| candidate.block_index == self.block.block_index.into_u64())
        else {
            return Err(factory.stale(reference));
        };
        let Some(text) = block.readable_text().map(ToOwned::to_owned) else {
            return Err(factory.stale(reference));
        };
        let hash = StableHash::new(&text).hex();
        if hash != self.block.text_hash || block.kind != self.block.kind {
            return Err(factory.stale(reference));
        }
        Ok(text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedLineReader {
    path: PathBuf,
    line_number: u64,
    maximum_line_bytes: u64,
}

impl BoundedLineReader {
    pub fn new(path: PathBuf, line_number: u64, maximum_line_bytes: u64) -> Self {
        Self {
            path,
            line_number,
            maximum_line_bytes,
        }
    }

    pub fn read_line(&self) -> std::result::Result<String, BoundedLineReadFailure> {
        let mut file = fs::File::open(&self.path).map_err(BoundedLineReadFailure::from_io)?;
        let mut buffer = [0_u8; 8192];
        let mut current_line = 1_u64;
        let mut selected = Vec::new();
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(BoundedLineReadFailure::from_io)?;
            if count == 0 {
                break;
            }
            for byte in &buffer[..count] {
                if current_line == self.line_number {
                    if selected.len() as u64 >= self.maximum_line_bytes {
                        return Err(BoundedLineReadFailure::Oversized);
                    }
                    selected.push(*byte);
                }
                if *byte == b'\n' {
                    if current_line == self.line_number {
                        return String::from_utf8(selected)
                            .map(|text| text.trim_end_matches('\n').to_string())
                            .map_err(|_| BoundedLineReadFailure::Malformed);
                    }
                    current_line += 1;
                }
            }
        }
        if current_line == self.line_number && !selected.is_empty() {
            return String::from_utf8(selected).map_err(|_| BoundedLineReadFailure::Malformed);
        }
        Err(BoundedLineReadFailure::Missing)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedLineReadFailure {
    Missing,
    PermissionDenied,
    IoFailure,
    Malformed,
    Oversized,
}

impl BoundedLineReadFailure {
    pub fn from_io(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => Self::Missing,
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            _ => Self::IoFailure,
        }
    }

    pub fn rejection(
        self,
        factory: &OperationRejectedFactory,
        reference: FragileOutputReference,
    ) -> OperationRejected {
        let reference = Some(RejectedFragileReference::Output(reference));
        match self {
            Self::Missing => factory.broken(reference),
            Self::PermissionDenied => factory.unauthorized(reference),
            Self::IoFailure | Self::Malformed => factory.stale(reference),
            Self::Oversized => factory.oversized(reference),
        }
    }

    pub fn transcript_block_rejection(
        self,
        factory: &OperationRejectedFactory,
        reference: FragileTranscriptBlockReference,
    ) -> OperationRejected {
        let reference = Some(RejectedFragileReference::TranscriptBlock(reference));
        match self {
            Self::Missing => factory.broken(reference),
            Self::PermissionDenied => factory.unauthorized(reference),
            Self::IoFailure | Self::Malformed => factory.stale(reference),
            Self::Oversized => factory.oversized(reference),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLineParser {
    source: SourceKind,
    source_identifier: signal_aggregator::SourceIdentifier,
    path: PathBuf,
    line_number: u64,
    line: String,
}

impl TranscriptLineParser {
    pub fn new(
        source: SourceKind,
        source_identifier: signal_aggregator::SourceIdentifier,
        path: PathBuf,
        line_number: u64,
        line: String,
    ) -> Self {
        Self {
            source,
            source_identifier,
            path,
            line_number,
            line,
        }
    }

    pub fn parse(&self) -> Option<TranscriptRecord> {
        match self.source {
            SourceKind::Claude => match ClaudeJsonlRecord::new(&self.line).into_transcript_record(
                self.path.clone(),
                self.line_number,
                self.source_identifier.clone(),
            ) {
                crate::adapter::claude::ClaudeJsonlRecordResult::Record(record) => Some(record),
                crate::adapter::claude::ClaudeJsonlRecordResult::Malformed => None,
            },
            SourceKind::Codex => match CodexJsonlRecord::new(&self.line).into_transcript_record(
                self.path.clone(),
                self.line_number,
                self.source_identifier.clone(),
            ) {
                crate::adapter::codex::CodexJsonlRecordResult::Record(record) => Some(record),
                crate::adapter::codex::CodexJsonlRecordResult::Malformed => None,
            },
            SourceKind::Pi => match PiJsonlRecord::new(&self.line).into_transcript_record(
                self.path.clone(),
                self.line_number,
                self.source_identifier.clone(),
            ) {
                crate::adapter::pi::PiJsonlRecordResult::Record(record) => Some(record),
                crate::adapter::pi::PiJsonlRecordResult::Malformed => None,
            },
            SourceKind::Repository => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRangeEstimator<'a> {
    index: &'a DurableFragileIndex,
    output: &'a IndexedOutput,
}

impl<'a> OutputRangeEstimator<'a> {
    pub fn new(index: &'a DurableFragileIndex, output: &'a IndexedOutput) -> Self {
        Self { index, output }
    }

    pub fn estimate(
        &self,
        range: &OutputReadRange,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SizeMetadata> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        match range {
            OutputReadRange::EntireOutput => Ok(self.output.size.clone()),
            OutputReadRange::Bytes(range) => {
                ByteRangeSelection::new(self.output.clone(), range.clone())
                    .estimate()
                    .map_err(|_| {
                        factory.invalid_range(Some(RejectedFragileReference::Output(
                            self.output.reference.clone(),
                        )))
                    })
            }
            OutputReadRange::Lines(range) => {
                LineRangeSelection::new(self.output.clone(), range.clone())
                    .estimate()
                    .map_err(|_| {
                        factory.invalid_range(Some(RejectedFragileReference::Output(
                            self.output.reference.clone(),
                        )))
                    })
            }
            OutputReadRange::Segment(reference) => {
                let segment = ReferenceResolver::new(self.index).resolve_segment(
                    reference,
                    request_identifier,
                    operation,
                )?;
                if segment.output_reference != self.output.reference {
                    return Err(factory.invalid_range(Some(
                        RejectedFragileReference::OutputSegment(reference.clone()),
                    )));
                }
                Ok(segment.size)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRangeReader<'a> {
    index: &'a DurableFragileIndex,
    output: IndexedOutput,
    text: String,
}

impl<'a> OutputRangeReader<'a> {
    pub fn new(index: &'a DurableFragileIndex, output: IndexedOutput, text: String) -> Self {
        Self {
            index,
            output,
            text,
        }
    }

    pub fn read(
        &self,
        range: &OutputReadRange,
        maximum_bytes: ByteLimit,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<SelectedOutputText> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        let selected = match range {
            OutputReadRange::EntireOutput => self.text.clone(),
            OutputReadRange::Bytes(range) => ByteRangeTextSelector::new(&self.text, range.clone())
                .select()
                .map_err(|_| {
                    factory.invalid_range(Some(RejectedFragileReference::Output(
                        self.output.reference.clone(),
                    )))
                })?,
            OutputReadRange::Lines(range) => LineRangeTextSelector::new(&self.text, range.clone())
                .select()
                .map_err(|_| {
                    factory.invalid_range(Some(RejectedFragileReference::Output(
                        self.output.reference.clone(),
                    )))
                })?,
            OutputReadRange::Segment(reference) => {
                let segment = ReferenceResolver::new(self.index).resolve_segment(
                    reference,
                    request_identifier,
                    operation,
                )?;
                if segment.output_reference != self.output.reference {
                    return Err(factory.invalid_range(Some(
                        RejectedFragileReference::OutputSegment(reference.clone()),
                    )));
                }
                self.text.clone()
            }
        };
        Ok(SelectedOutputText::new(
            selected,
            self.output.provenance.source,
            self.output.path.clone(),
            maximum_bytes,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedOutputText {
    size: SizeMetadata,
    excerpt: OutputTextExcerpt,
}

impl SelectedOutputText {
    pub fn new(text: String, source: SourceKind, path: PathBuf, maximum_bytes: ByteLimit) -> Self {
        let original_bytes = text.len() as u64;
        let projected = Utf8Prefix::new(&text, maximum_bytes.into_u64()).into_string();
        let projected_bytes = projected.len() as u64;
        let truncation = if projected_bytes < original_bytes {
            Some(Truncation {
                source,
                path: Some(FilesystemPath::new(path.display().to_string())),
                original_bytes: Some(ByteCount::new(original_bytes)),
                projected_bytes: ByteCount::new(projected_bytes),
                reason: TruncationReason::RequestLimit,
            })
        } else {
            None
        };
        Self {
            size: SizeMetadataFactory::from_text(&text, None).exact(),
            excerpt: OutputTextExcerpt {
                text: OutputText::new(projected),
                byte_count: ByteCount::new(projected_bytes),
                truncation,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedTranscriptBlockText {
    size: SizeMetadata,
    excerpt: TranscriptTextExcerpt,
}

impl SelectedTranscriptBlockText {
    pub fn new(text: String, source: SourceKind, path: PathBuf, maximum_bytes: ByteLimit) -> Self {
        let original_bytes = text.len() as u64;
        let projected = Utf8Prefix::new(&text, maximum_bytes.into_u64()).into_string();
        let projected_bytes = projected.len() as u64;
        let truncation = if projected_bytes < original_bytes {
            Some(Truncation {
                source,
                path: Some(FilesystemPath::new(path.display().to_string())),
                original_bytes: Some(ByteCount::new(original_bytes)),
                projected_bytes: ByteCount::new(projected_bytes),
                reason: TruncationReason::RequestLimit,
            })
        } else {
            None
        };
        Self {
            size: SizeMetadataFactory::from_text(&text, None).exact(),
            excerpt: TranscriptTextExcerpt {
                text: TranscriptText::new(projected),
                byte_count: ByteCount::new(projected_bytes),
                truncation,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangeTextSelector<'a> {
    text: &'a str,
    range: ByteRange,
}

impl<'a> ByteRangeTextSelector<'a> {
    pub fn new(text: &'a str, range: ByteRange) -> Self {
        Self { text, range }
    }

    pub fn select(&self) -> std::result::Result<String, RangeSelectionError> {
        let start = self.range.start.into_u64() as usize;
        let end = self.range.end.into_u64() as usize;
        if end < start
            || end > self.text.len()
            || !self.text.is_char_boundary(start)
            || !self.text.is_char_boundary(end)
        {
            return Err(RangeSelectionError);
        }
        Ok(self.text[start..end].to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineRangeTextSelector<'a> {
    text: &'a str,
    range: LineRange,
}

impl<'a> LineRangeTextSelector<'a> {
    pub fn new(text: &'a str, range: LineRange) -> Self {
        Self { text, range }
    }

    pub fn select(&self) -> std::result::Result<String, RangeSelectionError> {
        let start = self.range.start.into_u64();
        let end = self.range.end.into_u64();
        let lines = self.text.lines().collect::<Vec<_>>();
        let maximum_end = lines.len() as u64 + 1;
        if start == 0 || end < start || end > maximum_end {
            return Err(RangeSelectionError);
        }
        let start_index = (start - 1) as usize;
        let end_index = (end - 1) as usize;
        Ok(lines[start_index..end_index].join("\n"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeSelectionError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangeSelection {
    output: IndexedOutput,
    range: ByteRange,
}

impl ByteRangeSelection {
    pub fn new(output: IndexedOutput, range: ByteRange) -> Self {
        Self { output, range }
    }

    pub fn estimate(&self) -> std::result::Result<SizeMetadata, RangeSelectionError> {
        let start = self.range.start.into_u64();
        let end = self.range.end.into_u64();
        let output_bytes = self.output.size.byte_count.map_or(0, ByteCount::into_u64);
        if end < start || end > output_bytes {
            return Err(RangeSelectionError);
        }
        Ok(SizeMetadata {
            byte_count: Some(ByteCount::new(end - start)),
            line_count: None,
            segment_count: None,
            certainty: SizeCertainty::Exact,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineRangeSelection {
    output: IndexedOutput,
    range: LineRange,
}

impl LineRangeSelection {
    pub fn new(output: IndexedOutput, range: LineRange) -> Self {
        Self { output, range }
    }

    pub fn estimate(&self) -> std::result::Result<SizeMetadata, RangeSelectionError> {
        let start = self.range.start.into_u64();
        let end = self.range.end.into_u64();
        let maximum_end = self
            .output
            .size
            .line_count
            .map_or(1, |count| count.into_u64() + 1);
        if start == 0 || end < start || end > maximum_end {
            return Err(RangeSelectionError);
        }
        Ok(SizeMetadata {
            byte_count: None,
            line_count: Some(LineCount::new(end - start)),
            segment_count: None,
            certainty: SizeCertainty::Estimated,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackingFileState {
    path: PathBuf,
    fingerprint: SourceFingerprint,
}

impl BackingFileState {
    pub fn new(path: PathBuf, fingerprint: SourceFingerprint) -> Self {
        Self { path, fingerprint }
    }

    pub fn ensure_available(
        &self,
        factory: &OperationRejectedFactory,
        reference: Option<RejectedFragileReference>,
    ) -> OutputOperationResult<()> {
        match SourceFingerprint::from_path(&self.path) {
            Ok(current) if current == self.fingerprint => Ok(()),
            Ok(_) => Err(factory.stale(reference)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(factory.broken(reference))
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                Err(factory.unauthorized(reference))
            }
            Err(_) => Err(factory.broken(reference)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceFingerprint {
    byte_count: u64,
    modified_seconds: i64,
    modified_nanoseconds: u32,
}

impl SourceFingerprint {
    pub fn from_path(path: &Path) -> std::io::Result<Self> {
        let metadata = fs::metadata(path)?;
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        let duration = modified.duration_since(UNIX_EPOCH).unwrap_or_default();
        Ok(Self {
            byte_count: metadata.len(),
            modified_seconds: duration.as_secs() as i64,
            modified_nanoseconds: duration.subsec_nanos(),
        })
    }

    pub fn missing() -> Self {
        Self {
            byte_count: 0,
            modified_seconds: 0,
            modified_nanoseconds: 0,
        }
    }

    pub fn material(&self) -> String {
        format!(
            "{}:{}:{}",
            self.byte_count, self.modified_seconds, self.modified_nanoseconds
        )
    }

    pub fn to_json(&self) -> Value {
        json!({
            "byte_count": self.byte_count,
            "modified_seconds": self.modified_seconds,
            "modified_nanoseconds": self.modified_nanoseconds,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let reader = JsonReader::new(value);
        Some(Self {
            byte_count: reader.u64("byte_count")?,
            modified_seconds: reader.i64("modified_seconds")?,
            modified_nanoseconds: reader.u64("modified_nanoseconds")? as u32,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewProjector {
    preview_text: String,
    original_bytes: u64,
    source: SourceKind,
    path: PathBuf,
}

impl PreviewProjector {
    pub fn new(
        preview_text: String,
        original_bytes: u64,
        source: SourceKind,
        path: PathBuf,
    ) -> Self {
        Self {
            preview_text,
            original_bytes,
            source,
            path,
        }
    }

    pub fn project(&self, projection: &CardProjection) -> Option<OutputTextExcerpt> {
        match projection {
            CardProjection::MetadataOnly => None,
            CardProjection::BoundedPreview(bound) => Some(self.bounded(bound.maximum_bytes)),
        }
    }

    pub fn bounded(&self, maximum_bytes: ByteLimit) -> OutputTextExcerpt {
        let text = Utf8Prefix::new(&self.preview_text, maximum_bytes.into_u64()).into_string();
        let projected_bytes = text.len() as u64;
        let truncation = if projected_bytes < self.original_bytes {
            Some(Truncation {
                source: self.source,
                path: Some(FilesystemPath::new(self.path.display().to_string())),
                original_bytes: Some(ByteCount::new(self.original_bytes)),
                projected_bytes: ByteCount::new(projected_bytes),
                reason: TruncationReason::ProjectionLimit,
            })
        } else {
            None
        };
        OutputTextExcerpt {
            text: OutputText::new(text),
            byte_count: ByteCount::new(projected_bytes),
            truncation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlockPreviewProjector {
    preview_text: String,
    original_bytes: u64,
    source: SourceKind,
    path: PathBuf,
    availability: TranscriptBlockTextAvailability,
}

impl TranscriptBlockPreviewProjector {
    pub fn new(
        preview_text: String,
        original_bytes: u64,
        source: SourceKind,
        path: PathBuf,
        availability: TranscriptBlockTextAvailability,
    ) -> Self {
        Self {
            preview_text,
            original_bytes,
            source,
            path,
            availability,
        }
    }

    pub fn project(&self, projection: &CardProjection) -> Option<TranscriptTextExcerpt> {
        if self.availability != TranscriptBlockTextAvailability::ReadableText {
            return None;
        }
        match projection {
            CardProjection::MetadataOnly => None,
            CardProjection::BoundedPreview(bound) => Some(self.bounded(bound.maximum_bytes)),
        }
    }

    pub fn bounded(&self, maximum_bytes: ByteLimit) -> TranscriptTextExcerpt {
        let text = Utf8Prefix::new(&self.preview_text, maximum_bytes.into_u64()).into_string();
        let projected_bytes = text.len() as u64;
        let truncation = if projected_bytes < self.original_bytes {
            Some(Truncation {
                source: self.source,
                path: Some(FilesystemPath::new(self.path.display().to_string())),
                original_bytes: Some(ByteCount::new(self.original_bytes)),
                projected_bytes: ByteCount::new(projected_bytes),
                reason: TruncationReason::ProjectionLimit,
            })
        } else {
            None
        };
        TranscriptTextExcerpt {
            text: TranscriptText::new(text),
            byte_count: ByteCount::new(projected_bytes),
            truncation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Utf8Prefix<'a> {
    text: &'a str,
    maximum_bytes: u64,
}

impl<'a> Utf8Prefix<'a> {
    pub fn new(text: &'a str, maximum_bytes: u64) -> Self {
        Self {
            text,
            maximum_bytes,
        }
    }

    pub fn into_string(self) -> String {
        let maximum_bytes = self.maximum_bytes as usize;
        if self.text.len() <= maximum_bytes {
            return self.text.to_string();
        }
        let mut boundary = maximum_bytes;
        while boundary > 0 && !self.text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.text[..boundary].to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequestValidator {
    request_identifier: RequestIdentifier,
    operation: OperationKind,
    maximum_page_items: PageLimit,
}

impl PageRequestValidator {
    pub fn new(
        request_identifier: RequestIdentifier,
        operation: OperationKind,
        maximum_page_items: PageLimit,
    ) -> Self {
        Self {
            request_identifier,
            operation,
            maximum_page_items,
        }
    }

    pub fn validate(&self, page: &PageRequest) -> OutputOperationResult<()> {
        let factory =
            OperationRejectedFactory::new(self.request_identifier.clone(), self.operation);
        if page.limit.into_u64() == 0 {
            return Err(factory.invalid_request());
        }
        if page.limit.into_u64() > self.maximum_page_items.into_u64() {
            return Err(factory.oversized(None));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRequestValidator {
    request_identifier: RequestIdentifier,
    operation: OperationKind,
    maximum_preview_bytes: ByteLimit,
}

impl ProjectionRequestValidator {
    pub fn new(
        request_identifier: RequestIdentifier,
        operation: OperationKind,
        maximum_preview_bytes: ByteLimit,
    ) -> Self {
        Self {
            request_identifier,
            operation,
            maximum_preview_bytes,
        }
    }

    pub fn validate(&self, projection: &CardProjection) -> OutputOperationResult<()> {
        let factory =
            OperationRejectedFactory::new(self.request_identifier.clone(), self.operation);
        if let CardProjection::BoundedPreview(bound) = projection {
            if bound.maximum_bytes.into_u64() == 0 {
                return Err(factory.invalid_request());
            }
            if bound.maximum_bytes.into_u64() > self.maximum_preview_bytes.into_u64() {
                return Err(factory.oversized(None));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadLimitValidator {
    request_identifier: RequestIdentifier,
    operation: OperationKind,
    maximum_read_bytes: ByteLimit,
}

impl ReadLimitValidator {
    pub fn new(
        request_identifier: RequestIdentifier,
        operation: OperationKind,
        maximum_read_bytes: ByteLimit,
    ) -> Self {
        Self {
            request_identifier,
            operation,
            maximum_read_bytes,
        }
    }

    pub fn validate(&self, requested: ByteLimit) -> OutputOperationResult<()> {
        let factory =
            OperationRejectedFactory::new(self.request_identifier.clone(), self.operation);
        if requested.into_u64() == 0 {
            return Err(factory.invalid_request());
        }
        if requested.into_u64() > self.maximum_read_bytes.into_u64() {
            return Err(factory.oversized(None));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockRequestValidator {
    maximum_page_items: PageLimit,
    maximum_preview_bytes: ByteLimit,
}

impl TranscriptBlockRequestValidator {
    pub fn new(maximum_page_items: PageLimit, maximum_preview_bytes: ByteLimit) -> Self {
        Self {
            maximum_page_items,
            maximum_preview_bytes,
        }
    }

    pub fn validate_listing(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
        page: &PageRequest,
        projection: &CardProjection,
    ) -> OutputOperationResult<()> {
        PageRequestValidator::new(
            request_identifier.clone(),
            operation,
            self.maximum_page_items,
        )
        .validate(page)?;
        ProjectionRequestValidator::new(
            request_identifier.clone(),
            operation,
            self.maximum_preview_bytes,
        )
        .validate(projection)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockQueryValidator<'a> {
    query: &'a TranscriptBlockTextQuery,
}

impl<'a> TranscriptBlockQueryValidator<'a> {
    pub fn new(query: &'a TranscriptBlockTextQuery) -> Self {
        Self { query }
    }

    pub fn validate(
        &self,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        let factory = OperationRejectedFactory::new(request_identifier.clone(), operation);
        if QueryComplexity::new(self.query.as_query()).is_pathological() {
            Err(factory.invalid_query())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryComplexity<'a> {
    query: &'a Query,
}

impl<'a> QueryComplexity<'a> {
    pub fn new(query: &'a Query) -> Self {
        Self { query }
    }

    pub fn is_pathological(&self) -> bool {
        self.node_count() > 64 || self.depth() > 16 || self.has_empty_or_excessive_term()
    }

    pub fn node_count(&self) -> usize {
        QueryShape::new(self.query).node_count()
    }

    pub fn depth(&self) -> usize {
        QueryShape::new(self.query).depth()
    }

    pub fn has_empty_or_excessive_term(&self) -> bool {
        QueryShape::new(self.query).has_empty_or_excessive_term()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryShape<'a> {
    query: &'a Query,
}

impl<'a> QueryShape<'a> {
    pub fn new(query: &'a Query) -> Self {
        Self { query }
    }

    pub fn node_count(&self) -> usize {
        match self.query {
            Query::Contains(_) | Query::Near(_) => 1,
            Query::Not(child) => 1 + QueryShape::new(child).node_count(),
            Query::AllOf(children) | Query::AnyOf(children) => {
                1 + children
                    .iter()
                    .map(|child| QueryShape::new(child).node_count())
                    .sum::<usize>()
            }
        }
    }

    pub fn depth(&self) -> usize {
        match self.query {
            Query::Contains(_) | Query::Near(_) => 1,
            Query::Not(child) => 1 + QueryShape::new(child).depth(),
            Query::AllOf(children) | Query::AnyOf(children) => {
                1 + children
                    .iter()
                    .map(|child| QueryShape::new(child).depth())
                    .max()
                    .unwrap_or(0)
            }
        }
    }

    pub fn has_empty_or_excessive_term(&self) -> bool {
        match self.query {
            Query::Contains(term) => QueryTermShape::new(term).is_invalid(),
            Query::Near(query) => {
                query.distance.0 > 10_000
                    || QueryTermShape::new(&query.left).is_invalid()
                    || QueryTermShape::new(&query.right).is_invalid()
            }
            Query::Not(child) => QueryShape::new(child).has_empty_or_excessive_term(),
            Query::AllOf(children) | Query::AnyOf(children) => {
                children.is_empty()
                    || children
                        .iter()
                        .any(|child| QueryShape::new(child).has_empty_or_excessive_term())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTermShape<'a> {
    term: &'a QueryTerm,
}

impl<'a> QueryTermShape<'a> {
    pub fn new(term: &'a QueryTerm) -> Self {
        Self { term }
    }

    pub fn is_invalid(&self) -> bool {
        match self.term {
            QueryTerm::Word(word) => {
                word.value.len() > 256 || word.normalized().as_str().is_empty()
            }
            QueryTerm::Phrase(phrase) => {
                phrase.words.len() > 32
                    || phrase.words.iter().map(String::len).sum::<usize>() > 2048
                    || phrase.normalized_words().is_empty()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationQueryShape {
    material: String,
}

impl PaginationQueryShape {
    pub fn sessions(filter: &SessionListFilter, lowered_time_window: Option<&TimeWindow>) -> Self {
        Self {
            material: StableSignatureMaterial::new("sessions-query")
                .field(
                    "source_selection",
                    SourceSelectionSignature::new(&filter.source_selection).material(),
                )
                .field(
                    "time_window",
                    OptionalTimeWindowSignature::new(lowered_time_window).material(),
                )
                .finish(),
        }
    }

    pub fn subagents(filter: &SubagentListFilter) -> Self {
        Self {
            material: StableSignatureMaterial::new("subagents-query")
                .field("session_reference", filter.session_reference.as_str())
                .field(
                    "authored_status",
                    AuthoredStatusFilterSignature::new(&filter.authored_status).material(),
                )
                .finish(),
        }
    }

    pub fn outputs(filter: &OutputListFilter, lowered_time_window: Option<&TimeWindow>) -> Self {
        Self {
            material: StableSignatureMaterial::new("outputs-query")
                .field(
                    "source_selection",
                    SourceSelectionSignature::new(&filter.source_selection).material(),
                )
                .field(
                    "session_reference",
                    OptionalSignatureText::new(
                        filter
                            .session_reference
                            .as_ref()
                            .map(|reference| reference.as_str()),
                    )
                    .material(),
                )
                .field(
                    "subagent_reference",
                    OptionalSignatureText::new(
                        filter
                            .subagent_reference
                            .as_ref()
                            .map(|reference| reference.as_str()),
                    )
                    .material(),
                )
                .field(
                    "authored_status",
                    AuthoredStatusFilterSignature::new(&filter.authored_status).material(),
                )
                .field(
                    "time_window",
                    OptionalTimeWindowSignature::new(lowered_time_window).material(),
                )
                .finish(),
        }
    }

    pub fn segments(filter: &OutputSegmentListFilter) -> Self {
        Self {
            material: StableSignatureMaterial::new("segments-query")
                .field("output_reference", filter.output_reference.as_str())
                .finish(),
        }
    }

    pub fn transcript_blocks(
        filter: &TranscriptBlockFilter,
        lowered_time_window: Option<&TimeWindow>,
    ) -> Self {
        Self {
            material: StableSignatureMaterial::new("transcript-blocks-query")
                .field(
                    "filter",
                    TranscriptBlockFilterSignature::new(filter, lowered_time_window).material(),
                )
                .finish(),
        }
    }

    pub fn transcript_block_search(
        filter: &TranscriptBlockFilter,
        lowered_time_window: Option<&TimeWindow>,
        query: &TranscriptBlockTextQuery,
    ) -> Self {
        Self {
            material: StableSignatureMaterial::new("transcript-block-search-query")
                .field(
                    "filter",
                    TranscriptBlockFilterSignature::new(filter, lowered_time_window).material(),
                )
                .field(
                    "text_query",
                    TextQuerySignature::new(query.as_query()).material(),
                )
                .finish(),
        }
    }

    pub fn material(&self) -> &str {
        &self.material
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSelectionSignature<'a> {
    selection: &'a SourceSelection,
}

impl<'a> SourceSelectionSignature<'a> {
    pub fn new(selection: &'a SourceSelection) -> Self {
        Self { selection }
    }

    pub fn material(&self) -> String {
        match self.selection {
            SourceSelection::AllConfigured => StableSignatureMaterial::new("source-selection")
                .field("kind", "all-configured")
                .finish(),
            SourceSelection::Only(selected) => {
                let sources = selected
                    .sources
                    .iter()
                    .map(|source| SourceKindName::new(*source).as_str().to_string())
                    .collect::<BTreeSet<_>>();
                let mut material = StableSignatureMaterial::new("source-selection")
                    .field("kind", "only")
                    .field("source_count", sources.len().to_string());
                for source in sources {
                    material = material.field("source", source);
                }
                material.finish()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredStatusFilterSignature<'a> {
    filter: &'a AuthoredStatusFilter,
}

impl<'a> AuthoredStatusFilterSignature<'a> {
    pub fn new(filter: &'a AuthoredStatusFilter) -> Self {
        Self { filter }
    }

    pub fn material(&self) -> String {
        match self.filter {
            AuthoredStatusFilter::AnyAuthoredStatus => {
                StableSignatureMaterial::new("authored-status-filter")
                    .field("kind", "any")
                    .finish()
            }
            AuthoredStatusFilter::OnlyAuthoredStatus(status) => {
                StableSignatureMaterial::new("authored-status-filter")
                    .field("kind", "only")
                    .field("status", AuthoredStatusName::new(*status).as_str())
                    .finish()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockFilterSignature<'a> {
    filter: &'a TranscriptBlockFilter,
    lowered_time_window: Option<&'a TimeWindow>,
}

impl<'a> TranscriptBlockFilterSignature<'a> {
    pub fn new(
        filter: &'a TranscriptBlockFilter,
        lowered_time_window: Option<&'a TimeWindow>,
    ) -> Self {
        Self {
            filter,
            lowered_time_window,
        }
    }

    pub fn material(&self) -> String {
        StableSignatureMaterial::new("transcript-block-filter")
            .field(
                "source_selection",
                SourceSelectionSignature::new(&self.filter.source_selection).material(),
            )
            .field(
                "session_reference",
                OptionalSignatureText::new(
                    self.filter
                        .session_reference
                        .as_ref()
                        .map(|reference| reference.as_str()),
                )
                .material(),
            )
            .field(
                "subagent_reference",
                OptionalSignatureText::new(
                    self.filter
                        .subagent_reference
                        .as_ref()
                        .map(|reference| reference.as_str()),
                )
                .material(),
            )
            .field(
                "kind_selection",
                TranscriptBlockKindSelectionSignature::new(&self.filter.kind_selection).material(),
            )
            .field(
                "authored_status",
                AuthoredStatusFilterSignature::new(&self.filter.authored_status).material(),
            )
            .field(
                "time_window",
                OptionalTimeWindowSignature::new(self.lowered_time_window).material(),
            )
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockKindSelectionSignature<'a> {
    selection: &'a TranscriptBlockKindSelection,
}

impl<'a> TranscriptBlockKindSelectionSignature<'a> {
    pub fn new(selection: &'a TranscriptBlockKindSelection) -> Self {
        Self { selection }
    }

    pub fn material(&self) -> String {
        match self.selection {
            TranscriptBlockKindSelection::AllTranscriptBlockKinds => {
                StableSignatureMaterial::new("transcript-block-kind-selection")
                    .field("kind", "all")
                    .finish()
            }
            TranscriptBlockKindSelection::OnlyTranscriptBlockKinds(selected) => {
                let kinds = selected
                    .kinds
                    .iter()
                    .map(|kind| TranscriptBlockKindName::new(*kind).as_str().to_string())
                    .collect::<BTreeSet<_>>();
                let mut material = StableSignatureMaterial::new("transcript-block-kind-selection")
                    .field("kind", "only")
                    .field("kind_count", kinds.len().to_string());
                for kind in kinds {
                    material = material.field("selected_kind", kind);
                }
                material.finish()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextQuerySignature<'a> {
    query: &'a Query,
}

impl<'a> TextQuerySignature<'a> {
    pub fn new(query: &'a Query) -> Self {
        Self { query }
    }

    pub fn material(&self) -> String {
        match self.query {
            Query::Contains(term) => StableSignatureMaterial::new("text-query")
                .field("kind", "contains")
                .field("term", QueryTermSignature::new(term).material())
                .finish(),
            Query::AllOf(children) => self.children_material("all-of", children),
            Query::AnyOf(children) => self.children_material("any-of", children),
            Query::Not(child) => StableSignatureMaterial::new("text-query")
                .field("kind", "not")
                .field("child", TextQuerySignature::new(child).material())
                .finish(),
            Query::Near(query) => StableSignatureMaterial::new("text-query")
                .field("kind", "near")
                .field("left", QueryTermSignature::new(&query.left).material())
                .field("right", QueryTermSignature::new(&query.right).material())
                .field("distance", query.distance.0.to_string())
                .finish(),
        }
    }

    pub fn children_material(&self, kind: &'static str, children: &[Query]) -> String {
        let mut material = StableSignatureMaterial::new("text-query")
            .field("kind", kind)
            .field("child_count", children.len().to_string());
        for child in children {
            material = material.field("child", TextQuerySignature::new(child).material());
        }
        material.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTermSignature<'a> {
    term: &'a QueryTerm,
}

impl<'a> QueryTermSignature<'a> {
    pub fn new(term: &'a QueryTerm) -> Self {
        Self { term }
    }

    pub fn material(&self) -> String {
        match self.term {
            QueryTerm::Word(word) => StableSignatureMaterial::new("text-query-term")
                .field("kind", "word")
                .field("value", &word.value)
                .finish(),
            QueryTerm::Phrase(phrase) => {
                let mut material = StableSignatureMaterial::new("text-query-term")
                    .field("kind", "phrase")
                    .field("word_count", phrase.words.len().to_string());
                for word in &phrase.words {
                    material = material.field("word", word);
                }
                material.finish()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionalTimeWindowSignature<'a> {
    time_window: Option<&'a TimeWindow>,
}

impl<'a> OptionalTimeWindowSignature<'a> {
    pub fn new(time_window: Option<&'a TimeWindow>) -> Self {
        Self { time_window }
    }

    pub fn material(&self) -> String {
        match self.time_window {
            Some(time_window) => TimeWindowSignature::new(time_window).material(),
            None => StableSignatureMaterial::new("time-window")
                .field("kind", "none")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindowSignature<'a> {
    time_window: &'a TimeWindow,
}

impl<'a> TimeWindowSignature<'a> {
    pub fn new(time_window: &'a TimeWindow) -> Self {
        Self { time_window }
    }

    pub fn material(&self) -> String {
        match self.time_window {
            TimeWindow::Recent(duration) => StableSignatureMaterial::new("time-window")
                .field("kind", "recent")
                .field("amount", duration.amount.into_u64().to_string())
                .field("unit", DurationUnitName::new(duration.unit).as_str())
                .finish(),
            TimeWindow::Range(range) => StableSignatureMaterial::new("time-window")
                .field("kind", "range")
                .field("start", range.start.as_str())
                .field("end", range.end.as_str())
                .finish(),
            TimeWindow::Since(timestamp) => StableSignatureMaterial::new("time-window")
                .field("kind", "since")
                .field("start", timestamp.as_str())
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionalSignatureText<'a> {
    value: Option<&'a str>,
}

impl<'a> OptionalSignatureText<'a> {
    pub fn new(value: Option<&'a str>) -> Self {
        Self { value }
    }

    pub fn material(&self) -> String {
        match self.value {
            Some(value) => StableSignatureMaterial::new("optional-text")
                .field("kind", "some")
                .field("value", value)
                .finish(),
            None => StableSignatureMaterial::new("optional-text")
                .field("kind", "none")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableSignatureMaterial {
    material: String,
}

impl StableSignatureMaterial {
    pub fn new(label: &'static str) -> Self {
        Self {
            material: String::new(),
        }
        .field("label", label)
    }

    pub fn field(mut self, name: &'static str, value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        self.material.push_str(name);
        self.material.push('=');
        self.material.push_str(&value.len().to_string());
        self.material.push(':');
        self.material.push_str(value);
        self.material.push(';');
        self
    }

    pub fn finish(self) -> String {
        self.material
    }
}

pub trait PaginatedItemReference {
    fn pagination_reference(&self) -> &str;
}

impl PaginatedItemReference for IndexedSession {
    fn pagination_reference(&self) -> &str {
        self.reference.as_str()
    }
}

impl PaginatedItemReference for IndexedSubagent {
    fn pagination_reference(&self) -> &str {
        self.reference.as_str()
    }
}

impl PaginatedItemReference for IndexedOutput {
    fn pagination_reference(&self) -> &str {
        self.reference.as_str()
    }
}

impl PaginatedItemReference for IndexedOutputSegment {
    fn pagination_reference(&self) -> &str {
        self.reference.as_str()
    }
}

impl PaginatedItemReference for IndexedTranscriptBlock {
    fn pagination_reference(&self) -> &str {
        self.reference.as_str()
    }
}

impl PaginatedItemReference for IndexedTranscriptBlockSearchMatch {
    fn pagination_reference(&self) -> &str {
        self.block.reference.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationCursorBinding {
    collection: PageCollectionKind,
    order: ListingOrder,
    limit: PageLimit,
    query: PaginationQueryShape,
}

impl PaginationCursorBinding {
    pub fn new(
        collection: PageCollectionKind,
        page: &PageRequest,
        query: PaginationQueryShape,
    ) -> Self {
        Self {
            collection,
            order: page.order,
            limit: page.limit,
            query,
        }
    }

    pub fn signature<T: PaginatedItemReference>(&self, items: &[T]) -> String {
        let mut material = StableSignatureMaterial::new("pagination-cursor-binding")
            .field("signature_version", "2")
            .field("collection", self.collection.as_str())
            .field("order", ListingOrderName::new(self.order).as_str())
            .field("limit", self.limit.into_u64().to_string())
            .field("query", self.query.material())
            .field("item_count", items.len().to_string());
        for item in items {
            material = material.field("item_reference", item.pagination_reference());
        }
        StableHash::new(material.finish()).hex()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginatedItems<T> {
    items: Vec<T>,
    metadata: PageMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationWindow<T> {
    request_identifier: RequestIdentifier,
    operation: OperationKind,
    collection: PageCollectionKind,
    page: PageRequest,
    binding: PaginationCursorBinding,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Clone + PaginatedItemReference> PaginationWindow<T> {
    pub fn new(
        request_identifier: RequestIdentifier,
        operation: OperationKind,
        collection: PageCollectionKind,
        page: PageRequest,
        query: PaginationQueryShape,
    ) -> Self {
        let binding = PaginationCursorBinding::new(collection, &page, query);
        Self {
            request_identifier,
            operation,
            collection,
            page,
            binding,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn select(&self, items: &[T]) -> OutputOperationResult<PaginatedItems<T>> {
        let signature = self.binding.signature(items);
        let offset = self.cursor_offset(items.len(), &signature)?;
        let limit = self.page.limit.into_u64() as usize;
        let selected = items
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        let next_offset = offset + selected.len();
        let next_cursor = if next_offset < items.len() {
            Some(
                PageCursor::new(self.collection, self.page.order, next_offset, signature)
                    .to_reference(),
            )
        } else {
            None
        };
        Ok(PaginatedItems {
            items: selected.clone(),
            metadata: PageMetadata {
                limit: self.page.limit,
                returned_items: ItemCount::new(selected.len() as u64),
                total_items: Some(ItemCount::new(items.len() as u64)),
                next_cursor,
                order: self.page.order,
            },
        })
    }

    pub fn cursor_offset(
        &self,
        item_count: usize,
        signature: &str,
    ) -> OutputOperationResult<usize> {
        let Some(cursor) = &self.page.cursor else {
            return Ok(0);
        };
        let factory =
            OperationRejectedFactory::new(self.request_identifier.clone(), self.operation);
        let parsed = PageCursor::parse(cursor).ok_or_else(|| {
            factory.stale(Some(RejectedFragileReference::PageCursor(cursor.clone())))
        })?;
        if parsed.collection != self.collection
            || parsed.order != self.page.order
            || parsed.signature != signature
            || parsed.offset > item_count
        {
            return Err(factory.stale(Some(RejectedFragileReference::PageCursor(cursor.clone()))));
        }
        Ok(parsed.offset)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCollectionKind {
    Sessions,
    Subagents,
    Outputs,
    Segments,
    TranscriptBlocks,
}

impl PageCollectionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Subagents => "subagents",
            Self::Outputs => "outputs",
            Self::Segments => "segments",
            Self::TranscriptBlocks => "transcript-blocks",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sessions" => Some(Self::Sessions),
            "subagents" => Some(Self::Subagents),
            "outputs" => Some(Self::Outputs),
            "segments" => Some(Self::Segments),
            "transcript-blocks" => Some(Self::TranscriptBlocks),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCursor {
    collection: PageCollectionKind,
    order: ListingOrder,
    offset: usize,
    signature: String,
}

impl PageCursor {
    pub fn new(
        collection: PageCollectionKind,
        order: ListingOrder,
        offset: usize,
        signature: String,
    ) -> Self {
        Self {
            collection,
            order,
            offset,
            signature,
        }
    }

    pub fn to_reference(&self) -> FragilePageCursor {
        FragilePageCursor::new(format!(
            "cursor:v2:{}:{}:{}:{}",
            self.collection.as_str(),
            ListingOrderName::new(self.order).as_str(),
            self.offset,
            self.signature
        ))
    }

    pub fn parse(reference: &FragilePageCursor) -> Option<Self> {
        let parts = reference.as_str().split(':').collect::<Vec<_>>();
        if parts.len() != 6 || parts[0] != "cursor" || parts[1] != "v2" {
            return None;
        }
        Some(Self {
            collection: PageCollectionKind::parse(parts[2])?,
            order: ListingOrderName::parse(parts[3])?,
            offset: parts[4].parse().ok()?,
            signature: parts[5].to_string(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListingOrderName {
    order: ListingOrder,
}

impl ListingOrderName {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn as_str(&self) -> &'static str {
        match self.order {
            ListingOrder::OldestFirst => "oldest",
            ListingOrder::NewestFirst => "newest",
            ListingOrder::ReferenceAscending => "reference",
        }
    }

    pub fn parse(value: &str) -> Option<ListingOrder> {
        match value {
            "oldest" => Some(ListingOrder::OldestFirst),
            "newest" => Some(ListingOrder::NewestFirst),
            "reference" => Some(ListingOrder::ReferenceAscending),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationUnitName {
    unit: DurationUnit,
}

impl DurationUnitName {
    pub fn new(unit: DurationUnit) -> Self {
        Self { unit }
    }

    pub fn as_str(&self) -> &'static str {
        match self.unit {
            DurationUnit::Minutes => "minutes",
            DurationUnit::Hours => "hours",
            DurationUnit::Days => "days",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSessionSorter {
    order: ListingOrder,
}

impl IndexedSessionSorter {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn sort(&self, sessions: &mut [IndexedSession]) {
        sessions.sort_by(|left, right| {
            ChronologyOrdering::new(self.order).compare(
                left.chronology_timestamp(),
                left.reference.as_str(),
                right.chronology_timestamp(),
                right.reference.as_str(),
            )
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSubagentSorter {
    order: ListingOrder,
}

impl IndexedSubagentSorter {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn sort(&self, subagents: &mut [IndexedSubagent]) {
        subagents.sort_by(|left, right| {
            ChronologyOrdering::new(self.order).compare(
                left.chronology_timestamp(),
                left.reference.as_str(),
                right.chronology_timestamp(),
                right.reference.as_str(),
            )
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedOutputSorter {
    order: ListingOrder,
}

impl IndexedOutputSorter {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn sort(&self, outputs: &mut [IndexedOutput]) {
        outputs.sort_by(|left, right| {
            ChronologyOrdering::new(self.order).compare(
                left.chronology_timestamp(),
                left.reference.as_str(),
                right.chronology_timestamp(),
                right.reference.as_str(),
            )
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedSegmentSorter {
    order: ListingOrder,
}

impl IndexedSegmentSorter {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn sort(&self, segments: &mut [IndexedOutputSegment]) {
        segments.sort_by(|left, right| match self.order {
            ListingOrder::ReferenceAscending => {
                left.reference.as_str().cmp(right.reference.as_str())
            }
            ListingOrder::OldestFirst => left
                .segment_index
                .into_u64()
                .cmp(&right.segment_index.into_u64())
                .then_with(|| left.reference.as_str().cmp(right.reference.as_str())),
            ListingOrder::NewestFirst => right
                .segment_index
                .into_u64()
                .cmp(&left.segment_index.into_u64())
                .then_with(|| left.reference.as_str().cmp(right.reference.as_str())),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedTranscriptBlockSorter {
    order: ListingOrder,
}

impl IndexedTranscriptBlockSorter {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn sort(&self, blocks: &mut [IndexedTranscriptBlock]) {
        blocks.sort_by(|left, right| TranscriptBlockOrdering::new(self.order).compare(left, right));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockOrdering {
    order: ListingOrder,
}

impl TranscriptBlockOrdering {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn compare(
        &self,
        left: &IndexedTranscriptBlock,
        right: &IndexedTranscriptBlock,
    ) -> Ordering {
        match self.order {
            ListingOrder::ReferenceAscending => {
                left.reference.as_str().cmp(right.reference.as_str())
            }
            ListingOrder::OldestFirst => self
                .compare_oldest(left.chronology_timestamp(), right.chronology_timestamp())
                .then_with(|| {
                    left.source_sort_material()
                        .cmp(&right.source_sort_material())
                })
                .then_with(|| left.source_line_number.cmp(&right.source_line_number))
                .then_with(|| {
                    left.block_index
                        .into_u64()
                        .cmp(&right.block_index.into_u64())
                })
                .then_with(|| left.reference.as_str().cmp(right.reference.as_str())),
            ListingOrder::NewestFirst => self
                .compare_newest(left.chronology_timestamp(), right.chronology_timestamp())
                .then_with(|| {
                    left.source_sort_material()
                        .cmp(&right.source_sort_material())
                })
                .then_with(|| left.source_line_number.cmp(&right.source_line_number))
                .then_with(|| {
                    left.block_index
                        .into_u64()
                        .cmp(&right.block_index.into_u64())
                })
                .then_with(|| left.reference.as_str().cmp(right.reference.as_str())),
        }
    }

    pub fn compare_oldest(&self, left: Option<&Timestamp>, right: Option<&Timestamp>) -> Ordering {
        ChronologyOrdering::new(ListingOrder::OldestFirst).compare_oldest(left, right)
    }

    pub fn compare_newest(&self, left: Option<&Timestamp>, right: Option<&Timestamp>) -> Ordering {
        ChronologyOrdering::new(ListingOrder::NewestFirst).compare_newest(left, right)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChronologyOrdering {
    order: ListingOrder,
}

impl ChronologyOrdering {
    pub fn new(order: ListingOrder) -> Self {
        Self { order }
    }

    pub fn compare(
        &self,
        left_timestamp: Option<&Timestamp>,
        left_reference: &str,
        right_timestamp: Option<&Timestamp>,
        right_reference: &str,
    ) -> Ordering {
        match self.order {
            ListingOrder::ReferenceAscending => left_reference.cmp(right_reference),
            ListingOrder::OldestFirst => self
                .compare_oldest(left_timestamp, right_timestamp)
                .then_with(|| left_reference.cmp(right_reference)),
            ListingOrder::NewestFirst => self
                .compare_newest(left_timestamp, right_timestamp)
                .then_with(|| left_reference.cmp(right_reference)),
        }
    }

    pub fn compare_oldest(&self, left: Option<&Timestamp>, right: Option<&Timestamp>) -> Ordering {
        match (left, right) {
            (Some(left), Some(right)) => left.as_str().cmp(right.as_str()),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    pub fn compare_newest(&self, left: Option<&Timestamp>, right: Option<&Timestamp>) -> Ordering {
        match (left, right) {
            (Some(left), Some(right)) => right.as_str().cmp(left.as_str()),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSelectionFilter<'a> {
    selection: &'a SourceSelection,
}

impl<'a> SourceSelectionFilter<'a> {
    pub fn new(selection: &'a SourceSelection) -> Self {
        Self { selection }
    }

    pub fn accepts(&self, source: SourceKind) -> bool {
        match self.selection {
            SourceSelection::AllConfigured => true,
            SourceSelection::Only(selected) => selected.sources.contains(&source),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptionalTimeWindowFilter<'a> {
    time_window: Option<&'a TimeWindow>,
}

impl<'a> OptionalTimeWindowFilter<'a> {
    pub fn new(time_window: Option<&'a TimeWindow>) -> Self {
        Self { time_window }
    }

    pub fn accepts(&self, timestamp: Option<&Timestamp>) -> bool {
        match self.time_window {
            None => true,
            Some(time_window) => matches!(
                TimeWindowMatcher::new(time_window.clone()).accepts(timestamp),
                TimeWindowAcceptance::Accepted
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredStatusFilterMatcher<'a> {
    filter: &'a AuthoredStatusFilter,
}

impl<'a> AuthoredStatusFilterMatcher<'a> {
    pub fn new(filter: &'a AuthoredStatusFilter) -> Self {
        Self { filter }
    }

    pub fn accepts(&self, status: AuthoredStatus) -> bool {
        match self.filter {
            AuthoredStatusFilter::AnyAuthoredStatus => true,
            AuthoredStatusFilter::OnlyAuthoredStatus(expected) => *expected == status,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockKindSelectionMatcher<'a> {
    selection: &'a TranscriptBlockKindSelection,
}

impl<'a> TranscriptBlockKindSelectionMatcher<'a> {
    pub fn new(selection: &'a TranscriptBlockKindSelection) -> Self {
        Self { selection }
    }

    pub fn accepts(&self, kind: TranscriptBlockKind) -> bool {
        match self.selection {
            TranscriptBlockKindSelection::AllTranscriptBlockKinds => true,
            TranscriptBlockKindSelection::OnlyTranscriptBlockKinds(selected) => {
                selected.kinds.contains(&kind)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockFilterMatcher<'a> {
    filter: &'a TranscriptBlockFilter,
    lowered_time_window: Option<&'a TimeWindow>,
}

impl<'a> TranscriptBlockFilterMatcher<'a> {
    pub fn new(
        filter: &'a TranscriptBlockFilter,
        lowered_time_window: Option<&'a TimeWindow>,
    ) -> Self {
        Self {
            filter,
            lowered_time_window,
        }
    }

    pub fn matching_blocks(
        &self,
        blocks: Vec<IndexedTranscriptBlock>,
    ) -> Vec<IndexedTranscriptBlock> {
        blocks
            .into_iter()
            .filter(|block| {
                SourceSelectionFilter::new(&self.filter.source_selection)
                    .accepts(block.provenance.source)
            })
            .filter(|block| {
                self.filter
                    .session_reference
                    .as_ref()
                    .is_none_or(|reference| block.session_reference == *reference)
            })
            .filter(|block| {
                self.filter
                    .subagent_reference
                    .as_ref()
                    .is_none_or(|reference| block.subagent_reference.as_ref() == Some(reference))
            })
            .filter(|block| {
                TranscriptBlockKindSelectionMatcher::new(&self.filter.kind_selection)
                    .accepts(block.kind)
            })
            .filter(|block| {
                AuthoredStatusFilterMatcher::new(&self.filter.authored_status)
                    .accepts(block.provenance.authored_status)
            })
            .filter(|block| {
                OptionalTimeWindowFilter::new(self.lowered_time_window)
                    .accepts(block.provenance.observed_at.as_ref())
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlockReferenceFilterResolver<'a> {
    index: &'a DurableFragileIndex,
}

impl<'a> TranscriptBlockReferenceFilterResolver<'a> {
    pub fn new(index: &'a DurableFragileIndex) -> Self {
        Self { index }
    }

    pub fn resolve_filter_references(
        &self,
        filter: &TranscriptBlockFilter,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<()> {
        if let Some(reference) = &filter.session_reference {
            ReferenceResolver::new(self.index).resolve_session(
                reference,
                request_identifier,
                operation,
            )?;
        }
        if let Some(reference) = &filter.subagent_reference {
            ReferenceResolver::new(self.index).resolve_subagent(
                reference,
                request_identifier,
                operation,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedTranscriptBlockSearchMatch {
    block: IndexedTranscriptBlock,
    evidence: TranscriptBlockSearchEvidence,
}

impl IndexedTranscriptBlockSearchMatch {
    pub fn new(block: IndexedTranscriptBlock, evidence: TranscriptBlockSearchEvidence) -> Self {
        Self { block, evidence }
    }

    pub fn reply_match(&self, projection: &CardProjection) -> TranscriptBlockSearchMatch {
        TranscriptBlockSearchMatch {
            card: self.block.card(projection),
            evidence: self.evidence.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlockSearcher {
    query: TranscriptBlockTextQuery,
    maximum_read_bytes: ByteLimit,
}

impl TranscriptBlockSearcher {
    pub fn new(query: TranscriptBlockTextQuery, maximum_read_bytes: ByteLimit) -> Self {
        Self {
            query,
            maximum_read_bytes,
        }
    }

    pub fn search(
        &self,
        blocks: Vec<IndexedTranscriptBlock>,
        request_identifier: &RequestIdentifier,
        operation: OperationKind,
    ) -> OutputOperationResult<Vec<IndexedTranscriptBlockSearchMatch>> {
        let mut matches = Vec::new();
        for block in blocks {
            if block.text_availability != TranscriptBlockTextAvailability::ReadableText {
                continue;
            }
            let text = TranscriptBlockBackingReader::new(block.clone(), self.maximum_read_bytes)
                .read_text(request_identifier, operation)?;
            let search_text = SearchText::new(text);
            if let Some(evidence) = self
                .query
                .as_query()
                .find_in(&search_text)
                .evidence()
                .cloned()
            {
                matches.push(IndexedTranscriptBlockSearchMatch::new(
                    block,
                    TranscriptBlockSearchEvidence::new(evidence),
                ));
            }
        }
        Ok(matches)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeAccumulator {
    byte_count: u64,
    line_count: u64,
    segment_count: u64,
}

impl SizeAccumulator {
    pub fn new() -> Self {
        Self {
            byte_count: 0,
            line_count: 0,
            segment_count: 0,
        }
    }

    pub fn observe_text(&mut self, text: &str) {
        self.byte_count += text.len() as u64;
        self.line_count += OutputLineCounter::new(text).count();
        self.segment_count += 1;
    }

    pub fn observe_size(&mut self, size: &SizeMetadata) {
        self.byte_count += size.byte_count.map_or(0, ByteCount::into_u64);
        self.line_count += size.line_count.map_or(0, LineCount::into_u64);
        self.segment_count += size.segment_count.map_or(0, ItemCount::into_u64);
    }

    pub fn finish(self) -> SizeMetadata {
        SizeMetadata {
            byte_count: Some(ByteCount::new(self.byte_count)),
            line_count: Some(LineCount::new(self.line_count)),
            segment_count: Some(ItemCount::new(self.segment_count)),
            certainty: SizeCertainty::Exact,
        }
    }
}

impl Default for SizeAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeMetadataFactory {
    byte_count: u64,
    line_count: u64,
    segment_count: Option<u64>,
}

impl SizeMetadataFactory {
    pub fn from_text(text: &str, segment_count: Option<u64>) -> Self {
        Self {
            byte_count: text.len() as u64,
            line_count: OutputLineCounter::new(text).count(),
            segment_count,
        }
    }

    pub fn exact(&self) -> SizeMetadata {
        SizeMetadata {
            byte_count: Some(ByteCount::new(self.byte_count)),
            line_count: Some(LineCount::new(self.line_count)),
            segment_count: self.segment_count.map(ItemCount::new),
            certainty: SizeCertainty::Exact,
        }
    }

    pub fn unknown() -> SizeMetadata {
        SizeMetadata {
            byte_count: None,
            line_count: None,
            segment_count: None,
            certainty: SizeCertainty::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoredStatusAccumulator {
    observed: BTreeSet<u8>,
}

impl AuthoredStatusAccumulator {
    pub fn new() -> Self {
        Self {
            observed: BTreeSet::new(),
        }
    }

    pub fn observe(&mut self, status: AuthoredStatus) {
        self.observed
            .insert(AuthoredStatusOrdinal::new(status).ordinal());
    }

    pub fn finish(&self) -> AuthoredStatus {
        if self.observed.is_empty() {
            return AuthoredStatus::UnknownAuthorship;
        }
        if self.observed.len() > 1 {
            return AuthoredStatus::MixedAuthorship;
        }
        self.observed
            .iter()
            .next()
            .copied()
            .and_then(AuthoredStatusOrdinal::status_for_ordinal)
            .unwrap_or(AuthoredStatus::UnknownAuthorship)
    }
}

impl Default for AuthoredStatusAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredStatusOrdinal {
    ordinal: u8,
}

impl AuthoredStatusOrdinal {
    pub fn new(status: AuthoredStatus) -> Self {
        let ordinal = match status {
            AuthoredStatus::AgentAuthored => 0,
            AuthoredStatus::HumanAuthored => 1,
            AuthoredStatus::MixedAuthorship => 2,
            AuthoredStatus::UnknownAuthorship => 3,
        };
        Self { ordinal }
    }

    pub fn ordinal(self) -> u8 {
        self.ordinal
    }

    pub fn status_for_ordinal(ordinal: u8) -> Option<AuthoredStatus> {
        match ordinal {
            0 => Some(AuthoredStatus::AgentAuthored),
            1 => Some(AuthoredStatus::HumanAuthored),
            2 => Some(AuthoredStatus::MixedAuthorship),
            3 => Some(AuthoredStatus::UnknownAuthorship),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampOrdering<'a> {
    timestamp: &'a Timestamp,
}

impl<'a> TimestampOrdering<'a> {
    pub fn new(timestamp: &'a Timestamp) -> Self {
        Self { timestamp }
    }

    pub fn is_before_optional(&self, other: Option<&Timestamp>) -> bool {
        other.is_none_or(|other| self.timestamp.as_str() < other.as_str())
    }

    pub fn is_after_optional(&self, other: Option<&Timestamp>) -> bool {
        other.is_none_or(|other| self.timestamp.as_str() > other.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRejectedFactory {
    request_identifier: RequestIdentifier,
    operation: OperationKind,
}

impl OperationRejectedFactory {
    pub fn new(request_identifier: RequestIdentifier, operation: OperationKind) -> Self {
        Self {
            request_identifier,
            operation,
        }
    }

    pub fn rejected(
        &self,
        reason: OperationRejectionReason,
        reference: Option<RejectedFragileReference>,
    ) -> OperationRejected {
        OperationRejected {
            request_identifier: self.request_identifier.clone(),
            operation: self.operation,
            reason,
            reference,
        }
    }

    pub fn missing(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::Missing, reference)
    }

    pub fn stale(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::FragileReferenceStale, reference)
    }

    pub fn broken(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::FragileReferenceBroken, reference)
    }

    pub fn oversized(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::Oversized, reference)
    }

    pub fn unsupported(&self) -> OperationRejected {
        self.rejected(OperationRejectionReason::Unsupported, None)
    }

    pub fn unsupported_reference(
        &self,
        reference: Option<RejectedFragileReference>,
    ) -> OperationRejected {
        self.rejected(OperationRejectionReason::Unsupported, reference)
    }

    pub fn unauthorized(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::Unauthorized, reference)
    }

    pub fn invalid_request(&self) -> OperationRejected {
        self.rejected(OperationRejectionReason::InvalidRequest, None)
    }

    pub fn invalid_range(&self, reference: Option<RejectedFragileReference>) -> OperationRejected {
        self.rejected(OperationRejectionReason::InvalidRange, reference)
    }

    pub fn invalid_query(&self) -> OperationRejected {
        self.rejected(OperationRejectionReason::InvalidQuery, None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKindName {
    source: SourceKind,
}

impl SourceKindName {
    pub fn new(source: SourceKind) -> Self {
        Self { source }
    }

    pub fn as_str(&self) -> &'static str {
        match self.source {
            SourceKind::Claude => "Claude",
            SourceKind::Codex => "Codex",
            SourceKind::Pi => "Pi",
            SourceKind::Repository => "Repository",
        }
    }

    pub fn parse(value: &str) -> Option<SourceKind> {
        match value {
            "Claude" => Some(SourceKind::Claude),
            "Codex" => Some(SourceKind::Codex),
            "Pi" => Some(SourceKind::Pi),
            "Repository" => Some(SourceKind::Repository),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockKindName {
    kind: TranscriptBlockKind,
}

impl TranscriptBlockKindName {
    pub fn new(kind: TranscriptBlockKind) -> Self {
        Self { kind }
    }

    pub fn as_str(&self) -> &'static str {
        match self.kind {
            TranscriptBlockKind::UserPrompt => "UserPrompt",
            TranscriptBlockKind::AgentResponse => "AgentResponse",
            TranscriptBlockKind::ToolCall => "ToolCall",
            TranscriptBlockKind::ToolResult => "ToolResult",
            TranscriptBlockKind::Inference => "Inference",
            TranscriptBlockKind::SystemInstruction => "SystemInstruction",
            TranscriptBlockKind::Attachment => "Attachment",
            TranscriptBlockKind::SessionEvent => "SessionEvent",
            TranscriptBlockKind::Unclassified => "Unclassified",
        }
    }

    pub fn parse(value: &str) -> Option<TranscriptBlockKind> {
        match value {
            "UserPrompt" => Some(TranscriptBlockKind::UserPrompt),
            "AgentResponse" => Some(TranscriptBlockKind::AgentResponse),
            "ToolCall" => Some(TranscriptBlockKind::ToolCall),
            "ToolResult" => Some(TranscriptBlockKind::ToolResult),
            "Inference" => Some(TranscriptBlockKind::Inference),
            "SystemInstruction" => Some(TranscriptBlockKind::SystemInstruction),
            "Attachment" => Some(TranscriptBlockKind::Attachment),
            "SessionEvent" => Some(TranscriptBlockKind::SessionEvent),
            "Unclassified" => Some(TranscriptBlockKind::Unclassified),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptBlockTextAvailabilityName {
    availability: TranscriptBlockTextAvailability,
}

impl TranscriptBlockTextAvailabilityName {
    pub fn new(availability: TranscriptBlockTextAvailability) -> Self {
        Self { availability }
    }

    pub fn as_str(&self) -> &'static str {
        match self.availability {
            TranscriptBlockTextAvailability::ReadableText => "ReadableText",
            TranscriptBlockTextAvailability::UnavailableText => "UnavailableText",
            TranscriptBlockTextAvailability::EncryptedText => "EncryptedText",
        }
    }

    pub fn parse(value: &str) -> Option<TranscriptBlockTextAvailability> {
        match value {
            "ReadableText" => Some(TranscriptBlockTextAvailability::ReadableText),
            "UnavailableText" => Some(TranscriptBlockTextAvailability::UnavailableText),
            "EncryptedText" => Some(TranscriptBlockTextAvailability::EncryptedText),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredStatusName {
    status: AuthoredStatus,
}

impl AuthoredStatusName {
    pub fn new(status: AuthoredStatus) -> Self {
        Self { status }
    }

    pub fn as_str(&self) -> &'static str {
        match self.status {
            AuthoredStatus::AgentAuthored => "AgentAuthored",
            AuthoredStatus::HumanAuthored => "HumanAuthored",
            AuthoredStatus::MixedAuthorship => "MixedAuthorship",
            AuthoredStatus::UnknownAuthorship => "UnknownAuthorship",
        }
    }

    pub fn parse(value: &str) -> Option<AuthoredStatus> {
        match value {
            "AgentAuthored" => Some(AuthoredStatus::AgentAuthored),
            "HumanAuthored" => Some(AuthoredStatus::HumanAuthored),
            "MixedAuthorship" => Some(AuthoredStatus::MixedAuthorship),
            "UnknownAuthorship" => Some(AuthoredStatus::UnknownAuthorship),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeCertaintyName {
    certainty: SizeCertainty,
}

impl SizeCertaintyName {
    pub fn new(certainty: SizeCertainty) -> Self {
        Self { certainty }
    }

    pub fn as_str(&self) -> &'static str {
        match self.certainty {
            SizeCertainty::Exact => "Exact",
            SizeCertainty::Estimated => "Estimated",
            SizeCertainty::Unknown => "Unknown",
        }
    }

    pub fn parse(value: &str) -> Option<SizeCertainty> {
        match value {
            "Exact" => Some(SizeCertainty::Exact),
            "Estimated" => Some(SizeCertainty::Estimated),
            "Unknown" => Some(SizeCertainty::Unknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeMetadataJson<'a> {
    size: &'a SizeMetadata,
}

impl<'a> SizeMetadataJson<'a> {
    pub fn new(size: &'a SizeMetadata) -> Self {
        Self { size }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "byte_count": self.size.byte_count.map(ByteCount::into_u64),
            "line_count": self.size.line_count.map(LineCount::into_u64),
            "segment_count": self.size.segment_count.map(ItemCount::into_u64),
            "certainty": SizeCertaintyName::new(self.size.certainty).as_str(),
        })
    }

    pub fn from_json(value: &Value) -> Option<SizeMetadata> {
        let reader = JsonReader::new(value);
        Some(SizeMetadata {
            byte_count: reader.optional_u64("byte_count").map(ByteCount::new),
            line_count: reader.optional_u64("line_count").map(LineCount::new),
            segment_count: reader.optional_u64("segment_count").map(ItemCount::new),
            certainty: SizeCertaintyName::parse(&reader.string("certainty")?)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JsonReader<'a> {
    value: &'a Value,
}

impl<'a> JsonReader<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self { value }
    }

    pub fn value(&self, name: &str) -> Option<&'a Value> {
        self.value.get(name)
    }

    pub fn string(&self, name: &str) -> Option<String> {
        self.value
            .get(name)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }

    pub fn optional_string(&self, name: &str) -> Option<String> {
        self.value
            .get(name)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }

    pub fn u64(&self, name: &str) -> Option<u64> {
        self.value.get(name).and_then(Value::as_u64)
    }

    pub fn optional_u64(&self, name: &str) -> Option<u64> {
        self.value.get(name).and_then(Value::as_u64)
    }

    pub fn i64(&self, name: &str) -> Option<i64> {
        self.value.get(name).and_then(Value::as_i64)
    }

    pub fn array(&self, name: &str) -> Vec<&'a Value> {
        self.value
            .get(name)
            .and_then(Value::as_array)
            .map(|values| values.iter().collect())
            .unwrap_or_default()
    }

    pub fn strings(&self, name: &str) -> Vec<String> {
        self.value
            .get(name)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableReference {
    prefix: &'static str,
    material: String,
}

impl StableReference {
    pub fn new(prefix: &'static str, material: impl Into<String>) -> Self {
        Self {
            prefix,
            material: material.into(),
        }
    }

    pub fn as_string(&self) -> String {
        format!(
            "{}:v1:{}",
            self.prefix,
            StableHash::new(&self.material).hex()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableHash {
    material: String,
}

impl StableHash {
    pub fn new(material: impl Into<String>) -> Self {
        Self {
            material: material.into(),
        }
    }

    pub fn hex(&self) -> String {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in self.material.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("{hash:016x}")
    }
}

macro_rules! index_merger {
    ($name:ident, $item:ty, $reference:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            existing: Vec<$item>,
            current: Vec<$item>,
        }

        impl $name {
            pub fn new(existing: Vec<$item>, current: Vec<$item>) -> Self {
                Self { existing, current }
            }

            pub fn merge(self) -> Vec<$item> {
                let mut map = BTreeMap::new();
                for item in self.existing.into_iter().chain(self.current.into_iter()) {
                    map.insert(item.$reference.as_str().to_string(), item);
                }
                map.into_values().collect()
            }
        }
    };
}

index_merger!(IndexedSessionMerger, IndexedSession, reference);
index_merger!(IndexedSubagentMerger, IndexedSubagent, reference);
index_merger!(IndexedOutputMerger, IndexedOutput, reference);
index_merger!(IndexedSegmentMerger, IndexedOutputSegment, reference);
index_merger!(
    IndexedTranscriptBlockMerger,
    IndexedTranscriptBlock,
    reference
);
