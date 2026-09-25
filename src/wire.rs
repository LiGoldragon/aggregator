//! The two surfaces a contract value crosses: Datom text and the Signal frame.
//!
//! Text is what a person and a CLI exchange; the frame is what two processes
//! exchange over a Unix socket. One request is one frame carrying the rkyv
//! archive of a `Query`; one reply is one frame carrying a `Response`. There
//! are no exchange identifiers, lanes, or sub-replies: `signal` owns no
//! protocol above the archive, and the living has not decided one.

use std::{
    io::{Read, Write},
    num::NonZeroUsize,
    os::unix::net::UnixStream,
    path::PathBuf,
};

use datom_codec::{Actualizing, Budget, Composing, Datom, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use rkyv::{
    Archive, Deserialize, Portable,
    api::high::{HighDeserializer, HighValidator},
    bytecheck::CheckBytes,
    ptr_meta::Pointee,
    rancor,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use signal::{ByteViewable, FrameCapacity, FrameReading, FrameWriting, Signalizable};
use signal_aggregator::{ArchivedQuery, ArchivedResponse};

use crate::{
    Error, Result,
    text_query::{ArchivedTree, MAXIMUM_PROJECTION_DEPTH, TextQueryProjectionFault},
};

/// The extent a single Datom text is read within.
///
/// Configuration and request texts are bounded inputs; the ceiling is set well
/// above the largest configuration aggregator writes and far below what would
/// let one text exhaust the process.
pub const MAXIMUM_DATOM_EXTENT: i64 = 1 << 20;

/// The same extent as a byte count, which is what the protos reader spends.
pub const MAXIMUM_DATOM_TEXT_BYTES: usize = MAXIMUM_DATOM_EXTENT as usize;

/// The greatest nesting a Datom text may reach.
pub const MAXIMUM_DATOM_DEPTH: i64 = 256;

/// Datom text in and out of a contract value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatomText;

impl DatomText {
    /// Reads one contract value from Datom text.
    pub fn read<T: Composing>(context: &'static str, text: &str) -> Result<T> {
        Potential::<T>::from(text)
            .actualize(&mut Self::budget())
            .map_err(|fault| Error::datom(context, format!("{fault:?}")))
    }

    /// Prints one contract value as Datom text.
    pub fn print<T: Datomizable<Output = Datom>>(value: &T) -> String {
        value.datomize(Vec::new()).protosize().textualize()
    }

    pub fn budget() -> Budget {
        Budget {
            remaining: MAXIMUM_DATOM_EXTENT,
            reader: ReaderBudget {
                remaining: MAXIMUM_DATOM_TEXT_BYTES,
            },
            depth: 0,
            maximum_depth: MAXIMUM_DATOM_DEPTH,
        }
    }
}

/// A contract value that renders itself as Datom text.
pub trait DatomTextual {
    fn datom_text(&self) -> String;
}

impl<T: Datomizable<Output = Datom>> DatomTextual for T {
    fn datom_text(&self) -> String {
        DatomText::print(self)
    }
}

/// One Signal frame carrying one archived contract value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalFrame;

impl SignalFrame {
    /// Writes one value as one frame.
    pub fn write<T: Signalizable>(
        context: &'static str,
        writer: &mut impl Write,
        value: &T,
    ) -> Result<()> {
        let archive = value
            .signalize()
            .map_err(|error| Error::archive(context, error.to_string()))?;
        writer
            .write_frame(&archive, FrameCapacity::default())
            .map_err(|error| Error::frame(context, error))
    }

    /// Reads one frame and restores the value it carries, refusing it before
    /// decoding when its nesting passes the bounds of a received frame.
    pub fn read<T: Receivable>(context: &'static str, reader: &mut impl Read) -> Result<T> {
        let body = reader
            .read_frame(FrameCapacity::default())
            .map_err(|error| Error::frame(context, error))?;
        ReceivedFrame::new(body.bytes().to_vec())
            .restore()
            .map_err(|refusal| Error::frame_refused(context, refusal))
    }
}

/// The pointer nesting a contract value may wrap around one of its trees.
///
/// A request or reply reaches its tree through a few fixed layers (the root,
/// a list of matches, a leaf's phrase and word); this allowance covers them
/// with room to spare and is the only slack above the tree bound.
pub const FRAME_ENVELOPE_NESTING: usize = 16;

/// The greatest pointer nesting rkyv validation enters in one received frame.
///
/// Validation recurses once per nested pointer, so this ceiling bounds the
/// recursion before anything is decoded. Inside it, each tree is then measured
/// exactly against the projection budget while still archived.
pub const MAXIMUM_FRAME_NESTING: usize = MAXIMUM_PROJECTION_DEPTH + FRAME_ENVELOPE_NESTING;

/// Why a received frame was refused before it became a Rust value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameRefusal {
    /// The bytes do not validate as the expected archive, or nest pointers
    /// past `MAXIMUM_FRAME_NESTING`; nothing in them can be trusted, not even
    /// a request identifier.
    #[error("frame does not validate within {MAXIMUM_FRAME_NESTING} nested pointers: {detail}")]
    Unvalidated { detail: String },
    /// The frame validates but carries a tree past the projection bounds.
    #[error("request {request_identifier} carries a tree outside its bound: {fault}")]
    TreeOutsideBound {
        request_identifier: String,
        fault: TextQueryProjectionFault,
    },
    /// The validated archive could not be decoded.
    #[error("frame does not decode: {detail}")]
    Undecoded { detail: String },
}

/// An archived contract value whose trees are measured before it is decoded.
///
/// A value that carries no tree has nothing to measure.
pub trait BoundedFrame {
    fn bound_trees(&self) -> std::result::Result<(), FrameRefusal> {
        Ok(())
    }
}

impl BoundedFrame for ArchivedQuery {
    fn bound_trees(&self) -> std::result::Result<(), FrameRefusal> {
        match self {
            ArchivedQuery::SearchTranscriptBlocks(request) => request
                .transcript_block_text_query
                .measure()
                .map_err(|fault| FrameRefusal::TreeOutsideBound {
                    request_identifier: request.request_identifier.as_str().to_owned(),
                    fault,
                }),
            _ => Ok(()),
        }
    }
}

impl BoundedFrame for ArchivedResponse {
    fn bound_trees(&self) -> std::result::Result<(), FrameRefusal> {
        match self {
            ArchivedResponse::TranscriptBlocksSearched(searched) => searched
                .transcript_block_search_matches
                .iter()
                .try_for_each(|found| found.transcript_block_search_evidence.measure())
                .map_err(|fault| FrameRefusal::TreeOutsideBound {
                    request_identifier: searched.request_identifier.as_str().to_owned(),
                    fault,
                }),
            _ => Ok(()),
        }
    }
}

impl BoundedFrame for meta_signal_aggregator::ArchivedQuery {}

impl BoundedFrame for meta_signal_aggregator::ArchivedResponse {}

/// A contract value that can be restored from a received frame, named by the
/// archived form its frame carries.
pub trait Receivable: Sized {
    type Archived: Portable
        + Pointee<Metadata = ()>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<Self, HighDeserializer<rancor::Error>>
        + BoundedFrame;
}

impl<T> Receivable for T
where
    T: Archive,
    T::Archived: Portable
        + Pointee<Metadata = ()>
        + for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, HighDeserializer<rancor::Error>>
        + BoundedFrame,
{
    type Archived = T::Archived;
}

/// The body of one received frame, not yet trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedFrame {
    bytes: Vec<u8>,
}

impl ReceivedFrame {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Validates the archive within `MAXIMUM_FRAME_NESTING`, measures its trees
    /// in place, and only then decodes it.
    pub fn restore<T: Receivable>(&self) -> std::result::Result<T, FrameRefusal> {
        let archived = self.access::<T>()?;
        archived.bound_trees()?;
        rkyv::deserialize::<T, rancor::Error>(archived).map_err(|error| FrameRefusal::Undecoded {
            detail: error.to_string(),
        })
    }

    fn access<T: Receivable>(
        &self,
    ) -> std::result::Result<&<T as Receivable>::Archived, FrameRefusal> {
        let mut validator = Validator::new(
            ArchiveValidator::with_max_depth(&self.bytes, NonZeroUsize::new(MAXIMUM_FRAME_NESTING)),
            SharedValidator::new(),
        );
        rkyv::api::access_with_context::<<T as Receivable>::Archived, _, rancor::Error>(
            &self.bytes,
            &mut validator,
        )
        .map_err(|error| FrameRefusal::Unvalidated {
            detail: error.to_string(),
        })
    }
}

/// A Unix socket that carries one query and returns one response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnixSocketClient {
    path: PathBuf,
}

impl UnixSocketClient {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn exchange<Q, R>(&self, query: &Q) -> Result<R>
    where
        Q: Signalizable,
        R: Receivable,
    {
        let mut stream = UnixStream::connect(&self.path)
            .map_err(|error| Error::io("connecting unix socket", error))?;
        SignalFrame::write("query", &mut stream, query)?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(|error| Error::io("shutting down socket write", error))?;
        SignalFrame::read("response", &mut stream)
    }
}
