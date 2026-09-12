//! The two surfaces a contract value crosses: Datom text and the Signal frame.
//!
//! Text is what a person and a CLI exchange; the frame is what two processes
//! exchange over a Unix socket. One request is one frame carrying the rkyv
//! archive of a `Query`; one reply is one frame carrying a `Response`. There
//! are no exchange identifiers, lanes, or sub-replies: `signal` owns no
//! protocol above the archive, and the living has not decided one.

use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
};

use datom_codec::{Actualizing, Budget, Composing, Datom, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal::{
    ByteViewable, FrameCapacity, FrameReading, FrameWriting, Restorable, Signal, Signalizable,
};

use crate::{Error, Result};

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

    /// Reads one frame and restores the value it carries.
    pub fn read<T>(context: &'static str, reader: &mut impl Read) -> Result<T>
    where
        Signal<T>: Restorable<T>,
    {
        let body = reader
            .read_frame(FrameCapacity::default())
            .map_err(|error| Error::frame(context, error))?;
        Signal::<T>::from(body.bytes().to_vec())
            .restore()
            .map_err(|error| Error::archive(context, error.to_string()))
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
        Signal<R>: Restorable<R>,
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
