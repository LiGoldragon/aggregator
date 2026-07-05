use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
};

use meta_signal_aggregator::{
    AggregatorConfiguration, MetaAggregatorFrame, MetaAggregatorFrameBody, MetaAggregatorReply,
    MetaAggregatorRequest,
};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::{AggregatorFrame, AggregatorFrameBody, AggregatorReply, AggregatorRequest};
use signal_frame::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as FrameReply, Request,
    SessionEpoch, SubReply,
};

use crate::{ConfigurationStore, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatorClientCommand {
    arguments: ClientCommandArguments,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaAggregatorClientCommand {
    arguments: ClientCommandArguments,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationWriterCommand {
    arguments: ClientCommandArguments,
}

impl AggregatorClientCommand {
    pub fn from_environment() -> Self {
        Self {
            arguments: ClientCommandArguments::from_environment(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let configuration = self.arguments.configuration_store()?.read_configuration()?;
        let request_text = self.arguments.input_text()?;
        let request = NotaSource::new(&request_text)
            .parse::<AggregatorRequest>()
            .map_err(|error| Error::nota("ordinary request decode", error.to_string()))?;
        let reply =
            UnixSocketClient::new(PathBuf::from(configuration.ordinary_socket_path.as_str()))
                .exchange_ordinary(request)?;
        print!("{}", reply.to_nota());
        Ok(())
    }
}

impl MetaAggregatorClientCommand {
    pub fn from_environment() -> Self {
        Self {
            arguments: ClientCommandArguments::from_environment(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let configuration = self.arguments.configuration_store()?.read_configuration()?;
        let request_text = self.arguments.input_text()?;
        let request = NotaSource::new(&request_text)
            .parse::<MetaAggregatorRequest>()
            .map_err(|error| Error::nota("meta request decode", error.to_string()))?;
        let reply = UnixSocketClient::new(PathBuf::from(configuration.meta_socket_path.as_str()))
            .exchange_meta(request)?;
        print!("{}", reply.to_nota());
        Ok(())
    }
}

impl ConfigurationWriterCommand {
    pub fn from_environment() -> Self {
        Self {
            arguments: ClientCommandArguments::from_environment(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let text = self.arguments.input_text()?;
        let configuration = NotaSource::new(&text)
            .parse::<AggregatorConfiguration>()
            .map_err(|error| Error::nota("configuration decode", error.to_string()))?;
        self.arguments
            .configuration_store()?
            .write_configuration(&configuration)?;
        println!("{}", configuration.to_nota());
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCommandArguments {
    arguments: Vec<String>,
}

impl ClientCommandArguments {
    pub fn from_environment() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    pub fn configuration_store(&self) -> Result<ConfigurationStore> {
        Ok(ConfigurationStore::at_path(self.configuration_path()?))
    }

    pub fn configuration_path(&self) -> Result<PathBuf> {
        if let Some(path) = self.flag_value("--configuration") {
            return Ok(PathBuf::from(path));
        }
        std::env::var("AGGREGATOR_CONFIGURATION")
            .map(PathBuf::from)
            .map_err(|_| Error::argument("missing --configuration or AGGREGATOR_CONFIGURATION"))
    }

    pub fn input_text(&self) -> Result<String> {
        if let Some(text) = self.flag_value("--request") {
            return Ok(text.to_string());
        }
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|error| Error::io("reading standard input", error))?;
        if text.trim().is_empty() {
            Err(Error::argument(
                "request/configuration NOTA is required on stdin",
            ))
        } else {
            Ok(text)
        }
    }

    pub fn flag_value(&self, name: &str) -> Option<&str> {
        self.arguments
            .windows(2)
            .find(|window| window[0] == name)
            .map(|window| window[1].as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnixSocketClient {
    path: PathBuf,
}

impl UnixSocketClient {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn exchange_ordinary(&self, request: AggregatorRequest) -> Result<AggregatorReply> {
        let frame = AggregatorFrame::new(AggregatorFrameBody::Request {
            exchange: SocketExchangeIdentity::first().connector_exchange(),
            request: Request::from_payload(request),
        });
        let reply_frame = AggregatorFrame::decode_length_prefixed(
            &self.exchange_bytes(
                &frame
                    .encode_length_prefixed()
                    .map_err(|error| Error::frame("ordinary request encode", error))?,
            )?,
        )
        .map_err(|error| Error::frame("ordinary reply decode", error))?;
        OrdinaryReplyEnvelope::new(reply_frame).single_reply()
    }

    pub fn exchange_meta(&self, request: MetaAggregatorRequest) -> Result<MetaAggregatorReply> {
        let frame = MetaAggregatorFrame::new(MetaAggregatorFrameBody::Request {
            exchange: SocketExchangeIdentity::first().connector_exchange(),
            request: Request::from_payload(request),
        });
        let reply_frame = MetaAggregatorFrame::decode_length_prefixed(
            &self.exchange_bytes(
                &frame
                    .encode_length_prefixed()
                    .map_err(|error| Error::frame("meta request encode", error))?,
            )?,
        )
        .map_err(|error| Error::frame("meta reply decode", error))?;
        MetaReplyEnvelope::new(reply_frame).single_reply()
    }

    pub fn exchange_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.path)
            .map_err(|error| Error::io("connecting unix socket", error))?;
        stream
            .write_all(request_bytes)
            .map_err(|error| Error::io("writing socket request", error))?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(|error| Error::io("shutting down socket write", error))?;
        let mut reply = Vec::new();
        stream
            .read_to_end(&mut reply)
            .map_err(|error| Error::io("reading socket reply", error))?;
        Ok(reply)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketExchangeIdentity {
    session_epoch: SessionEpoch,
}

impl SocketExchangeIdentity {
    pub fn first() -> Self {
        Self {
            session_epoch: SessionEpoch::new(1),
        }
    }

    pub fn connector_exchange(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            self.session_epoch,
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrdinaryReplyEnvelope {
    frame: AggregatorFrame,
}

impl OrdinaryReplyEnvelope {
    pub fn new(frame: AggregatorFrame) -> Self {
        Self { frame }
    }

    pub fn single_reply(self) -> Result<AggregatorReply> {
        match self.frame.into_body() {
            AggregatorFrameBody::Reply { reply, .. } => Self::single_committed_reply(reply),
            other => Err(Error::protocol(
                "ordinary reply shape",
                format!("expected reply frame, got {other:?}"),
            )),
        }
    }

    pub fn single_committed_reply(reply: FrameReply<AggregatorReply>) -> Result<AggregatorReply> {
        match reply {
            FrameReply::Accepted {
                outcome: AcceptedOutcome::Committed,
                per_operation,
            } if per_operation.len() == 1 => match per_operation.into_head() {
                SubReply::Ok(reply) => Ok(reply),
                other => Err(Error::protocol(
                    "ordinary reply shape",
                    format!("expected successful sub-reply, got {other:?}"),
                )),
            },
            FrameReply::Rejected { reason } => Err(Error::protocol(
                "ordinary reply rejection",
                format!("request rejected before execution: {reason}"),
            )),
            other => Err(Error::protocol(
                "ordinary reply shape",
                format!("expected committed single-operation reply, got {other:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaReplyEnvelope {
    frame: MetaAggregatorFrame,
}

impl MetaReplyEnvelope {
    pub fn new(frame: MetaAggregatorFrame) -> Self {
        Self { frame }
    }

    pub fn single_reply(self) -> Result<MetaAggregatorReply> {
        match self.frame.into_body() {
            MetaAggregatorFrameBody::Reply { reply, .. } => Self::single_committed_reply(reply),
            other => Err(Error::protocol(
                "meta reply shape",
                format!("expected reply frame, got {other:?}"),
            )),
        }
    }

    pub fn single_committed_reply(
        reply: FrameReply<MetaAggregatorReply>,
    ) -> Result<MetaAggregatorReply> {
        match reply {
            FrameReply::Accepted {
                outcome: AcceptedOutcome::Committed,
                per_operation,
            } if per_operation.len() == 1 => match per_operation.into_head() {
                SubReply::Ok(reply) => Ok(reply),
                other => Err(Error::protocol(
                    "meta reply shape",
                    format!("expected successful sub-reply, got {other:?}"),
                )),
            },
            FrameReply::Rejected { reason } => Err(Error::protocol(
                "meta reply rejection",
                format!("request rejected before execution: {reason}"),
            )),
            other => Err(Error::protocol(
                "meta reply shape",
                format!("expected committed single-operation reply, got {other:?}"),
            )),
        }
    }
}
