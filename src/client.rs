use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
};

use meta_signal_aggregator::{AggregatorConfiguration, MetaAggregatorRequest};
use nota::{NotaEncode, NotaSource};
use signal_aggregator::AggregatorRequest;

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
        let _request = NotaSource::new(&request_text)
            .parse::<AggregatorRequest>()
            .map_err(|error| Error::nota("ordinary request decode", error.to_string()))?;
        let reply =
            UnixSocketClient::new(PathBuf::from(configuration.ordinary_socket_path.as_str()))
                .exchange(&request_text)?;
        print!("{reply}");
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
        let _request = NotaSource::new(&request_text)
            .parse::<MetaAggregatorRequest>()
            .map_err(|error| Error::nota("meta request decode", error.to_string()))?;
        let reply = UnixSocketClient::new(PathBuf::from(configuration.meta_socket_path.as_str()))
            .exchange(&request_text)?;
        print!("{reply}");
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

    pub fn exchange(&self, request_text: &str) -> Result<String> {
        let mut stream = UnixStream::connect(&self.path)
            .map_err(|error| Error::io("connecting unix socket", error))?;
        stream
            .write_all(request_text.as_bytes())
            .map_err(|error| Error::io("writing socket request", error))?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(|error| Error::io("shutting down socket write", error))?;
        let mut reply = String::new();
        stream
            .read_to_string(&mut reply)
            .map_err(|error| Error::io("reading socket reply", error))?;
        Ok(reply)
    }
}
