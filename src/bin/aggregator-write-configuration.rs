use aggregator::ConfigurationWriterCommand;

fn main() {
    if let Err(error) = ConfigurationWriterCommand::from_environment().run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
