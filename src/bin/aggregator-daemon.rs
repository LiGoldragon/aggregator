use aggregator::AggregatorDaemonCommand;

fn main() {
    if let Err(error) = AggregatorDaemonCommand::from_environment().run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
