use aggregator::AggregatorClientCommand;

fn main() {
    if let Err(error) = AggregatorClientCommand::from_environment().run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
