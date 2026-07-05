use aggregator::MetaAggregatorClientCommand;

fn main() {
    if let Err(error) = MetaAggregatorClientCommand::from_environment().run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
