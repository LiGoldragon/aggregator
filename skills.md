# aggregator skills

`aggregator` is a runtime component. Keep the ordinary CLI and meta CLI thin:
they talk to the daemon through `signal-aggregator` and
`meta-signal-aggregator`. Keep collection in Nexus/adapters, configuration
commit in SEMA, and framed communication in Signal.
