# Generated runtime schema artifacts

The runtime schema sketch in `schema/runtime.schema` documents the intended
Signal/Nexus/SEMA split. This scaffold does not wire `schema-rust` codegen yet.

TODO when the runtime schema generator is ready for this component:

```sh
AGGREGATOR_UPDATE_SCHEMA_ARTIFACTS=1 cargo check
```

Expected generated modules will live under `src/schema/`, matching the daemon
module pattern used by established runtime components.
