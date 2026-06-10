# Cross-validation regression suite

Validates the Rust server against the Java KairosDB implementation running on
the same Cassandra keyspace: every aggregator, group-by output, desc ordering,
gzip ingest, `query/tags`, `save_as` write-back, and cross-server delete.

## Running

1. Start Cassandra (e.g. `docker run -d -p 9042:9042 cassandra:4.1`).
2. Start the Java server on port 8082 against that Cassandra.
3. Start the Rust server: `KAIROSD_LISTEN=127.0.0.1:18080 KAIROSD_DATASTORE=cassandra cargo run -p kairosd`
4. `python3 regression/cross_validate.py`

Every check asserts byte-identical (or semantically identical) responses from
both servers. Exit code 0 means full parity on the covered surface.
