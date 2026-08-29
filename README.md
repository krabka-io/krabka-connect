# krabka-connect

Krabka Connect: the connector framework, the connectors built on it, and the
worker that runs them.

A connector moves records between an external system and Kafka. This repository
defines what a connector *is* — the `Source`, `Sink` and `Converter` traits every
one implements — ships a Postgres change-data-capture source, and provides the
runtime that drives a connector, checkpoints its progress, and resumes it after
a restart.

It layers on four sibling repositories:
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for ids, units
and SASL, [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) for
the Kafka clients records are shipped over,
[`krabka-streams-rs`](https://github.com/krabka-io/krabka-streams-rs) for the
schema-registry serdes, and
[`krabka-broker`](https://github.com/krabka-io/krabka-broker) for the broker its
integration suites boot.

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-connect` | The connector SPI: `Source`, `Sink`, `Converter`, and the checkpoint contract between them |
| `krabka-connect-derive` | Derive macros for connector configuration |
| `krabka-connect-postgres` | A Postgres change-data-capture source, over logical decoding |
| `krabka-connect-worker` | The runtime: polls a source, batches into a sink, and checkpoints |
| `krabka-replicator` | Cluster-to-cluster replication, wire-compatible with MirrorMaker 2 |

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are supported and both are gated in CI. Cargo remains the dependency source
of truth: Bazel reads the same `Cargo.toml` and `Cargo.lock` through
`crate.from_cargo`, so there is no second dependency set to keep in sync.

## Sibling revisions

Sibling crates are declared against crates.io in the member manifests and pinned
by revision in one `[patch.crates-io]` table in [`Cargo.toml`](Cargo.toml). That
table is the only place a revision is bumped, and
[`sync-siblings`](.github/workflows/sync-siblings.yml) proposes those bumps as
pull requests.

Every crate each sibling publishes is listed there, not only the ones this
repository names directly. Cargo ignores a dependency's own patch table, so a
crate reached transitively would otherwise resolve from the registry while its
git twin is also in the graph.

## Suites that need a daemon

The Postgres CDC acceptance test starts Postgres and a Confluent Schema Registry
in containers, runs a connector against them, restarts it, and checks that it
resumes from its checkpoint rather than replaying or skipping. It needs a Docker
daemon, which Bazel cannot declare as an input, so it is tagged `manual` and
stays out of a plain `bazel test //...`:

```bash
cargo test -p krabka-connect-worker --test postgres_cdc -- --ignored
```

The broker it boots binds every interface and advertises itself on the Docker
bridge gateway, because the registry runs in a container and cannot route to
loopback.

## Mutation testing

```bash
bazel test //crates/connect:connect_mutants
```

Exclusions live in [`.cargo/mutants.toml`](.cargo/mutants.toml), which Bazel
reaches through `--@rules_rs_mutants//mutants:config`. Without that flag the
sweep enumerates from a synthetic workspace where the file is out of scope, and
the exclusions silently do not exclude.
