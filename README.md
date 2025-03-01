# Brume

Brume is a lightweight tool designed for **bidirectional file synchronization**.
It is composed of three main components:
- a Rust library for backend-agnostic synchronization,
- a daemon that runs in the background and periodically synchronizes files,
- a command-line interface (CLI) tool to interact with the daemon.

Brume's synchronization algorithm is bi-directional, meaning that files can be synchronized from the local file system to the remote server and vice versa.
Conflicts are automatically detected, but should be manually resolved by the user. It is inspired by the [csync](https://csync.org/) library used by the nextcloud client.


> [!CAUTION]
> Do not use this project in production, it should be considered very early WIP.

## Supported Backends

Brume's synchronization is backend agnostic, new backend can be easily added.

It currently supports:
- local unix filesystem
- Nextcloud server

## Usage

Assuming you have a working Rust toolchain and have cloned this project:
1. Start the daemon:
```
cargo run --release --bin brumed
```

2. Create a new synchronization using the CLI:
```
cargo run --release --bin=brume-cli new --local /tmp/test --remote http://admin:admin@localhost:8080
```

More commands are available:
```
❯ cargo run --release --bin=brume-cli help
CLI to manage folders synchronized with brume

Usage: brume-cli <COMMAND>

Commands:
  new      Create a new synchronization [aliases: add]
  ls       List all synchronizations [aliases: list]
  rm       Remove a synchronization [aliases: remove, delete]
  pause    Pause a synchronization
  resume   Resume a synchronization
  status   Get the status of a synchronization
  resolve  Resolve a conflict in a synchronization
  help     Print this message or the help of the given subcommand(s)

```
