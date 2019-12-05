krs
---
krs is a simple Kafka command-line tool written in Rust, inspired by [kt](https://github.com/fgeller/kt).

[![crates.io](https://img.shields.io/crates/v/krs.svg)](https://crates.io/crates/krs)
[![Build Status](https://travis-ci.org/igozali/krs.svg?branch=master)](https://travis-ci.org/igozali/krs)

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org/).

### Features

*   Easy to type
*   Fast startup
*   Hopefully intuitive to use
*   Diagnostic output to stderr, main output to stdout, which allows below feature
*   JSON output for easy consumption with [jq](https://stedolan.github.io/jq/)
*   Change some context variables (e.g. Kafka brokers or Zookeeper host) via environment variables, `.env` files, or pass it directly through the CLI.

### Walkthrough

First and foremost, always consult `-h` or `help` (or let me know if some stuff
is unclear).
```bash
krs -h
krs help
# All subcommands also have `help`, thanks to the amazing `clap` library.
krs env help
```

krs collects Kafka brokers and Zookeeper hosts from environment variables,
`.env` file, and the CLI arguments as its context. To show context variables:
```bash
$ krs env show # or just `krs env`
```

If in doubt, you can always ask the tool which environment you're currently
working on:
```
$ cat .env
KRS_ZOOKEEPER=localhost:2181
$ export KRS_BROKERS=localhost:9092
$ krs env show
Environment:
  brokers: localhost:2181 (from env var (KRS_BROKERS))
  zookeeper: localhost:9092 (from .env file (KRS_ZOOKEEPER))
```

There's a small helper to set the context variables (or you can just put the
environment variables `KRS_BROKERS`/`KRS_ZOOKEEPER` in your `.env` file
directly):
```
$ krs env set -b localhost:9092 -z localhost:2181
```

Once you've set context variables, you can list Kafka topics:
```bash
krs topics list # or just `krs topics`
# You should pipe the output to `jq` to make it look much better.
krs topics | jq
```

Describe a Kafka topic: 
```bash
krs topics describe -b localhost:9092 -z localhost:2181 -t topic-name
```

Produce and consume from topics:
```
krs topics create -t my-topic
krs producer -t my-topic
Starting console producer. Press Ctrl+C to exit.
> hello
Sent: (0, 1)
```

### Compatibility

[MSRV](https://github.com/rust-embedded/wg/blob/master/ops/msrv.md) is Rust
1.39.0 (mostly because I built the tool with that version and haven't tested it
with previous ones).

Mostly tested with `Kafka 2.1.1-cp1 (Commit:9aa84c2aaa91e392)`.

### Upcoming Features

_Note: There will be no guarantees as to when these features are going to be
implemented, but feel free to submit a PR._

* Consume from specified offset (earliest, latest, etc.)
* Describe configs for other Kafka resources (consumer groups, etc.)
* Port more management commands
* Bit more fine-grained control for producer (handle keys, send to partitions)
* More colors on the stderr output.

### Why?

* Typing `kafka-console-consumer --broker-list localhost:9092 --topic test` is
  a mouthful.
* In cases you forgot the arguments to `kafka-topics --create`, the tool will
  bombard you with its complete list of options, burying the most important
  error message that tells you what arguments you're missing.
* JVM startup is annoyingly slow for simple CLI tools.
* [RIIR](https://github.com/ansuz/RIIR), but in good spirit! I think the [tool
  where the inspiration came from](https://github.com/fgeller/kt) was already a
  fantastic idea.
* But most importantly, I just needed a project to learn Rust! :)
