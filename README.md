krs
---
krs is a simple Kafka command-line tool written in Rust, inspired by [kt](https://github.com/fgeller/kt).

Dual-licensed under MIT or the [UNLICENSE](https://unlicense.org/).

## Features

*   Easy to type.
*   JSON output for easy consumption with `jq`.
*   Fast startup (thanks Rust!)
*   Hopefully intuitive to use
*   Change some context variables (e.g. Kafka brokers or Zookeeper host) via environment variables, `.env` files, or pass it directly through the CLI.

## Quick Guide

For help, always consult `-h` or `help`:
```bash
krs -h
krs help
```

To show context variables:
```bash
krs env show
# By default `krs env` will invoke `krs env show`
krs env
```

To list Kafka topics:
```bash
krs topics list -b localhost:9092
```

Describe a Kafka topic: 
```bash
krs topics describe -b localhost:9092 -z localhost:2181 -t topic-name
```

Kafka brokers and Zookeeper hosts can be specified via environment variables:
```bash
export KRS_BROKERS=localhost:9092
krs topics list
```

Consume from topics:
```
krs consumer -t 
```

You can always ask the tool which environment you're currently working on:
```
$ cat .env
KRS_ZOOKEEPER=localhost:2181
$ export KRS_BROKERS=localhost:9092
$ krs env show
Environment:
  brokers: localhost:2181 (from env var (KRS_BROKERS))
  zookeeper: localhost:9092 (from .env file (KRS_ZOOKEEPER))
```

## Why?

* Typing `kafka-console-consumer --broker-list localhost:9092 --topic test` is
  a mouthful.
* JVM startup is annoyingly slow for simple CLI tools.
* [RIIR](https://github.com/ansuz/RIIR)!
* But most importantly, I needed a Rust project to learn Rust! :)
