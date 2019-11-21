krs
---
krs is a simple Kafka command-line tool written in Rust.

**Still in development. Please check later!**

## Features

*   Easy to type.
*   (Hopefully) intuitive to use.
*   Change brokers/zookeeper via environment variables, `.env` files, or pass it directly through the CLI.

## Quick Guide

For help, always consult `-h`:
```
krs -h
```

To list Kafka topics:
```
krs topics list -b localhost:9092
```

Describe a Kafka topic: 
```
krs topics describe -b localhost:9092 -z localhost:2181 -t topic-name
```

Brokers and Zookeeper hosts can be specified via environment variables:
```
export KRS_BROKERS=localhost:9092
krs topics list
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
