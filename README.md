# Pony-Kafka

Welcome to Pony Kafka. 

This is a pure kafka client written in Pony. The goal is to eventually reach feature parity with the official kafka client.

# Why

The main reason this exists is because the alternatives weren't necessarily going to be clean/easy to integrate into Pony and its normal "async everything" manner of working.

# Building

You need [ponyc](https://github.com/ponylang/ponyc) to compile `pony-kafka`. This is currently tested with `ponyc` version master.

You also need the following (in addition to what is needed for Pony itself):

* LZ4
* Snappy
* Zlib

For Ubuntu 16.04 or newer you can run:

```bash
sudo apt-get install libsnappy-dev liblz4-dev zlib1g-dev
```

For older Ubuntu you can run:

```bash
sudo apt-get install libsnappy-dev
cd /tmp
wget -O liblz4-1.7.5.tar.gz https://github.com/lz4/lz4/archive/v1.7.5.tar.gz
tar zxvf liblz4-1.7.5.tar.gz
cd lz4-1.7.5
sudo make install
```

For OSX you can run:

```bash
brew install snappy lz4
```

You can then build the pony-kafka tests by running:

```bash
ponyc pony-kafka
```

or the example performance application by running:

```bash
ponyc examples/performance
```

# Current status

This is currently alpha quality software that still needs more work before it is production ready.

A quick summary of features:

Feature | Description | Status
--- | --- | ---
Basic Consumer | Ability to connect to kafka brokers and consume messages | Implemented
Group Consumer | Ability to do high level consumer failover like official kafka client | Not Implemented
Producer | Ability to connect to kafka brokers and produce messages | Implemented
Leader Failover | Ability to correctly recover from/react to kafka leader failover | Partially Implemented
Compression | Ability to use LZ4/Snappy/Zlib compression for message sets | Implemented
Message Format V2 | Ability to use message set format version 2 | Not Implemented
Idempotence/Transaction | Ability to use idempotence/transactions | Not Implemented
Metrics | Ability to collect metrics and provide reports of metrics periodically | Not Implemented
Security | Ability to use SSL/SASL/etc to secure connection to Kafka brokers | Not Implemented
Message Interceptors | Ability to intercept messages before produce and after consume to be able to modify them or extract metadata from them for monitoring and other purposes | Not Implemented
Producer Batching | Ability to batch produce requests for efficiency | Implemented
Producer Rate Limiting | Ability to limit number of outstanding produce requests | Implemented
Throttling | Ability to tell producers of data to slow down due to network congestion | Implemented
Message Delivery Reports | Report back to producers once Kafka has confirm message has been successfully stored | Implemented
Logging | Logging of what is happening/errors | Partially Implemented
Error Handling | Ability to gracefully handle errors (retry if possible; fail fast if not) | Partially Implemented
Documentation | Comprehensive documentation for developers using Pony Kafka and developers enhancing Pony Kafka | Not Implemented
Testing | Comprehensive test suite to confirm everything is working as expected | Not Implemented
