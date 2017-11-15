/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

use "options"
use "customlogger"

class val KafkaConfigError
  let _message: String

  new val create(m: String) =>
    _message = m

  fun message(): String =>
    _message

primitive KafkaConfigCLIParser
  fun opts(): Array[(String, (None | String), ArgumentType, (Required |
    Optional), String)]
  =>
    // items in the tuple are: Argument Name, Argument Short Name,
    //   Argument Type, Required or Optional, Help Text
    let opts_array = Array[(String, (None | String), ArgumentType, (Required |
      Optional), String)]

    opts_array.push(("client_name", None, StringArgument, Required,
      "Name to identify client as with Kafka brokers (\"Kafka Client\")"))
    opts_array.push(("client_mode", None, StringArgument, Required,
      "producer or consumer"))
    opts_array.push(("topic", None, StringArgument, Required,
      "Kafka topic to consume from"))
    opts_array.push(("brokers", None, StringArgument, Required,
      "Initial brokers to connect to. Format: 'host:port,host:port,...'"))
    opts_array.push(("log_level", None, StringArgument, Required,
      "Log Level. Fine, Info, Warn, Error. (Warn)"))
    opts_array.push(("consumer_check_crc", None, None, Optional,
      "Verify CRC of consumed messages to check against corruption. Default is to not verify CRCs."))
    opts_array.push(("fetch_interval", None, I64Argument,
      Required, "How often to fetch data from kafka in nanoseconds (only applies if we didn't receive data on our previous fetch attempt) (100000000)"))
    opts_array.push(("min_fetch_bytes", None, I64Argument,
      Required, "Minimum # of bytes for Kafka broker to attempt to return on a fetch request (1)"))
    opts_array.push(("max_fetch_bytes", None, I64Argument,
      Required, "Maximum # of bytes for Kafka broker to attempt to return on a fetch request (100000000)"))
    opts_array.push(("partition_fetch_max_bytes", None, I64Argument,
      Required, "Maximum # of bytes to request for a single topic/partition at a time (1048576)"))
    opts_array.push(("produce_timeout_ms", None, I64Argument,
      Required, "Max # ms kafka can wait for all acks required (100)"))
    opts_array.push(("max_produce_buffer_ms", None, I64Argument,
      Required, "# ms to buffer for producing to kafka (0)"))
    opts_array.push(("max_produce_message_size", None, I64Argument, Required,
      "Max message size in bytes for producing to kafka after buffering (1000000)"))
    opts_array.push(("max_inflight_requests", None, I64Argument, Required,
      "Max # of in flight requests per broker connection (mainly applies to produce requests) (1000000)"))
    opts_array.push(("produce_acks", None, I64Argument, Required,
      "# of acks to wait for.. -1 = all replicas; 0 = no replicas; N = N replicas. (-1)"))
    opts_array.push(("producer_compression", None, StringArgument, Required,
      "Compression codec to use when producing messages to Kafka. gzip/snappy/lz4/none. (none)"))

    opts_array

  fun print_usage(out: OutStream) =>
    for (long, short, arg_type, arg_req, help) in opts().values() do
      let short_str = match short
             | let s: String => "/-" + s
             else "" end

      let arg_type_str = match arg_type
             | StringArgument => "(String)"
             | I64Argument => "(Integer)"
             | F64Argument => "(Float)"
             else "" end

      out.print("--" + long + short_str + "       " + arg_type_str + "    "
        + help)
    end

  fun apply(args: Array[String] val, out: OutStream): KafkaConfig val ? =>
    var log_level = "Warn"

    var topic = ""
    var brokers = recover val Array[(String, I32)] end

    var max_inflight_requests: USize = 1_000_000
    var max_message_size: I32 = 1_000_000
    var max_produce_buffer_ms: U64 = 0
    var client_mode = ""
    var client_name = "Kafka Client"
    var compression = "none"
    var produce_acks: I16 = -1
    var check_crc: Bool = false
    var produce_timeout_ms: I32 = 100
    var fetch_interval: U64 = 100_000_000
    var min_fetch_bytes: I32 = 1
    var max_fetch_bytes: I32 = 100_000_000
    var partition_fetch_max_bytes: I32 = 1_048_576

    let options = Options(args, false)

    for (long, short, arg_type, arg_req, _) in opts().values() do
      options.add(long, short, arg_type, arg_req)
    end

    // TODO: implement all the other options that kafka client supports
    for option in options do
      match option
      | ("max_produce_buffer_ms", let input: I64) =>
        max_produce_buffer_ms = input.u64()
      | ("max_produce_message_size", let input: I64) =>
        max_message_size = input.i32()
      | ("max_inflight_requests", let input: I64) =>
        max_inflight_requests = input.usize()
      | ("produce_timeout_ms", let input: I64) =>
        produce_timeout_ms = input.i32()
      | ("produce_acks", let input: I64) =>
        produce_acks = input.i16()
      | ("fetch_interval", let input: I64) =>
        fetch_interval = input.u64()
      | ("min_fetch_bytes", let input: I64) =>
        min_fetch_bytes = input.i32()
      | ("max_fetch_bytes", let input: I64) =>
        max_fetch_bytes = input.i32()
      | ("partition_fetch_max_bytes", let input: I64) =>
        partition_fetch_max_bytes = input.i32()
      | ("client_mode", let input: String) =>
        client_mode = input
      | ("client_name", let input: String) =>
        client_name = input
      | ("producer_compression", let input: String) =>
        compression = input
      | ("topic", let input: String) =>
        topic = input
      | ("consumer_check_crc", let input: None) =>
        check_crc = true
      | ("brokers", let input: String) =>
        brokers = _brokers_from_input_string(input)?
      | ("log_level", let input: String) =>
        log_level = input
      end
    end

    // create kafka config

    match KafkaConfigFactory(client_name, client_mode, topic, brokers,
      log_level, max_produce_buffer_ms, max_message_size, compression,
      produce_acks, check_crc, produce_timeout_ms, max_inflight_requests,
      fetch_interval, min_fetch_bytes, max_fetch_bytes,
      partition_fetch_max_bytes, out)
    | let kc: KafkaConfig val =>
      kc
    | let kce: KafkaConfigError =>
      @printf[U32]("%s\n".cstring(), kce.message().cstring())
      error
    else
      error
    end

  fun _brokers_from_input_string(inputs: String): Array[(String, I32)] val ? =>
    let brokers = recover trn Array[(String, I32)] end

    for input in inputs.split(",").values() do
      let i = input.split(":")
      let host = i(0)?
      let port: I32 = try i(1)?.i32()? else 9092 end
      brokers.push((host, port))
    end

    consume brokers

  fun _topics_from_input_string(inputs: String): Array[String] val =>
    let topics = recover trn Array[String] end

    for input in inputs.split(",").values() do
      topics.push(input)
    end

    consume topics

primitive KafkaConfigFactory
  fun apply(client_name': String,
    client_mode': String,
    topic': String,
    brokers': Array[(String, I32)] val,
    log_level': String,
    max_produce_buffer_ms': U64,
    max_message_size': I32,
    producer_compression': String,
    produce_acks': I16,
    check_crc': Bool,
    produce_timeout_ms': I32,
    max_inflight_requests': USize,
    fetch_interval': U64,
    min_fetch_bytes': I32,
    max_fetch_bytes': I32,
    partition_fetch_max_bytes': I32,
    out': OutStream):
    (KafkaConfig val | KafkaConfigError)
  =>
    let log_level = match log_level'
      | "Fine" => Fine
      | "Info" => Info
      | "Warn" => Warn
      | "Error" => Error
      else
        return KafkaConfigError("Error! Invalid log_level: " + log_level')
      end

    let logger = StringLogger(log_level, out')

    if (brokers'.size() == 0) or (topic' == "") then
      return KafkaConfigError("Error! Either brokers is empty or topics is empty!")
    end

    if produce_acks' < -1 then
      return KafkaConfigError("Error! Invalid produce_acks value: " + produce_acks'.string())
    end

    let client_mode = match client_mode'
      | "consumer" => KafkaConsumeOnly
      | "producer" => KafkaProduceOnly
      else
        return KafkaConfigError("Error! Invalid client mode: " + client_mode')
      end

    let producer_compression = match producer_compression'
      | "gzip" => KafkaGzipTopicCompression
      | "snappy" => KafkaSnappyTopicCompression
      | "lz4" => KafkaLZ4TopicCompression
      | "none" => KafkaNoTopicCompression
      else
        return KafkaConfigError("Error! Invalid producer compression: " + producer_compression')
      end

    recover
      let kc = KafkaConfig(logger, client_name' + " " + topic' where
        produce_acks' = produce_acks',
        max_message_size' = max_message_size',
        max_produce_buffer_ms' = max_produce_buffer_ms',
        produce_timeout_ms' = produce_timeout_ms',
        max_inflight_requests' = max_inflight_requests',
        fetch_interval' = fetch_interval',
        min_fetch_bytes' = min_fetch_bytes',
        max_fetch_bytes' = max_fetch_bytes',
        partition_fetch_max_bytes' = partition_fetch_max_bytes',
        check_crc' = check_crc')

      // add topic config to consumer
      kc.add_topic_config(topic', client_mode where compression = producer_compression)

      for (host, port) in brokers'.values() do
        kc.add_broker(host, port)
      end

      kc
    end

