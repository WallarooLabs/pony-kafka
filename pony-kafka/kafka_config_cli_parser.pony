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

class KafkaConfigCLIParser
  let _pre: String
  let _out: OutStream
  let _client_mode: KafkaTopicType
  let _client_name: String

  new create(out: OutStream, client_mode: KafkaTopicType =
    KafkaProduceAndConsume, client_name: String = "Kafka Client", prefix:
    (String | None) = None)
  =>
    _out = out
    _pre = _prefix(prefix)
    _client_mode = client_mode
    _client_name = client_name

  fun tag _prefix(prefix: (String | None) = None): String =>
    match prefix
    | let s: String => _clean_string(s) + "_"
    else ""
    end

  fun tag _clean_string(s: String): String =>
    let valid_chars = "abcdefghijklmnopqrstuvwxyz0123456789_"
    let temp_s = recover trn String end

    for c in s.lower().values() do
      if valid_chars.contains(String.>push(c)) then
        temp_s.push(c)
      end
    end

    temp_s

  fun opts(): Array[(String, (None | String), ArgumentType, (Required |
    Optional), String)]
  =>
    // items in the tuple are: Argument Name, Argument Short Name,
    //   Argument Type, Required or Optional, Help Text
    let opts_array = Array[(String, (None | String), ArgumentType, (Required |
      Optional), String)]

    opts_array.push((_pre + "topic", None, StringArgument, Required,
      "Kafka topic to consume from"))
    opts_array.push((_pre + "brokers", None, StringArgument, Required,
      "Initial brokers to connect to. Format: 'host:port,host:port,...'"))
    opts_array.push((_pre + "log_level", None, StringArgument, Required,
      "Log Level. Fine, Info, Warn, Error. (Warn)"))


    // Consumer specific options
    if (_client_mode is KafkaProduceAndConsume) or
      (_client_mode is KafkaConsumeOnly) then
      opts_array.push((_pre + "consumer_check_crc", None, None, Optional,
        "(Consumer) Verify CRC of consumed messages to check against corruption. Default is to not verify CRCs."))
      opts_array.push((_pre + "fetch_interval", None, I64Argument,
        Required, "(Consumer) How often to fetch data from kafka in nanoseconds (only applies if we didn't receive data on our previous fetch attempt) (100000000)"))
      opts_array.push((_pre + "min_fetch_bytes", None, I64Argument,
        Required, "(Consumer) Minimum # of bytes for Kafka broker to attempt to return on a fetch request (1)"))
      opts_array.push((_pre + "max_fetch_bytes", None, I64Argument,
        Required, "(Consumer) Maximum # of bytes for Kafka broker to attempt to return on a fetch request (100000000)"))
      opts_array.push((_pre + "partition_fetch_max_bytes", None, I64Argument,
        Required, "(Consumer) Maximum # of bytes to request for a single topic/partition at a time (1048576)"))
      opts_array.push((_pre + "offset_default", None, StringArgument, Required,
      "Set the Consumer offset default to be used when not overridden. "
        + "Valid consumer offset values are positive integers (absolute offset), negative integers (relative from END offset at time of offsets refresh), END, START. (START)"))

    end

    opts_array.push((_pre + "partitions", None, StringArgument, Required,
      "Comma and colon separated list of partitions:offset pairs. Consumer offset can be left off and the default will be used."
        + "Valid consumer offset values are positive integers (absolute offset), negative integers (relative from END offset at time of offsets refresh), END, START). Example arguments: '0,2,4' or '1:5,3:-5,5,7:END,9:START'. Defaults to all partitions if partition list provided any other partitions in kafka will be ignore and any partitions specified that don't exist in kafka will be treated as an error"))

    // Producer specific options
    if (_client_mode is KafkaProduceAndConsume) or
      (_client_mode is KafkaProduceOnly) then
      opts_array.push((_pre + "produce_timeout_ms", None, I64Argument,
        Required, "(Producer) Max # ms kafka can wait for all acks required (100)"))
      opts_array.push((_pre + "max_produce_buffer_ms", None, I64Argument,
        Required, "(Producer) # ms to buffer for producing to kafka (0)"))
      opts_array.push((_pre + "max_produce_message_size", None, I64Argument, Required,
        "(Producer) Max message size in bytes for producing to kafka after buffering (1000000)"))
      opts_array.push((_pre + "max_inflight_requests", None, I64Argument, Required,
        "(Producer) Max # of in flight requests per broker connection (mainly applies to produce requests) (1000000)"))
      opts_array.push((_pre + "produce_acks", None, I64Argument, Required,
        "(Producer) # of acks to wait for.. -1 = all replicas; 0 = no replicas; N = N replicas. (-1)"))
      opts_array.push((_pre + "producer_compression", None, StringArgument, Required,
        "(Producer) Compression codec to use when producing messages to Kafka. gzip/snappy/lz4/none. (none)"))
    end

    opts_array

  fun print_usage() =>
    for (long, short, arg_type, arg_req, help) in opts().values() do
      let short_str = match short
             | let s: String => "/-" + s
             else "" end

      let arg_type_str = match arg_type
             | StringArgument => "(String)"
             | I64Argument => "(Integer)"
             | F64Argument => "(Float)"
             else "" end

      _out.print("--" + long + short_str + "       " + arg_type_str + "    "
        + help)
    end

  fun parse(args: Array[String] val): KafkaConfig iso^ ? =>
    let kco = parse_options(args)?

    // create kafka config
    let kconf = KafkaConfigFactory(consume kco, _out)

    match consume kconf
    | let kc: KafkaConfig iso =>
      kc
    | let kce: KafkaConfigError =>
      @printf[U32]("%s\n".cstring(), kce.message().cstring())
      error
    end

  fun parse_options(args: Array[String] val): KafkaConfigOptions iso^ ? =>
    var log_level = "Warn"

    var topic = ""
    var brokers = recover val Array[(String, I32)] end

    var max_inflight_requests: USize = 1_000_000
    var max_message_size: I32 = 1_000_000
    var max_produce_buffer_ms: U64 = 0
    var compression = "none"
    var produce_acks: I16 = -1
    var check_crc: Bool = false
    var produce_timeout_ms: I32 = 100
    var fetch_interval: U64 = 100_000_000
    var min_fetch_bytes: I32 = 1
    var max_fetch_bytes: I32 = 100_000_000
    var partition_fetch_max_bytes: I32 = 1_048_576
    var offset_default: String = "START"
    var partitions: String = ""

    let options = Options(args, false)

    for (long, short, arg_type, arg_req, _) in opts().values() do
      options.add(long, short, arg_type, arg_req)
    end

    // TODO: implement all the other options that kafka client supports
    for option in options do
      match option
      | (_pre + "max_produce_buffer_ms", let input: I64) =>
        max_produce_buffer_ms = input.u64()
      | (_pre + "max_produce_message_size", let input: I64) =>
        max_message_size = input.i32()
      | (_pre + "max_inflight_requests", let input: I64) =>
        max_inflight_requests = input.usize()
      | (_pre + "produce_timeout_ms", let input: I64) =>
        produce_timeout_ms = input.i32()
      | (_pre + "produce_acks", let input: I64) =>
        produce_acks = input.i16()
      | (_pre + "fetch_interval", let input: I64) =>
        fetch_interval = input.u64()
      | (_pre + "min_fetch_bytes", let input: I64) =>
        min_fetch_bytes = input.i32()
      | (_pre + "max_fetch_bytes", let input: I64) =>
        max_fetch_bytes = input.i32()
      | (_pre + "partition_fetch_max_bytes", let input: I64) =>
        partition_fetch_max_bytes = input.i32()
      | (_pre + "producer_compression", let input: String) =>
        compression = input
      | (_pre + "offset_default", let input: String) =>
        offset_default = input
      | (_pre + "partitions", let input: String) =>
        partitions = input
      | (_pre + "topic", let input: String) =>
        topic = input
      | (_pre + "consumer_check_crc", let input: None) =>
        check_crc = true
      | (_pre + "brokers", let input: String) =>
        brokers = _brokers_from_input_string(input)?
      | (_pre + "log_level", let input: String) =>
        log_level = input
      end
    end

    KafkaConfigOptions(_client_name, _client_mode, topic, brokers,
      log_level, max_produce_buffer_ms, max_message_size, compression,
      produce_acks, check_crc, produce_timeout_ms, max_inflight_requests,
      fetch_interval, min_fetch_bytes, max_fetch_bytes,
      partition_fetch_max_bytes, offset_default, partitions)

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

class val KafkaConfigOptions
  var client_name: String
  var client_mode: KafkaTopicType
  var topic: String
  var brokers: Array[(String, I32)] val
  var log_level: String
  var max_produce_buffer_ms: U64
  var max_message_size: I32
  var producer_compression: String
  var produce_acks: I16
  var check_crc: Bool
  var produce_timeout_ms: I32
  var max_inflight_requests: USize
  var fetch_interval: U64
  var min_fetch_bytes: I32
  var max_fetch_bytes: I32
  var partition_fetch_max_bytes: I32
  var offset_default: String
  var partitions: String

  new iso create(client_name': String,
    client_mode': KafkaTopicType,
    topic': String,
    brokers': Array[(String, I32)] val,
    log_level': String = "Warn",
    max_produce_buffer_ms': U64 = 0,
    max_message_size': I32 = 1_000_000,
    producer_compression': String = "none",
    produce_acks': I16 = -1,
    check_crc': Bool = false,
    produce_timeout_ms': I32 = 100,
    max_inflight_requests': USize = 1_000_000,
    fetch_interval': U64 = 100_000_000,
    min_fetch_bytes': I32 = 1,
    max_fetch_bytes': I32 = 100_000_000,
    partition_fetch_max_bytes': I32 = 1_048_576,
    offset_default': String = "START",
    partitions': String = "")
  =>
    client_name = client_name'
    client_mode = client_mode'
    topic = topic'
    brokers = brokers'
    log_level = log_level'
    max_produce_buffer_ms = max_produce_buffer_ms'
    max_message_size = max_message_size'
    producer_compression = producer_compression'
    produce_acks = produce_acks'
    check_crc = check_crc'
    produce_timeout_ms = produce_timeout_ms'
    max_inflight_requests = max_inflight_requests'
    fetch_interval = fetch_interval'
    min_fetch_bytes = min_fetch_bytes'
    max_fetch_bytes = max_fetch_bytes'
    partition_fetch_max_bytes = partition_fetch_max_bytes'
    offset_default = offset_default'
    partitions = partitions'

primitive KafkaConfigFactory
  fun apply(kco: KafkaConfigOptions,
    out: OutStream):
    (KafkaConfig iso^ | KafkaConfigError)
  =>
    let log_level = match kco.log_level
      | "Fine" => Fine
      | "Info" => Info
      | "Warn" => Warn
      | "Error" => Error
      else
        return KafkaConfigError("Error! Invalid log_level: " + kco.log_level)
      end

    let logger = StringLogger(log_level, out)

    if (kco.brokers.size() == 0) or (kco.topic == "") then
      return KafkaConfigError("Error! Either brokers is empty or topics is empty!")
    end

    if kco.produce_acks < -1 then
      return KafkaConfigError("Error! Invalid produce_acks value: " + kco.produce_acks.string())
    end

    let producer_compression = match kco.producer_compression
      | "gzip" => KafkaGzipTopicCompression
      | "snappy" => KafkaSnappyTopicCompression
      | "lz4" => KafkaLZ4TopicCompression
      | "none" => KafkaNoTopicCompression
      else
        return KafkaConfigError("Error! Invalid producer compression: " + kco.producer_compression)
      end

    let default_start_offset = try _parse_offset(kco.offset_default, None)?
         else
           return KafkaConfigError("Error! Invalid default consumer offset: " + kco.offset_default)
         end

    let partitions = try _parse_partitions(kco.partitions, default_start_offset)?
         else
           return KafkaConfigError("Error! Error parsing partitions/offsets: " + kco.partitions)
         end


    recover
      let kc = KafkaConfig(logger, kco.client_name where
        produce_acks' = kco.produce_acks,
        max_message_size' = kco.max_message_size,
        max_produce_buffer_ms' = kco.max_produce_buffer_ms,
        produce_timeout_ms' = kco.produce_timeout_ms,
        max_inflight_requests' = kco.max_inflight_requests,
        fetch_interval' = kco.fetch_interval,
        min_fetch_bytes' = kco.min_fetch_bytes,
        max_fetch_bytes' = kco.max_fetch_bytes,
        partition_fetch_max_bytes' = kco.partition_fetch_max_bytes,
        check_crc' = kco.check_crc)

      // add topic config to consumer
      kc.add_topic_config(kco.topic, kco.client_mode where compression = producer_compression,
        default_consumer_start_offset' = default_start_offset,
        partitions' = partitions)

      for (host, port) in kco.brokers.values() do
        kc.add_broker(host, port)
      end

      kc
    end

  fun _parse_offset(offset: String, offset_default: (ConsumerRequestOffset | None)): ConsumerRequestOffset ? =>
    match offset.upper()
      | "END" => KafkaOffsetEnd
      | "START" => KafkaOffsetBeginning
      | "" =>
        // no offset specified.. use default if possible
        match offset_default
        | let o: ConsumerRequestOffset => o
        else
          // if no default, throw an error
          error
        end
      | let s: String => s.i64()?
      end

  fun _parse_partitions(partitions: String, offset_default: ConsumerRequestOffset): Array[(KafkaPartitionId, ConsumerRequestOffset)] val ? =>
    let partitions_offsets = recover trn Array[(KafkaPartitionId, ConsumerRequestOffset)] end

    for part in partitions.split(",").values() do
      let i = part.split(":")
      let part_id = i(0)?.i32()?
      let offset_str = try i(1)? else "" end
      let offset = _parse_offset(offset_str, offset_default)?
      partitions_offsets.push((part_id, offset))
    end

    consume partitions_offsets

