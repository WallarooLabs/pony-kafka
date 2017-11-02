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

use "net"
use "collections"
use "time"
use "options"
use "../../pony-kafka/customlogger"
use "../../pony-kafka"

actor Main is KafkaClientManager
  var _kc: (KafkaClient tag | None) = None
  let _env: Env
  let logger: Logger[String] val
  var _client_mode: String = ""
  var _topic: String = ""
  var _num_messages: USize = 1_000_000
  var _message_size: USize = 1_024

  new create(env: Env) =>
    _env = env

    var help: Bool = false

    let options = Options(env.args, false)

    for (long, short, arg_type, arg_req, _) in opts().values() do
      options.add(long, short, arg_type, arg_req)
    end

    // TODO: implement all the other options that kafka client supports
    for option in options do
      match option
      | ("help", let input: None) =>
        help = true
      | ("client_mode", let input: String) =>
        _client_mode = input
      | ("num_messages", let input: I64) =>
        _num_messages = input.usize()
      | ("produce_message_size", let input: I64) =>
        _message_size = input.usize()
      end
    end

    if help or (env.args.size() == 1) then
      logger = StringLogger(Warn, env.out)
      print_usage(env.out)
      KafkaConfigCLIParser.print_usage(env.out)
      return
    end


    let kconf =
      try
        KafkaConfigCLIParser(env.args, env.out)?
      else
        logger = StringLogger(Warn, env.out)
        print_usage(env.out)
        KafkaConfigCLIParser.print_usage(env.out)
        return
      end

    logger = kconf.logger

    try
      _topic = kconf.topics.keys().next()?
    else
      @printf[I32]("Error getting topic from config\n".cstring())
      return
    end

    // create kafka client and register producer
    let kc = try KafkaClient(env.root as AmbientAuth, kconf, this) else return end
    _kc = kc
    if _client_mode == "producer" then
      kc.register_producer(P(kc, logger, _topic, _num_messages, _message_size))
    end

  fun tag opts(): Array[(String, (None | String), ArgumentType, (Required |
    Optional), String)]
  =>
    // items in the tuple are: Argument Name, Argument Short Name,
    //   Argument Type, Required or Optional, Help Text
    let opts_array = Array[(String, (None | String), ArgumentType, (Required |
      Optional), String)]

    opts_array.push(("help", "h", None, Optional,
      "print help"))
    opts_array.push(("client_mode", None, StringArgument, Required,
      "producer or consumer"))
    opts_array.push(("produce_messages", None, I64Argument, Required,
      "number of messages to produce (1,000,000)"))
    opts_array.push(("produce_message_size", None, I64Argument, Required,
      "size of messages to produce (1024)"))

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

  be receive_kafka_topics_partitions(topic_partitions: Map[String,
    (KafkaTopicType, Set[I32])] val) =>
//    @printf[I32]("Received topics/partitions from client\n".cstring())

    if _client_mode == "consumer" then
      match _kc
      | let kc: KafkaClient tag =>
        let consumers = recover iso Array[KafkaConsumer tag] end
        consumers.push(C(kc, logger, "1", _num_messages))

        kc.register_consumers(_topic, consume consumers)
        kc.consumer_resume_all()
        @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Consuming data\n").cstring())
      end
    end

  be kafka_client_error(error_report: KafkaErrorReport) =>
    @printf[I32]("Kafka client error\n".cstring())

// kafka consumer actor
actor C is KafkaConsumer
  let logger: Logger[String]
  let _name: String
  let num_msgs: USize
  var num_msgs_consumed: USize = 0
  let _kc: KafkaClient


  new create(kc': KafkaClient, logger': Logger[String], name: String, num_msgs': USize = 1_000_000) =>
    _kc = kc'
    logger = logger'
    _name = name
    num_msgs = num_msgs'

  // behavior kafka calls for each message received that should be sent to this
  // actor
  be receive_kafka_message(msg: KafkaMessage val,
    network_received_timestamp: U64)
  =>
    num_msgs_consumed = num_msgs_consumed + 1
    msg.send_consume_successful()

//    if (num_msgs_consumed % 100000) == 0 then
//      @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Received " + num_msgs_consumed.string() + " messages so far\n").cstring())
//    end

    if num_msgs_consumed == num_msgs then
      @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Received " + num_msgs_consumed.string() + " messages as requested\n").cstring())
      @printf[I32]("Shutting down\n".cstring())
      _kc.dispose()
    end

// kafka producer actor
actor P is KafkaProducer
  // variable to hold producer mapping for sending requests to broker
  // connections
  var _kafka_producer_mapping: (KafkaProducerMapping ref | None) = None

  let logger: Logger[String]
  let num_msgs: USize
  var num_msgs_produced: USize = 0
  var num_msgs_produced_acked: USize = 0
  let msg_size: USize
  let topic: String
  var _throttled: Bool = false
  let _kc: KafkaClient

  new create(kc': KafkaClient, logger': Logger[String], topic': String, num_msgs': USize = 1_000_000, msg_size': USize = 1_024) =>
    _kc = kc'
    logger = logger'
    topic = topic'
    num_msgs = num_msgs'
    msg_size = msg_size'

    @printf[I32](("Requested to produce " + num_msgs.string() + " of " + msg_size.string() + " bytes.\n").cstring())

  fun ref update_producer_mapping(mapping: KafkaProducerMapping):
    (KafkaProducerMapping | None)
  =>
//    @printf[I32]("Producer mapping updated\n".cstring())
    _kafka_producer_mapping = mapping

  fun ref producer_mapping(): (KafkaProducerMapping | None) =>
    _kafka_producer_mapping

  fun ref _kafka_producer_throttled(topic_mapping: Map[String, Map[I32, I32]]
    val)
  =>
//    @printf[I32]("Producer throttled\n".cstring())
    if not _throttled then
      _throttled = true
    end

  fun ref _kafka_producer_unthrottled(topic_mapping: Map[String, Map[I32, I32]]
    val, fully_unthrottled: Bool)
  =>
//    @printf[I32]("Producer unthrottled\n".cstring())
    if _throttled then
      _throttled = false
      match _kafka_producer_mapping
      | let p: KafkaProducerMapping => produce_data()
      end
    end

  be kafka_producer_ready() =>
    @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Producing data\n").cstring())
    produce_data()

  be kafka_message_delivery_report(delivery_report: KafkaProducerDeliveryReport)
  =>
    num_msgs_produced_acked = num_msgs_produced_acked + 1
    if not (delivery_report.status is ErrorNone) then
      logger(Error) and logger.log(Error, "received delivery report: " +
        delivery_report.string())
    else
      logger(Fine) and logger.log(Fine, "received delivery report: " +
        delivery_report.string())
    end
    if num_msgs_produced_acked == num_msgs then
      @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Received acks for all messages produced\n").cstring())
      @printf[I32]("Shutting down\n".cstring())
      _kc.dispose()
    end

  // example produce data function
  be produce_data() =>
    if _throttled then
//      @printf[I32](("Stopping producing data because throttled. Produced so far: " + num_msgs_produced.string() + "\n").cstring())
      return
    end

    if num_msgs_produced < num_msgs then

      let o = recover val Array[U8].>undefined(msg_size) end
      let v = o
      let k = None

      try
        let ret = (_kafka_producer_mapping as KafkaProducerMapping
          ref).send_topic_message(topic, o, v, k)
        if ret isnt None then error end
      else
        logger(Error) and logger.log(Error, "error sending message to brokers")
      end
      num_msgs_produced = num_msgs_produced + 1
      produce_data()
    else
      @printf[I32]((Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S") + ": Done producing data\n").cstring())
    end

