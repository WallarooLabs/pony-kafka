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
use "../../pony-kafka/customlogger"
use "../../pony-kafka"
use "random"

actor Main is KafkaClientManager
  var _kc: (KafkaClient tag | None) = None
  let _env: Env
  let _logger: Logger[String] val

  new create(env: Env) =>
    _env = env
    _logger = StringLogger(Warn, env.out)

    // create kafka config
    let kconf =
      recover iso
        let kc = KafkaConfig(_logger, "My Client" where
          use_java_compatible_snappy_compression' = false)
        kc.add_broker("127.0.0.1", 9092)
// uncomment for producer only config
//        kc.add_topic_config("test")

// uncomment for consumer only config and tell kafka to send messages to
// consumers
//        kc.add_topic_config("test", KafkaConsumeOnly where
// consumer_message_handler = recover KafkaRoundRobinConsumerMessageHandler end,
// compression = KafkaGzipTopicCompression)

// producer/consumer config and tell kafka to send messages to consumers
        kc.add_topic_config("test", KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end, compression =
          KafkaSnappyTopicCompression)

        kc
      end


    // create kafka client and register producer
    try
      let kc = KafkaClient(env.root as AmbientAuth, consume kconf, this)
      kc.register_producer(P(_logger))
      _kc = kc
    end

  be receive_kafka_topics_partitions(topic_partitions: Map[String,
    (KafkaTopicType, Set[I32])] val) =>
    match _kc
    | let kc: KafkaClient tag =>
      let consumers = recover iso Array[KafkaConsumer tag] end
      consumers.push(C(_logger, "1"))
      consumers.push(C(_logger, "2"))
      consumers.push(C(_logger, "3"))

      kc.register_consumers("test", consume consumers)
      kc.consumer_resume_all()
    end

  be kafka_client_error(error_report: KafkaErrorReport) =>
    None

// kafka consumer actor
actor C is KafkaConsumer
  let logger: Logger[String]
  let _name: String
  var _i: USize = 1


  new create(logger': Logger[String], name: String) =>
    logger = logger'
    _name = name

  // behavior kafka calls for each message received that should be sent to this
  // actor
  be receive_kafka_message(msg: KafkaMessage val,
    network_received_timestamp: U64)
  =>
    logger(Fine) and logger.log(Fine, "Received kafka message")
    let m = String.from_array(msg.get_value())

    logger.log(Info, "CONSUMER(" + _name + ")-MSG(" + _i.string() +
      "): Received Msg. topic: " + msg.get_topic() + ", partition: " +
      msg.get_partition_id().string() + ", offset: " + msg.get_offset().string()
      + ", value: " + m)

    _i = _i + 1
    msg.send_consume_successful()


// kafka producer actor
actor P is KafkaProducer
  // variable to hold producer mapping for sending requests to broker
  // connections
  var _kafka_producer_mapping: (KafkaProducerMapping ref | None) = None

  let mt: Random = MT
  let logger: Logger[String]

  new create(logger': Logger[String]) =>
    logger = logger'

  fun ref update_producer_mapping(mapping: KafkaProducerMapping):
    (KafkaProducerMapping | None)
  =>
    _kafka_producer_mapping = mapping

  fun ref producer_mapping(): (KafkaProducerMapping | None) =>
    _kafka_producer_mapping

  fun ref _kafka_producer_throttled(topic_mapping: Map[String, Map[I32, I32]]
    val)
  =>
    None

  fun ref _kafka_producer_unthrottled(topic_mapping: Map[String, Map[I32, I32]]
    val, fully_unthrottled: Bool)
  =>
    None

  be kafka_producer_ready() =>
    produce_data()

  be kafka_message_delivery_report(delivery_report: KafkaProducerDeliveryReport)
  =>
    if not (delivery_report.status is ErrorNone) then
      logger(Error) and logger.log(Error, "received delivery report: " +
        delivery_report.string())
    else
      logger(Fine) and logger.log(Fine, "received delivery report: " +
        delivery_report.string())
    end

  // example produce data function
  fun ref produce_data() =>
    logger(Info) and logger.log(Info, "asked to produce data")
    let d = generate_data(100)
    let d2 = generate_data(1000)

    logger.log(Info, "PRODUCER: Sending messages to topic: " + "test")
    for (o, v, k) in d.values() do
      try
        logger.log(Info, "PRODUCER: Sending message: " + (v as String))
      end
    end
    try
      let ret = (_kafka_producer_mapping as KafkaProducerMapping
        ref).send_topic_messages("test", d)
      if ret isnt None then error end
    else
      logger(Error) and logger.log(Error, "error sending messages to brokers")
    end

    logger.log(Info, "PRODUCER: Sending messages to topic: " + "test")
    for (o, v, k) in d2.values() do
      try
        logger.log(Info, "PRODUCER: Sending message: " + (v as String))
      end
      try
        let ret = (_kafka_producer_mapping as KafkaProducerMapping
          ref).send_topic_message("test", o, v, k)
        if ret isnt None then error end
      else
        logger(Error) and logger.log(Error, "error sending messages to brokers")
      end
    end

  // generate data
  fun ref generate_data(start: USize = 0): Array[(Any tag, (ByteSeq |
    Array[ByteSeq] val), (None | ByteSeq | Array[ByteSeq] val))]
  =>
    let msgs = Array[(Any tag, (ByteSeq | Array[ByteSeq] val), (None | ByteSeq |
       Array[ByteSeq] val))]

    var num_msgs = mt.int(11)
    var i: USize = start
    var a = "(" + i.string() + ") - 2begin"
    msgs.push((a, a, None))
    i = i + 1

    if num_msgs > 9 then
      a = "(" + i.string() + ") - 2this is anothr test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 8 then
      a = "(" + i.string() + ") - 2this is and another test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 7 then
      a = "(" + i.string() + ") - 2this is anothr test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 6 then
      a = "(" + i.string() + ") - 2this is and another test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 5 then
      a = "(" + i.string() + ") - 2this is anothr test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 4 then
      a = "(" + i.string() + ") - 2this is and another test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 3 then
      a = "(" + i.string() + ") - 2this is anothr test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 2 then
      a = "(" + i.string() + ") - 2this is and another test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 1 then
      a = "(" + i.string() + ") - 2this is anothr test"
      msgs.push((a, a, None))
      i = i + 1
    end

    if num_msgs > 0 then
      a = "(" + i.string() + ") - 2this is and another test"
      msgs.push((a, a, None))
      i = i + 1
    end

    a = "(" + i.string() + ") - 2done"
    msgs.push((a, a, None))
    i = i + 1

    msgs

