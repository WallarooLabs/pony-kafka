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
use "compression"
use "customlogger"
use "random"

// type for whether a topic config is a consume only, produce only, or both
type KafkaTopicType is (KafkaConsumeOnly | KafkaProduceOnly |
  KafkaProduceAndConsume)

primitive KafkaConsumeOnly
primitive KafkaProduceOnly
primitive KafkaProduceAndConsume

type KafkaTopicCompressionType is (KafkaNoTopicCompression |
  KafkaGzipTopicCompression | KafkaSnappyTopicCompression |
  KafkaLZ4TopicCompression)

primitive KafkaNoTopicCompression
  fun box apply(): I8 => 0
primitive KafkaGzipTopicCompression
  fun box apply(): I8 => 1
primitive KafkaSnappyTopicCompression
  fun box apply(): I8 => 2
primitive KafkaLZ4TopicCompression
  fun box apply(): I8 => 3


primitive _KafkaProducerAuth

// trait for a kafka client manager (this will get adminitrative messages
trait KafkaClientManager
  be receive_kafka_topics_partitions(topic_partitions: Map[String,
    (KafkaTopicType, Set[I32])] val)

  // unrecoverable client error
  be kafka_client_error(error_report: KafkaErrorReport)

// trait for a kafka consumer
trait KafkaConsumer
  be receive_kafka_message(msg: KafkaMessage val,
    network_received_timestamp: U64)

// trait for a kafka producer
trait KafkaProducer
  fun ref update_producer_mapping(mapping: KafkaProducerMapping):
    (KafkaProducerMapping | None)

  fun ref producer_mapping(): (KafkaProducerMapping | None)

  // called by kafka client to let producers know of updated producer mapping
  be _create_producer_mapping(mapping: KafkaProducerMapping iso) =>
    let old = update_producer_mapping(consume mapping)

    match old
    | let pm: KafkaProducerMapping =>
      pm.conf.logger(Error) and
      pm.conf.logger.log(Error, "Creating producer mapping when it has " +
      "already been created. This should never happen.")
    else
      kafka_producer_ready()
    end

  be _update_brokers_and_topic_mapping(brokers: Map[I32, (_KafkaBroker val,
    KafkaBrokerConnection tag)] val, topic_mapping: Map[String, Map[I32, I32]]
    val)
  =>
    let pm = producer_mapping()
    match pm
    | let pm': KafkaProducerMapping =>
      pm'.update_brokers_and_topic_mapping(brokers, topic_mapping)
    end

  be kafka_producer_ready()

  be kafka_message_delivery_report(delivery_report: KafkaProducerDeliveryReport)

  be _kafka_producer_throttle(topic_mapping: Map[String, Map[I32, I32]] val,
    ack_requested: Bool, client: KafkaClient, p: KafkaProducer tag)
  =>
    let pm = producer_mapping()
    match pm
    | let pm': KafkaProducerMapping => pm'.update_topic_mapping(topic_mapping)
    end

    if ack_requested then
      client.throttle_ack(topic_mapping, this, p)
    end

    _kafka_producer_throttled(topic_mapping)

  fun ref _kafka_producer_throttled(topic_mapping: Map[String, Map[I32, I32]]
    val)

  be _kafka_producer_unthrottle(topic_mapping: Map[String, Map[I32, I32]] val,
    ack_requested: Bool, client: KafkaClient, p: KafkaProducer tag,
    fully_unthrottled: Bool)
  =>
    let pm = producer_mapping()
    match pm
    | let pm': KafkaProducerMapping => pm'.update_topic_mapping(topic_mapping)
    end

    if ack_requested then
      client.unthrottle_ack(topic_mapping, this, p)
    end

    _kafka_producer_unthrottled(topic_mapping, fully_unthrottled)

  fun ref _kafka_producer_unthrottled(topic_mapping: Map[String, Map[I32, I32]]
    val, fully_unthrottled: Bool)

// trait for a class that can handle kafka messages on produce (assigning
// partition ids on send)
trait KafkaProducerMessageHandler
  fun ref apply(key: (ByteSeq | Array[ByteSeq] val | None), key_size: USize,
    num_partitions: I32): (I32 | None)

  fun clone(): KafkaProducerMessageHandler iso^

// trait for a class that can handle kafka messages on consume (distributing
// messages to consumers on receive)
trait KafkaConsumerMessageHandler
  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)

  fun clone(): KafkaConsumerMessageHandler iso^

class KafkaProducerRoundRobinPartitioner is KafkaProducerMessageHandler
  var _x: I32 = 0

  new create() => None

  fun ref apply(key: (ByteSeq | Array[ByteSeq] val | None), key_size: USize,
    num_partitions: I32): I32
  =>
    _x = (_x + 1) % num_partitions

  fun clone(): KafkaProducerMessageHandler iso^ =>
    recover iso KafkaProducerRoundRobinPartitioner end

class KafkaProducerRandomPartitioner is KafkaProducerMessageHandler
  let _mt: MT
  let _seed: U64

  new create(seed: U64 = 5489)
  =>
    _seed = seed
    _mt = MT(_seed)

  fun ref apply(key: (ByteSeq | Array[ByteSeq] val | None), key_size: USize,
    num_partitions: I32): I32
  =>
    (_mt.next() % num_partitions.u64()).i32()

  fun clone(): KafkaProducerMessageHandler iso^ =>
    recover iso KafkaProducerRandomPartitioner(_seed) end


class KafkaProducerHashPartitioner is KafkaProducerMessageHandler
  new create() => None

  fun ref apply(key: (ByteSeq | Array[ByteSeq] val), key_size: USize,
    num_partitions: I32): I32
  =>
    // TODO: replace Crc32 with Murmur2 to be compatible with default partitions
    // for java client?
    // Crc32 shoule be compatible with C client
    match key
    | let a: Array[U8] val => (Crc32.crc32(a) % num_partitions.usize()).i32()
    | let s: String =>
      let a = s.array(); (Crc32.crc32(a) % num_partitions.usize()).i32()
    | let arr: Array[ByteSeq] val => Crc32.crc32_array(arr).i32()
    else
      -1 // this should never happen
    end

  fun ref apply(key: None, key_size: USize, num_partitions: I32): I32 =>
    0 // always return partition 0 if None

  fun clone(): KafkaProducerMessageHandler iso^ =>
    recover iso KafkaProducerHashPartitioner end

class KafkaProducerHashRoundRobinPartitioner is KafkaProducerMessageHandler
  let rr_partitioner: KafkaProducerRoundRobinPartitioner =
    KafkaProducerRoundRobinPartitioner
  let hash_partitioner: KafkaProducerHashPartitioner =
    KafkaProducerHashPartitioner

  new create() => None

  fun ref apply(key: (ByteSeq | Array[ByteSeq] val), key_size: USize,
    num_partitions: I32): I32
  =>
    if key_size > 0 then
      hash_partitioner(key, key_size, num_partitions)
    else
      rr_partitioner(key, key_size, num_partitions)
    end

  fun ref apply(key: None, key_size: USize, num_partitions: I32): I32 =>
    rr_partitioner(key, key_size, num_partitions)

  fun clone(): KafkaProducerMessageHandler iso^ =>
    recover iso KafkaProducerHashRoundRobinPartitioner end

class KafkaRoundRobinConsumerMessageHandler is KafkaConsumerMessageHandler
  var _x: USize = 0

  new create() => None

  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)
  =>
    try consumers(_x = (_x + 1) % consumers.size()) end

  fun clone(): KafkaConsumerMessageHandler iso^ =>
    recover iso KafkaRoundRobinConsumerMessageHandler end

class KafkaRandomConsumerMessageHandler is KafkaConsumerMessageHandler
  let _mt: MT
  let _seed: U64

  new create(seed: U64 = 5489) =>
    _seed = seed
    _mt = MT(_seed)

  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)
  =>
    try consumers(_mt.next().usize() % consumers.size()) end

  fun clone(): KafkaConsumerMessageHandler iso^ => recover iso
    KafkaRandomConsumerMessageHandler(_seed) end

class KafkaPartitionConsumerMessageHandler is KafkaConsumerMessageHandler
  new create() => None

  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)
  =>
    try consumers(msg.get_partition_id().usize() % consumers.size()) end

  fun clone(): KafkaConsumerMessageHandler iso^ =>
    recover iso KafkaPartitionConsumerMessageHandler end

class KafkaHashConsumerMessageHandler is KafkaConsumerMessageHandler
  new create() => None

  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)
  =>
    let key = msg.get_key()
    try
      match key
      | let k: Array[U8] val => consumers(@ponyint_hash_block[U64](k.cpointer(),
         k.size()).usize() % consumers.size())
      else
        consumers(0) // always return first consumer
      end
    end

  fun clone(): KafkaConsumerMessageHandler iso^ =>
    recover iso KafkaHashConsumerMessageHandler end

class KafkaHashRoundRobinConsumerMessageHandler is KafkaConsumerMessageHandler
  let rr_handler: KafkaRoundRobinConsumerMessageHandler
  let hash_handler: KafkaHashConsumerMessageHandler

  new create() =>
    rr_handler = KafkaRoundRobinConsumerMessageHandler
    hash_handler = KafkaHashConsumerMessageHandler

  fun ref apply(consumers: Array[KafkaConsumer tag] val, msg: KafkaMessage val):
     (KafkaConsumer tag | None)
  =>
    let key = msg.get_key()
    match key
    | let a: Array[U8] val =>
      if a.size() > 0 then
        hash_handler(consumers, msg)
      else
        rr_handler(consumers, msg)
      end
    else
      rr_handler(consumers, msg)
    end

  fun clone(): KafkaConsumerMessageHandler iso^ =>
    recover iso KafkaHashRoundRobinConsumerMessageHandler end

class val KafkaErrorReport
  let status: KafkaError
  let topic: String
  let partition: I32

  new val create(status': KafkaError, topic': String, partition': I32)
  =>
    status = status'
    topic = topic'
    partition = partition'

  fun string(): String =>
    "KafkaProducerDeliveryReport: [ "
      + "status = " + status.string()
      + ", topic = " + topic
      + ", partition = " + partition.string()
      + " ]\n"

class val KafkaProducerDeliveryReport
  let status: KafkaError
  let topic: String
  let partition: I32
  let first_offset_assigned: I64
  let timestamp: (I64 | None)
  let opaque: Any tag

  new val create(status': KafkaError, topic': String, partition': I32,
    first_offset_assigned': I64, timestamp': (I64 | None), opaque': Any tag)
  =>
    status = status'
    topic = topic'
    partition = partition'
    first_offset_assigned = first_offset_assigned'
    timestamp = timestamp'
    opaque = opaque'

  fun string(): String =>
    "KafkaProducerDeliveryReport: [ "
      + "status = " + status.string()
      + ", topic = " + topic
      + ", partition = " + partition.string()
      + ", first_offset_assigned = " + first_offset_assigned.string()
      + ", timestamp = " + timestamp.string()
      + " ]\n"

// kafka config class to encapsulate all config information passed to the kafka
// client (and passed along to broker connections)
// TODO: needs to be extended to take additional details (model after main kafka
// java or c client)
class KafkaConfig
  let client_name: String
  let consumer_topics: Set[String] = consumer_topics.create()
  let producer_topics: Set[String] = producer_topics.create()
  let topics: Map[String, KafkaTopicConfig] = topics.create()
  let brokers: Set[_KafkaBroker] = brokers.create()
  var replica_id: I32 = -1
  let fetch_interval: U64
  let refresh_metadata_interval: U64
  let min_fetch_bytes: I32
  let max_fetch_bytes: I32
  let produce_acks: I16
  let produce_timeout_ms: I32
  let logger: Logger[String]
  let use_java_compatible_snappy_compression: Bool
  let max_inflight_requests: USize
  let partition_fetch_max_bytes: I32
  let max_message_size: I32
  let max_produce_buffer_time: U64
  let max_produce_buffer_messages: U64

  new create(logger': Logger[String], client_name': String,
    fetch_interval': U64 = 100_000_000, min_fetch_bytes': I32 = 1,
    max_fetch_bytes': I32 = 32768, produce_acks': I16 = 1,
    produce_timeout_ms': I32 = 100,
    use_java_compatible_snappy_compression': Bool = false,
    max_inflight_requests': USize = 5, partition_fetch_max_bytes': I32 = 32768,
    max_message_size': I32 = 1000000,
    refresh_metadata_interval': U64 = 300_000_000_000,
    max_produce_buffer_ms': U64 = 0,
    max_produce_buffer_messages': U64 = 0)
  =>
    client_name = client_name'
    fetch_interval = fetch_interval'
    min_fetch_bytes = min_fetch_bytes'
    max_fetch_bytes = max_fetch_bytes'
    produce_acks = produce_acks'
    produce_timeout_ms = produce_timeout_ms'
    logger = logger'
    use_java_compatible_snappy_compression =
      use_java_compatible_snappy_compression'
    max_inflight_requests = max_inflight_requests'
    partition_fetch_max_bytes = partition_fetch_max_bytes'
    max_message_size = max_message_size'
    refresh_metadata_interval = refresh_metadata_interval'
    max_produce_buffer_time = max_produce_buffer_ms' * 1_000_000
    max_produce_buffer_messages = max_produce_buffer_messages'

  fun ref _set_replica_id(replica_id': I32) =>
    replica_id = replica_id'

  fun ref add_broker(host: String, port: I32 = 9092) =>
    brokers.set(_KafkaBroker(-1, host, port))

  fun ref add_topic_config(topic_name: String,
    role: KafkaTopicType = KafkaProduceOnly,
    producer_message_handler: KafkaProducerMessageHandler val
    = recover val KafkaProducerHashRoundRobinPartitioner end,
    consumer_message_handler: KafkaConsumerMessageHandler val = recover val
    KafkaRoundRobinConsumerMessageHandler end,
    compression: KafkaTopicCompressionType = KafkaNoTopicCompression)
  =>
    let topic_config = KafkaTopicConfig(topic_name, role,
      producer_message_handler, consumer_message_handler, compression)

    topics(topic_config.topic_name) = topic_config

    if (topic_config.role is KafkaProduceAndConsume)
      or (topic_config.role is KafkaConsumeOnly) then
      consumer_topics.set(topic_config.topic_name)
    end

    if (topic_config.role is KafkaProduceAndConsume)
      or (topic_config.role is KafkaProduceOnly) then
      producer_topics.set(topic_config.topic_name)
    end

// topic config class
// TODO: needs to be extended to take additional details like which partitions
// to read from
class KafkaTopicConfig is Equatable[KafkaTopicConfig box]
  let topic_name: String
  let producer_message_handler: KafkaProducerMessageHandler val
  let consumer_message_handler: KafkaConsumerMessageHandler val
  let role: KafkaTopicType
  let compression: KafkaTopicCompressionType

  new create(topic_name': String, role': KafkaTopicType = KafkaProduceOnly,
    producer_message_handler': KafkaProducerMessageHandler val = recover val
    KafkaProducerHashRoundRobinPartitioner end,
    consumer_message_handler': KafkaConsumerMessageHandler val = recover val
    KafkaRoundRobinConsumerMessageHandler end,
    compression': KafkaTopicCompressionType = KafkaNoTopicCompression)
  =>
    topic_name = topic_name'
    role = role'
    compression = compression'

    consumer_message_handler = consumer_message_handler'

    producer_message_handler = producer_message_handler'

  fun hash(): U64 =>
    topic_name.hash()

  fun eq(that: KafkaTopicConfig box): Bool =>
    (topic_name == that.topic_name)

// kafka producer mapping class is responsible for taking messages from actors
// and sending them to the appropriate broker connections for transmitting to
// kafka brokers
class KafkaProducerMapping
  let conf: KafkaConfig val
  var topic_mapping: Map[String, Map[I32, I32]] val
  var brokers: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val
  let topic_partitioners: Map[String, KafkaProducerMessageHandler] =
    topic_partitioners.create()
  let _auth: _KafkaProducerAuth = _KafkaProducerAuth
  let _producer: KafkaProducer tag

  new create(conf': KafkaConfig val,
    topic_mapping': Map[String, Map[I32, I32]] val,
    brokers': Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val,
    producer: KafkaProducer tag)
  =>
    conf = conf'
    _producer = producer
    topic_mapping = topic_mapping'
    brokers = brokers'
    for (topic, tc) in conf.topics.pairs() do
      if conf.producer_topics.contains(topic) then
        topic_partitioners(topic) = tc.producer_message_handler.clone()
      end
    end

  fun ref update_brokers_and_topic_mapping(
     brokers': Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val,
     topic_mapping': Map[String, Map[I32, I32]] val)
  =>
    brokers = brokers'
    update_topic_mapping(topic_mapping')

  fun ref update_topic_mapping(topic_mapping': Map[String, Map[I32, I32]] val)
  =>
    // TODO: Add logic to compare old and new topic mapping to identify newly
    // unthrottled partitions for which to send buffered messages
    topic_mapping = topic_mapping'

  // main logic for sending messages to brokers
  fun ref send_topic_messages(
    topic: String, msgs: Array[(Any tag, (ByteSeq | Array[ByteSeq] val),
    (None | ByteSeq | Array[ByteSeq] val))]):
    (None | Array[(KafkaError, I32, Any tag)])
  =>
    let msgs_to_send: Map[I32, Array[ProducerKafkaMessage val] iso] iso =
      recover iso Map[I32, Array[ProducerKafkaMessage val] iso] end

    var error_msgs: (None | Array[(KafkaError, I32, Any tag)]) = None

    if conf.producer_topics.contains(topic) then
      let error_msgs' = Array[(KafkaError, I32, Any tag)]

      try
        let tc = conf.topics(topic)
        let tm = topic_mapping(topic)
        let message_partitioner = topic_partitioners(topic)

        for (opaque, value, key) in msgs.values() do
          let key_size = match key
            | let b: ByteSeq => b.size()
            | let a: Array[ByteSeq] val => calc_array_byteseq_size(a)
            else
              0
            end

          let value_size = match value
            | let b: ByteSeq => b.size()
            | let a: Array[ByteSeq] val => calc_array_byteseq_size(a)
            else
              0
            end

          if (key_size + value_size) > conf.max_message_size.usize() then
            error_msgs'.push((ErrorClientMessageTooLarge, -1, opaque))
            continue
          end

          // run user specified message partitioner to determine which partition
          // the message needs to be sent to
          let part_id = try message_partitioner(key, key_size, tm.size().i32())
            as I32 else -1 end

          if (part_id < 0) or (part_id > tm.size().i32()) then
            error_msgs'.push((KafkaClientInvalidPartition, part_id, opaque))
            continue
          end

          let broker_id = tm(part_id)

          if broker_id == -999 then
            // TODO: implement buffering logic for throttled brokers
            conf.logger(Error) and conf.logger.log(Error,
              "Error buffering logic for throttled brokers not implemented!")
            error_msgs'.push((KafkaClientNoBuffering, part_id, opaque))
            continue
          end

          let m = recover val ProducerKafkaMessage(_producer, opaque, value,
            value_size, key, key_size) end

          if not msgs_to_send.contains(part_id) then
            msgs_to_send(part_id) = recover iso Array[ProducerKafkaMessage val]
              end
          end

          try
            msgs_to_send(part_id).push(m)
          else
            conf.logger(Error) and conf.logger.log(Error,
              "Error adding message to array in map. This should never happen.")
            error_msgs'.push((KafkaClientShouldNeverHappen, part_id, opaque))
          end
        end
      else
        conf.logger(Error) and conf.logger.log(Error, "Error loading " +
          "configuration or state while preparing messages to send for " +
          "topic: " + topic + ". This should never happen.")
        for (opaque, value, key) in msgs.values() do
          error_msgs'.push((KafkaClientShouldNeverHappen, -1, opaque))
        end
        return error_msgs'
      end

    else
      let error_msgs' = Array[(KafkaError, I32, Any tag)]
      for (opaque, value, key) in msgs.values() do
        error_msgs'.push((KafkaClientProducerTopicNotRegistered, -1, opaque))
      end
      return error_msgs'
    end


    let mts = consume val msgs_to_send

    // send all messages to all brokers; they will look at only what's relevant
    // to them
    conf.logger(Fine) and conf.logger.log(Fine, "Sending messages to brokers")
    for (broker_id, (broker_info, broker_tag)) in brokers.pairs() do
      broker_tag.send_kafka_messages(topic, mts, _auth)
    end

    error_msgs

  fun calc_array_byteseq_size(array: Array[ByteSeq] val): USize =>
    var size: USize = 0
    for a in array.values() do
      size = size + a.size()
    end
    size

  fun ref send_topic_message(topic: String, opaque: Any tag,
    value: (ByteSeq | Array[ByteSeq] val),
    key: (None | ByteSeq | Array[ByteSeq] val) = None):
    (None | (KafkaError, I32, Any tag))
  =>
    if conf.producer_topics.contains(topic) then
      try
        let tc = conf.topics(topic)
        let tm = topic_mapping(topic)
        let message_partitioner = topic_partitioners(topic)

        let key_size = match key
          | let b: ByteSeq => b.size()
          | let a: Array[ByteSeq] val => calc_array_byteseq_size(a)
          else
            0
          end

        let value_size = match value
          | let b: ByteSeq => b.size()
          | let a: Array[ByteSeq] val => calc_array_byteseq_size(a)
          else
            0
          end

        if (key_size + value_size) > conf.max_message_size.usize() then
          return (ErrorClientMessageTooLarge, -1, opaque)
        end

        // run user specified message partitioner to determine which partition
        // the message needs to be sent to
        let part_id =
          try
            message_partitioner(key, key_size, tm.size().i32()) as I32
          else
            -1
          end

        if (part_id < 0) or (part_id > tm.size().i32()) then
          return (KafkaClientInvalidPartition, part_id, opaque)
        end

        let broker_id = tm(part_id)

        if broker_id == -999 then
          // TODO: implement buffering logic for throttled brokers
          conf.logger(Error) and conf.logger.log(Error,
            "Error buffering logic for throttled brokers not implemented!")
          return (KafkaClientNoBuffering, part_id, opaque)
        end

        let msg = recover val ProducerKafkaMessage(_producer, opaque, value,
          value_size, key, key_size) end

        // send message to appropriate broker
        conf.logger(Fine) and conf.logger.log(Fine, "Sending message to broker")

        (_, let broker_tag) =
          try
            brokers(broker_id)
          else
            conf.logger(Error) and conf.logger.log(Error,
              "Error looking up brokers(broker_id): " + broker_id.string())
            error
          end

        broker_tag.send_kafka_message(topic, part_id, msg, _auth)
      else
        conf.logger(Error) and conf.logger.log(Error,
          "Error preparing message to send for topic: " + topic)
      end
    else
      return (KafkaClientProducerTopicNotRegistered, -1, opaque)
    end

    None

// kafka client actor is responsible for creating broker connections and making
// sure everything is set up/coordinated correctly
actor KafkaClient
  let _conf: KafkaConfig val
  let _brokers: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] =
    _brokers.create()
  let _broker_connection_factory: KafkaBrokerConnectionFactory val
  let _auth: TCPConnectionAuth
  let _manager: KafkaClientManager tag
  let _producers: SetIs[KafkaProducer tag] = _producers.create()
  var _brokers_read_only: Map[I32, (_KafkaBroker val, KafkaBrokerConnection
    tag)] val = recover val _brokers_read_only.create() end

  let _uninitialized_brokers: Set[I32] = _uninitialized_brokers.create()

  let _topic_leader_state: Map[String, Map[I32, (I32, Bool, Bool)]] =
    _topic_leader_state.create()
  var _topic_mapping_read_only: Map[String, Map[I32, I32]] val = recover val
    _topic_mapping_read_only.create() end
  var _topic_partitions_read_only: Map[String, (KafkaTopicType, Set[I32])] val =
     recover val _topic_partitions_read_only.create() end

  var fully_initialized: Bool = false

  let _topic_consumer_handlers: Map[String, KafkaConsumerMessageHandler val] =
    _topic_consumer_handlers.create()
  var _topic_consumer_handlers_read_only: Map[String,
    KafkaConsumerMessageHandler val] val = recover val Map[String,
    KafkaConsumerMessageHandler val] end

  let _topic_consumers: Map[String, Array[KafkaConsumer tag]] =
    _topic_consumers.create()
  var _topic_consumers_read_only: Map[String, Array[KafkaConsumer tag] val] val
    = recover val Map[String, Array[KafkaConsumer tag] val] end

  let _leader_change_unthrottle_acks: MapIs[Map[String, Map[I32, I32]] val,
    (Map[String, Set[I32] iso] val, I32, SetIs[KafkaProducer tag])] =
    _leader_change_unthrottle_acks.create()
  let _leader_change_throttle_acks: MapIs[Map[String, Map[I32, I32]] val,
    (Map[String, Set[I32] iso] val, I32, SetIs[KafkaProducer tag])] =
    _leader_change_throttle_acks.create()

  new create(auth: TCPConnectionAuth, conf: KafkaConfig val,
    manager: KafkaClientManager tag,
    broker_connection_factory: KafkaBrokerConnectionFactory val =
    SimpleKafkaBrokerConnectionFactory)
  =>
    _conf = conf
    _broker_connection_factory = broker_connection_factory
    _auth = auth
    _manager = manager

    for (topic, tc) in conf.topics.pairs() do
      _topic_consumer_handlers(topic) = tc.consumer_message_handler
    end

    _update_consumer_handlers_read_only()

    for topic in _conf.consumer_topics.values() do
      _topic_consumers(topic) = Array[KafkaConsumer tag]
    end

    // create initial broker connections for discovering kafka metadata; these
    // kill themselves after reading metadata
    for b in _conf.brokers.values() do
      _broker_connection_factory(_auth, recover iso _KafkaHandler(this, _conf,
        _topic_consumer_handlers_read_only) end, b.host, b.port.string())
    end

  fun ref _update_consumer_handlers_read_only() =>
    let topic_consumer_handlers: Map[String, KafkaConsumerMessageHandler val]
      iso = recover iso Map[String, KafkaConsumerMessageHandler val] end

    for (topic, consumer_handler) in _topic_consumer_handlers.pairs() do
      topic_consumer_handlers(topic) = consumer_handler
    end

    _topic_consumer_handlers_read_only = consume val topic_consumer_handlers

  fun ref update_consumers_read_only() =>
    let topic_consumers: Map[String, Array[KafkaConsumer tag] val] iso = recover
       iso Map[String, Array[KafkaConsumer tag] val] end
    for (topic, consumers) in _topic_consumers.pairs() do
      let my_consumers: Array[KafkaConsumer tag] iso = recover iso
        Array[KafkaConsumer tag] end
      for c in consumers.values() do
        my_consumers.push(c)
      end
      topic_consumers(topic) = consume my_consumers
    end

    _topic_consumers_read_only = consume topic_consumers

  be update_consumer_message_handler(topic: String,
    consumer_handler: KafkaConsumerMessageHandler val)
  =>
    for (_, bc) in _brokers.values() do
      bc._update_consumer_message_handler(topic, consumer_handler)
    end

    _topic_consumer_handlers(topic) = consumer_handler
    _update_consumer_handlers_read_only()


  be register_consumer(topic: String, c: KafkaConsumer tag) =>
    _register_consumers(topic, recover val [c] end)

  be register_consumers(topic: String, consumers: Array[KafkaConsumer tag] val)
  =>
    _register_consumers(topic, consumers)

  // can't use hash because it changes the order of consumers provided and that
  // might matter to someone
  // Maybe people need to be able to provide something else in addition to the
  // Consumer tag for their use in the consumer_message_handler?
  fun ref _register_consumers(topic: String,
    consumers: Array[KafkaConsumer tag] val)
  =>
    try
      let tc = _topic_consumers(topic)
      for c in consumers.values() do
        if not tc.contains(c) then
          tc.push(c)
        end
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error,
        "Error adding consumers to topic_consumers. This should never happen.")
    end

    update_consumers_read_only()

    for (_, bc) in _brokers.values() do
      bc._update_consumers(_topic_consumers_read_only)
    end

  be replace_consumers(topic: String, consumers: Array[KafkaConsumer tag] val)
    =>
    if _topic_consumers.contains(topic) then
      try
        _topic_consumers(topic).clear()
      else
        _conf.logger(Error) and _conf.logger.log(Error, "Error clearing " +
          "consumers in topic_consumers. This should never happen.")
      end
    end

    _register_consumers(topic, consumers)

  // TODO: ability to have a single consumer mute (i.e. if we're using round
  // robin handler we'd skip that consumer); not sure if it's a good ability
  // in general for other handlers [hash, etc] that can't just send a message
  // to a different consumer so maybe not worth implementing?)
  be consumer_pause(topic: String, partition_id: I32) =>
    var something_paused: Bool = false

    try
      (let current_part_leader, let throttled, let consume_paused) =
        _topic_leader_state(topic)(partition_id)
      if consume_paused == false then
        _topic_leader_state(topic)(partition_id) = (current_part_leader,
          throttled, true)
        something_paused = true
        _conf.logger(Fine) and _conf.logger.log(Fine,
          "Pausing consuming topic: " + topic + " and partition: " +
          partition_id.string() + ".")
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, "Error pausing topic: " +
        topic + " and partition: " + partition_id.string() + ".")
    end

    if something_paused then
      for (_, bc) in _brokers.values() do
        bc._consumer_pause(topic, partition_id)
      end
    end

  be consumer_pause_all() =>
    var something_paused: Bool = false

    _conf.logger(Fine) and _conf.logger.log(Fine,
      "Pausing consuming all topics.")

    // update topic partition/leader mapping if something changed
    for (topic, topic_leader_state) in _topic_leader_state.pairs() do
      for (part_id, (current_part_leader, throttled, consume_paused)) in
        topic_leader_state.pairs() do
        if consume_paused == false then
          topic_leader_state(part_id) = (current_part_leader, throttled, true)
          something_paused = true
        end
      end
    end


    if something_paused then
      for (_, bc) in _brokers.values() do
        bc._consumer_pause_all()
      end
    end

  be consumer_resume(topic: String, partition_id: I32) =>
    var something_resumed: Bool = false

    try
      (let current_part_leader, let throttled, let consume_paused) =
        _topic_leader_state(topic)(partition_id)
      if consume_paused == true then
        _topic_leader_state(topic)(partition_id) = (current_part_leader,
          throttled, false)
        something_resumed = true
        _conf.logger(Fine) and _conf.logger.log(Fine,
          "Pausing consuming topic: " + topic + " and partition: " +
          partition_id.string() + ".")
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error,
        "Error resuming consuming topic: " + topic + " and partition: " +
        partition_id.string() + ".")
    end

    if something_resumed then
      for (_, bc) in _brokers.values() do
        bc._consumer_resume(topic, partition_id)
      end
    end

  be consumer_resume_all() =>
    var something_resumed: Bool = false

    _conf.logger(Fine) and _conf.logger.log(Fine,
      "Resuming consuming all topics.")

    // update topic partition/leader mapping if something changed
    for (topic, topic_leader_state) in _topic_leader_state.pairs() do
      for (part_id, (current_part_leader, throttled, consume_paused)) in
        topic_leader_state.pairs() do
        if consume_paused == true then
          topic_leader_state(part_id) = (current_part_leader, throttled, false)
          something_resumed = true
        end
      end
    end

    if something_resumed then
      for (_, bc) in _brokers.values() do
        bc._consumer_resume_all()
      end
    end

  // wait for brokers to get initialized
  be _broker_initialized(broker_id: I32) =>
    _uninitialized_brokers.unset(broker_id)

    // send all producers their producer mappings so they can start producing
    if (not fully_initialized) and (_uninitialized_brokers.size() == 0) then
      fully_initialized = true
      create_and_send_producer_mappings()
    end

  // update metadata based on what broker connections got from kafka
  be _update_metadata(meta: _KafkaMetadata val) =>
    var brokers_modified: Bool = false
    var topic_mapping_modified: Bool = false
    var new_topic_partition_added: Bool = false

    // create permanent broker connections to all kafka brokers
    for b in meta.brokers.values() do
      if _brokers.contains(b.node_id) then
        if _uninitialized_brokers.contains(b.node_id) then
          try
            (_, let bc) = _brokers(b.node_id)
            bc._update_metadata(meta)
          else
            _conf.logger(Error) and _conf.logger.log(Error, "Error looking " +
              "up broker_id in _brokers map. This should never happen.")
          end
        end
      else
        let bc = _broker_connection_factory(_auth, recover iso
          _KafkaHandler(this, _conf, _topic_consumer_handlers_read_only,
          b.node_id) end, b.host, b.port.string())
        bc._update_metadata(meta)

        // make sure new broker connection unpauses topics/partitions that
        // aren't paused
        for (topic, topic_leader_state) in _topic_leader_state.pairs() do
          for (part_id, (current_part_leader, throttled, consume_paused)) in
            topic_leader_state.pairs() do
            if consume_paused == false then
              bc._consumer_resume(topic, part_id)
            end
          end
        end

        _uninitialized_brokers.set(b.node_id)

        _brokers(b.node_id) = (b, bc)
        brokers_modified = true
      end
    end

    // if we have new brokers, tell everything about it
    if brokers_modified then
      // send updated brokers list to all broker connections

      let brokers_list: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)]
        iso = recover iso brokers_list.create() end
      for (k, v) in _brokers.pairs() do
        try
          brokers_list.insert(k, v)
        end
      end

      let final_brokers_list: Map[I32, (_KafkaBroker val, KafkaBrokerConnection
        tag)] val = consume brokers_list

      for (_, bc) in _brokers.values() do
        bc._update_brokers_list(final_brokers_list)
      end

      _brokers_read_only = final_brokers_list
    end

    // update topic partition/leader mapping if something changed
    for tmeta in meta.topics_metadata.values() do
      let topic_leader_state = try _topic_leader_state(tmeta.topic)
        else
          let tm = Map[I32, (I32, Bool, Bool)]
          _topic_leader_state(tmeta.topic) = tm
          topic_mapping_modified = true
          new_topic_partition_added = true
          tm
        end

      for part_meta in tmeta.partitions_metadata.values() do
        (let current_part_leader, let throttled, let consume_paused) =
          topic_leader_state.get_or_else(part_meta.partition_id, (-99, true,
          true))
        // New partitions/topics start out throttled. Broker connection has to
        // explicitly unthrottle
        if current_part_leader != part_meta.leader then
          topic_leader_state(part_meta.partition_id) = (part_meta.leader,
            throttled, consume_paused)
          topic_mapping_modified = true
          new_topic_partition_added = if current_part_leader == -99 then true
            else new_topic_partition_added end
        end
      end
    end

    // if topic mapping changed, tell all producers and send all brokers latest
    // metadata
    if topic_mapping_modified then
      // tell all broker connections to update their state so everyone is in
      // sync
      for (_, bc) in _brokers.values() do
        bc._update_metadata(meta)
      end

      update_read_only_topic_mapping()

      // let kafka client manager know latest topic/partitions list
      if new_topic_partition_added then

        let map_topic_partitions: Map[String, (KafkaTopicType, Set[I32])] iso =
          recover iso map_topic_partitions.create() end
        for (topic, topic_leader_state) in _topic_leader_state.pairs() do
          try
            let map_topic_parts: Set[I32] iso = recover map_topic_parts.create()
               end
            for partition_id in topic_leader_state.keys() do
              map_topic_parts.set(partition_id)
            end
            let ktt: KafkaTopicType =
              if _conf.producer_topics.contains(topic)
              and _conf.consumer_topics.contains(topic) then
                KafkaProduceAndConsume
              elseif
                _conf.producer_topics.contains(topic)
                and not _conf.consumer_topics.contains(topic) then
                  KafkaProduceOnly
                elseif not _conf.producer_topics.contains(topic)
                  and _conf.consumer_topics.contains(topic) then
                    KafkaConsumeOnly
                  else
                    KafkaProduceAndConsume // this should never be reached
                  end
            map_topic_partitions.insert(topic, (ktt, consume map_topic_parts))
          end
        end

        _topic_partitions_read_only = consume map_topic_partitions

        _manager.receive_kafka_topics_partitions(_topic_partitions_read_only)
      end

      // update producers with new info
      for p in _producers.values() do
        p._update_brokers_and_topic_mapping(_brokers_read_only,
          _topic_mapping_read_only)
      end
    end

  fun ref update_read_only_topic_mapping() =>
    let map_topic_mapping: Map[String, Map[I32, I32]] iso = recover iso
      map_topic_mapping.create() end
    for (topic, topic_leader_state) in _topic_leader_state.pairs() do
      try
        let map_topic_part_mapping: Map[I32, I32] iso = recover
          map_topic_part_mapping.create() end
        for (partition_id, (leader_id, throttled, consume_paused)) in
          topic_leader_state.pairs() do
          map_topic_part_mapping.insert(partition_id, if throttled then -999
            else leader_id end)
        end
        map_topic_mapping.insert(topic, consume map_topic_part_mapping)
      end
    end

    _topic_mapping_read_only = consume map_topic_mapping

  // only tell producers if there are actually topics to produce on by the
  // client
  fun create_and_send_producer_mappings() =>
    if _conf.producer_topics.size() > 0 then
      for p in _producers.values() do
        p._create_producer_mapping(recover iso KafkaProducerMapping(_conf,
          _topic_mapping_read_only, _brokers_read_only, p) end)
      end
    end

  // register producers that will need to be able to publish to kafka
  be register_producer(p: KafkaProducer tag) =>
    _producers.set(p)

    if (_conf.producer_topics.size() > 0) and fully_initialized then
      // only create producer mapping if we're fully initialized

      // simulate throttle ack for any outstanding throttle requests
      for topic_mapping_throttle in _leader_change_throttle_acks.keys() do
        throttle_ack(topic_mapping_throttle, p, p)
      end

      // simulate unthrottle ack for any outstanding unthrottle requests
      for topic_mapping_unthrottle in _leader_change_unthrottle_acks.keys() do
        unthrottle_ack(topic_mapping_unthrottle, p, p)
      end

      // create producer mapping
      p._create_producer_mapping(recover iso KafkaProducerMapping(_conf,
        _topic_mapping_read_only, _brokers_read_only, p) end)
    end

  // throttling without leader change (no ack confirmation from producers)
  be _throttle_producers(broker_id: I32) =>
    // mark all partitions for the broker as throttled
    for (topic, map_leader_state) in _topic_leader_state.pairs() do
      for (partition_id, (leader_id, throttled, consume_paused)) in
        map_leader_state.pairs() do
        if leader_id == broker_id then
          map_leader_state(partition_id) = (leader_id, true, consume_paused)
        end
      end
    end

    update_read_only_topic_mapping()

    // send updated topic mapping to producers so they can pause/buffer
    // producing
    for p in _producers.values() do
      p._kafka_producer_throttle(_topic_mapping_read_only, false, this, p)
    end

  // unthrottling without leader change (no ack confirmation from producers)
  be _unthrottle_producers(broker_id: I32) =>
    var fully_unthrottled: Bool = true

    // mark all partitions for the broker as unthrottled
    for (topic, map_leader_state) in _topic_leader_state.pairs() do
      for (partition_id, (leader_id, throttled, consume_paused)) in
        map_leader_state.pairs() do
        if leader_id == broker_id then
          map_leader_state(partition_id) = (leader_id, false, consume_paused)
        elseif throttled == true then
          fully_unthrottled = false
        end
      end
    end

    update_read_only_topic_mapping()

    // if not full initialized then don't update mapping to send to producers
    if not fully_initialized then
      return
    end

    // send updated topic mapping to producers so they can resume producing
    for p in _producers.values() do
      p._kafka_producer_unthrottle(_topic_mapping_read_only, false, this, p,
        fully_unthrottled)
    end

  be _leader_change_throttle(topics_to_throttle: Map[String, Set[I32] iso] val,
    broker_id: I32)
  =>
    // mark topic/partitions requested as throttled
    for (topic, partitions) in topics_to_throttle.pairs() do
      try
        let topic_leader_state_current = _topic_leader_state(topic)
        for partition_id in partitions.values() do
          (let leader_id, let throttled, let consume_paused) =
            topic_leader_state_current(partition_id)
          topic_leader_state_current(partition_id) = (leader_id, true,
            consume_paused)
        end
      else
        _conf.logger(Error) and _conf.logger.log(Error, "Leader change " +
          "throttle: Error looking up topic or partition leader/throttle " +
          "info. This should never happen.")
      end
    end

    update_read_only_topic_mapping()

    // if not full initialized then don't update mapping to send to producers
    if not fully_initialized then
      return
    end

    _leader_change_throttle_acks(_topic_mapping_read_only) =
      (topics_to_throttle, broker_id, SetIs[KafkaProducer tag])

    // send updated topic mapping to producers so they can pause/buffer
    // producing
    for p in _producers.values() do
      p._kafka_producer_throttle(_topic_mapping_read_only, true, this, p)
    end

  be throttle_ack(topic_mapping: Map[String, Map[I32, I32]] val, actual_p:
    KafkaProducer tag, sent_p: KafkaProducer tag) =>
    if not (actual_p is sent_p) then
      _conf.logger(Error) and _conf.logger.log(Error, "Throttle: Actual " +
        "producer and sent producer are not the same! This should never " +
        "happen.")
    end

    try
      (let topics_to_throttle, let broker_id, let acks_received) =
        _leader_change_throttle_acks(topic_mapping)
      acks_received.set(sent_p)

      // if we've received all acks
      if acks_received.size() == _producers.size() then
        _leader_change_throttle_acks.remove(topic_mapping)

        // Notify broker of all acks received for throttle
        (_, let bc) = _brokers(broker_id)
        bc._leader_change_throttle_ack(topics_to_throttle)
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, "Throttle: Error " +
        "looking up topic_mapping in _leader_change_throttle_acks. This " +
        "should never happen.")
    end

  be _leader_change_unthrottle(
    topics_to_unthrottle: Map[String, Set[I32] iso] val,
    broker_id: I32)
  =>
    var fully_unthrottled: Bool = true

    // mark topic/partitions requested as unthrottled
    for (topic, map_leader_state) in _topic_leader_state.pairs() do
      for (partition_id, (leader_id, throttled, consume_paused)) in
        map_leader_state.pairs() do
        try
          if topics_to_unthrottle.contains(topic) and
            topics_to_unthrottle(topic).contains(partition_id) then
            map_leader_state(partition_id) = (leader_id, false, consume_paused)
          elseif throttled == true then
            fully_unthrottled = false
          end
        else
          _conf.logger(Error) and _conf.logger.log(Error, "Leader change " +
            "unthrottle: Error looking up topic or partition in " +
            "topics_to_unthrottle. This should never happen.")
        end
      end
    end

    update_read_only_topic_mapping()

    // TODO: do we really need to track unthrottle acks? Might be able to ignore
    // it since there's no synchronization need regarding acks for unthrottles
    _leader_change_unthrottle_acks(_topic_mapping_read_only) =
      (topics_to_unthrottle, broker_id, SetIs[KafkaProducer tag])

    // send updated topic mapping to producers so they can resume producing
    for p in _producers.values() do
      p._kafka_producer_unthrottle(_topic_mapping_read_only, true, this, p,
        fully_unthrottled)
    end

  be unthrottle_ack(topic_mapping: Map[String, Map[I32, I32]] val,
    actual_p: KafkaProducer tag, sent_p: KafkaProducer tag)
  =>
    if not (actual_p is sent_p) then
      _conf.logger(Error) and _conf.logger.log(Error, "Unthrottle: Actual " +
        "producer and sent producer are not the same! This should never " +
        "happen.")
    end

    try
      (let topics_to_unthrottle, let broker_id, let acks_received) =
        _leader_change_unthrottle_acks(topic_mapping)
      acks_received.set(sent_p)

      // if we've received all acks
      if acks_received.size() == _producers.size() then
        _leader_change_unthrottle_acks.remove(topic_mapping)
        // TODO: Notify broker of all acks received for unthrottle
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, "Unthrottle: Error " +
        "looking up topic_mapping in _leader_change_unthrottle_acks. This " +
        "should never happen.")
    end

  be _unrecoverable_error(error_report: KafkaErrorReport) =>
    // log error
    _conf.logger(Error) and _conf.logger.log(Error, "Kafka Client encountered" +
      " unrecoverable error! " + error_report.string())

    _conf.logger(Error) and _conf.logger.log(Error, "SHUTTING DOWN CLIENT!")

    // let manager know of the unrecoverable
    _manager.kafka_client_error(error_report)

    // call dispose to clean up/shut down
    dispose()

  // TODO: Make sure dispose is being done properly and client will throw
  // errors or something if it gets future messages after dispose
  be dispose() =>
    for (_, bc) in _brokers.values() do
      bc.dispose()
    end
    _brokers.clear()

