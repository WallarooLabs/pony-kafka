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

use "ponytest"
use "customlogger"
use "custombuffered"
use "collections"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)
  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestProduceV0ApiNoCompression)
    test(_TestProduceV0ApiGzipCompression)
    test(_TestProduceV0ApiLZ4Compression)
    test(_TestProduceV0ApiSnappyCompression)
    test(_TestProduceV1ApiNoCompression)
    test(_TestProduceV1ApiGzipCompression)
    test(_TestProduceV1ApiLZ4Compression)
    test(_TestProduceV1ApiSnappyCompression)
    test(_TestProduceV2ApiNoCompression)
    test(_TestProduceV2ApiGzipCompression)
    test(_TestProduceV2ApiLZ4Compression)
    test(_TestProduceV2ApiSnappyCompression)
    test(_TestProduceV0ApiSplitLargeRequest)
    test(_TestProduceV1ApiSplitLargeRequest)
    test(_TestProduceV2ApiSplitLargeRequest)
    test(_TestOffsetsV0Api)
    test(_TestOffsetsV1Api)


// kafka producer actor
actor TestP is KafkaProducer
  new create() =>
    None

  fun ref update_producer_mapping(mapping: KafkaProducerMapping):
    (KafkaProducerMapping | None) => None

  be kafka_producer_ready() => None

  be kafka_message_delivery_report(delivery_report: KafkaProducerDeliveryReport)
  =>
    None

  fun ref producer_mapping(): (KafkaProducerMapping | None) => None

  fun ref _kafka_producer_throttled(topic_mapping: Map[String, Map[I32, I32]]
    val)
  =>
    None

  fun ref _kafka_producer_unthrottled(topic_mapping: Map[String, Map[I32, I32]]
    val, fully_unthrottled: Bool)
  =>
    None


primitive DataGen
  // generate data
  fun generate_data(p: KafkaProducer tag = TestP): Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]]
  =>
    let msgs = Array[ProducerKafkaMessage val]

    var i: USize = 0

    var a: Array[U8] val = ("(" + i.string() + ") - 2begin").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is anothr test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is and another test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is anothr test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is and another test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is anothr test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is and another test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is anothr test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is and another test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is anothr test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2this is and another test").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    a = ("(" + i.string() + ") - 2done").array()
    msgs.push(recover iso ProducerKafkaMessage(p, a, a, a.size()) end)
    i = i + 1

    let part_msgs: Map[I32, Array[ProducerKafkaMessage val]] = Map[I32,
      Array[ProducerKafkaMessage val]]
    let topics_msgs: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]=
      Map[String, Map[I32, Array[ProducerKafkaMessage val]]]

    part_msgs(0) = consume msgs

    topics_msgs("test") = consume part_msgs

    topics_msgs

  fun generate_random_data(topic: String, msg_size: USize, num_msgs: USize, p:
    KafkaProducer tag = TestP): Map[I32, Array[ProducerKafkaMessage val] iso]
    val ?
  =>
    let part_msgs: Map[I32, Array[ProducerKafkaMessage val] iso] iso = recover
      iso part_msgs.create() end
    part_msgs(0) = recover iso Array[ProducerKafkaMessage val] end

    for i in Range[USize](0, num_msgs) do
      let a = recover Array[U8] end
      a.undefined(msg_size)
      let b = consume val a
      part_msgs(0).push(recover iso ProducerKafkaMessage(p, b, b, b.size()) end)
    end

    part_msgs

primitive _RunOffsetsApiTest
  """
  Test kafka api
  """
  fun apply(h: TestHelper, api_to_use: _KafkaOffsetsApi) ? =>
    let logger = StringLogger(Warn, h.env.out)
    var correlation_id: I32 = 9

    let topic: String = "test"
    let topic2: String = "test2"
    let partition_id: I32 = 0

    let state = _KafkaState

    let topic_state = _KafkaTopicState(topic,
      KafkaRoundRobinConsumerMessageHandler)
    let topic_partition_state = _KafkaTopicPartitionState(0, partition_id, 0,
      recover Array[I32] end, recover Array[I32] end)
    topic_partition_state.current_leader = true
    topic_state.partitions_state(partition_id) = topic_partition_state
    state.topics_state(topic) = topic_state

    let topic2_state = _KafkaTopicState(topic2,
      KafkaRoundRobinConsumerMessageHandler)
    let topic2_partition_state = _KafkaTopicPartitionState(0, partition_id, 0,
      recover Array[I32] end, recover Array[I32] end)
    topic2_partition_state.current_leader = false
    topic2_state.partitions_state(partition_id) = topic2_partition_state
    state.topics_state(topic2) = topic2_state

    // create kafka config
    let conf =
      recover val
        let kc = KafkaConfig(logger, "My Client" where max_message_size' =
          32738)
        kc.add_broker("127.0.0.1", 9092)

        // add topic config to consumer
// producer/consumer config and tell kafka to send messages to consumers
        kc.add_topic_config(topic, KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end)

        // add topic config to consumer
        kc.add_topic_config(topic2, KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end)

        kc
      end

    let offset_request: Array[ByteSeq] val =
      api_to_use.encode_request(correlation_id, conf, state.topics_state)

    let rb = recover ref Reader end
    for d in offset_request.values() do
      match d
      | let s: String => rb.append(s.array())
      | let a: Array[U8] val => rb.append(a)
      end
    end

    let remaining_size = _KafkaI32Codec.decode(logger, rb,
      "error decoding size")
    h.assert_eq[USize](remaining_size.usize(), rb.size())

    (let api_key', let api_version', let correlation_id', let client_name') =
      _KafkaRequestHeader.decode(logger, rb)

    h.assert_eq[I16](api_key', api_to_use.api_key())
    h.assert_eq[I16](api_version', api_to_use.version())
    h.assert_eq[I32](correlation_id', correlation_id)
    h.assert_eq[String](client_name', conf.client_name)

    (let replica_id', let topics_requested') = api_to_use.decode_request(logger,
      rb)
    h.assert_eq[I32](replica_id', conf.replica_id)

    for (t, ts) in state.topics_state.pairs() do
      for (part_id, part_state) in ts.partitions_state.pairs() do
        if part_state.current_leader then
          (let request_timestamp', let num_requested') =
            topics_requested'(t)(part_id)
          h.assert_eq[I64](request_timestamp', part_state.request_timestamp)
          h.assert_eq[I32](num_requested', 1)
        end
      end
    end


primitive _RunProduceApiTest
  """
  Test kafka api
  """
  fun apply(h: TestHelper, compression: KafkaTopicCompressionType, api_to_use:
    _KafkaProduceApi) ?
  =>
    let consumers = recover iso Array[KafkaConsumer tag] end

    let logger = StringLogger(Warn, h.env.out)
    let msgs = DataGen.generate_data()
    var correlation_id: I32 = 9

    let topic: String = "test"
    let partition_id: I32 = 0

    let state = _KafkaState
    let topic_state = _KafkaTopicState(topic,
      KafkaRoundRobinConsumerMessageHandler)
    let topic_partition_state = _KafkaTopicPartitionState(0, partition_id, 0,
      recover Array[I32] end, recover Array[I32] end)

    topic_state.partitions_state(partition_id) = topic_partition_state
    state.topics_state(topic) = topic_state

    // create kafka config
    let conf =
      recover val
        let kc = KafkaConfig(logger, "My Client")
        kc.add_broker("127.0.0.1", 9092)

        // add topic config to consumer
// producer/consumer config and tell kafka to send messages to consumers
        kc.add_topic_config(topic, KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end, compression = compression)

        kc
      end

    let produce_request: Array[ByteSeq] val =
      api_to_use.encode_request(correlation_id, conf, msgs)

    let rb = recover ref Reader end
    for d in produce_request.values() do
      match d
      | let s: String => rb.append(s.array())
      | let a: Array[U8] val => rb.append(a)
      end
    end


    let remaining_size = _KafkaI32Codec.decode(logger, rb,
      "error decoding size")
    h.assert_eq[USize](remaining_size.usize(), rb.size())

    (let api_key', let api_version', let correlation_id', let client_name') =
      _KafkaRequestHeader.decode(logger, rb)

    h.assert_eq[I16](api_key', api_to_use.api_key())
    h.assert_eq[I16](api_version', api_to_use.version())
    h.assert_eq[I32](correlation_id', correlation_id)
    h.assert_eq[String](client_name', conf.client_name)

    let broker_conn = _MockKafkaBrokerConnection
    (let produce_acks', let produce_timeout', let msgs') =
      api_to_use.decode_request(broker_conn, logger, rb, state.topics_state)

    h.assert_eq[I16](produce_acks', conf.produce_acks)
    h.assert_eq[I32](produce_timeout', conf.produce_timeout_ms)

    h.assert_eq[USize](msgs'.size(), msgs.size())

    for (t', tm') in msgs'.pairs() do
      let tm = msgs(t')
      h.assert_eq[USize](tm'.size(), tm.size())
      for (p', pm') in tm'.pairs() do
        let pm = tm(p')
        h.assert_eq[USize](pm'.size(), pm.size())
        for (i', m') in pm'.pairs() do
          let m = pm(i')
          match m.get_value()
          | let b: ByteSeq =>
            h.assert_true(ByteSeqComparator.eq(m'.get_value(), b))
          | let ba: Array[ByteSeq] val =>
            let r = recover ref Reader end
            for d in ba.values() do
              match d
              | let s: String => r.append(s.array())
              | let a: Array[U8] val => r.append(a)
              end
            end
            h.assert_true(ByteSeqComparator.eq(m'.get_value(),
              r.block(r.size())))
          else
            // this should never happen
            error
          end
          match m.get_key()
          | let b: ByteSeq =>
            h.assert_true(KeyComparator.eq(m'.get_key(), b))
          | let ba: Array[ByteSeq] val =>
            let r = recover ref Reader end
            for d in ba.values() do
              match d
              | let s: String => r.append(s.array())
              | let a: Array[U8] val => r.append(a)
              end
            end
            h.assert_true(KeyComparator.eq(m'.get_key(), r.block(r.size())))
          | let n: None =>
            h.assert_true(KeyComparator.eq(m'.get_key(), n))
          else
            // this should never happen
            error
          end
        end
      end
    end


class iso _TestProduceV0ApiNoCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV0ApiNoCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaNoTopicCompression, _KafkaProduceV0)

class iso _TestProduceV0ApiGzipCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV0ApiGzipCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaGzipTopicCompression, _KafkaProduceV0)

class iso _TestProduceV0ApiLZ4Compression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV0ApiLZ4Compression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaLZ4TopicCompression, _KafkaProduceV0)

class iso _TestProduceV0ApiSnappyCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV0ApiSnappyCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaSnappyTopicCompression, _KafkaProduceV0)

class iso _TestProduceV1ApiNoCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV1ApiNoCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaNoTopicCompression, _KafkaProduceV1)

class iso _TestProduceV1ApiGzipCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV1ApiGzipCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaGzipTopicCompression, _KafkaProduceV1)

class iso _TestProduceV1ApiLZ4Compression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV1ApiLZ4Compression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaLZ4TopicCompression, _KafkaProduceV1)

class iso _TestProduceV1ApiSnappyCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV1ApiSnappyCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaSnappyTopicCompression, _KafkaProduceV1)

class iso _TestProduceV2ApiNoCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV2ApiNoCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaNoTopicCompression, _KafkaProduceV2)

class iso _TestProduceV2ApiGzipCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV2ApiGzipCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaGzipTopicCompression, _KafkaProduceV2)

class iso _TestProduceV2ApiLZ4Compression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV2ApiLZ4Compression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaLZ4TopicCompression, _KafkaProduceV2)

class iso _TestProduceV2ApiSnappyCompression is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV2ApiSnappyCompression"

  fun apply(h: TestHelper) ? =>
    _RunProduceApiTest(h, KafkaSnappyTopicCompression, _KafkaProduceV2)

class iso _TestProduceV0ApiSplitLargeRequest is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV0ApiSplitLargeRequest"

  fun apply(h: TestHelper) ? =>
    _ProduceApiCombineSplitTest(h, _KafkaProduceV0)

class iso _TestProduceV1ApiSplitLargeRequest is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV1ApiSplitLargeRequest"

  fun apply(h: TestHelper) ? =>
    _ProduceApiCombineSplitTest(h, _KafkaProduceV1)

class iso _TestProduceV2ApiSplitLargeRequest is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/ProduceV2ApiSplitLargeRequest"

  fun apply(h: TestHelper) ? =>
    _ProduceApiCombineSplitTest(h, _KafkaProduceV2)

class iso _TestOffsetsV0Api is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/OffsetsV0"

  fun apply(h: TestHelper) ? =>
    _RunOffsetsApiTest(h, _KafkaOffsetsV0)

class iso _TestOffsetsV1Api is UnitTest
  """
  Test kafka api
  """
  fun name(): String => "kafka/OffsetsV1"

  fun apply(h: TestHelper) ? =>
    _RunOffsetsApiTest(h, _KafkaOffsetsV1)

primitive _ProduceApiCombineSplitTest
  """
  Test kafka api
  """
  fun apply(h: TestHelper, api_to_use: _KafkaProduceApi) ? =>
    let consumers = recover iso Array[KafkaConsumer tag] end
    let logger = StringLogger(Warn, h.env.out)
    var correlation_id: I32 = 9

    let topic: String = "test"
    let topic2: String = "test2"
    let partition_id: I32 = 0

    let state = _KafkaState

    let topic_state = _KafkaTopicState(topic,
      KafkaRoundRobinConsumerMessageHandler)
    let topic_partition_state = _KafkaTopicPartitionState(0, partition_id, 0,
      recover Array[I32] end, recover Array[I32] end)
    topic_partition_state.current_leader = true
    topic_partition_state.leader_change = false
    topic_state.partitions_state(partition_id) = topic_partition_state
    state.topics_state(topic) = topic_state

    let topic2_state = _KafkaTopicState(topic2,
      KafkaRoundRobinConsumerMessageHandler)
    let topic2_partition_state = _KafkaTopicPartitionState(0, partition_id, 0,
      recover Array[I32] end, recover Array[I32] end)
    topic2_partition_state.current_leader = false
    topic2_partition_state.leader_change = false
    topic2_state.partitions_state(partition_id) = topic2_partition_state
    state.topics_state(topic2) = topic2_state

    // create kafka config
    let conf =
      recover val
        let kc = KafkaConfig(logger, "My Client" where max_message_size' =
          32738)
        kc.add_broker("127.0.0.1", 9092)

        // add topic config to consumer
// producer/consumer config and tell kafka to send messages to consumers
        kc.add_topic_config(topic, KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end)


        // add topic config to consumer
        kc.add_topic_config(topic2, KafkaProduceAndConsume where
          consumer_message_handler = recover
          KafkaRoundRobinConsumerMessageHandler end)

        kc
      end

    let pending_buffer: Array[(I32, U64, Map[String, Map[I32,
      Array[ProducerKafkaMessage val]]])] = pending_buffer.create()

    // initialize pending buffer
    pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage
      val]]]))


    let msgs = DataGen.generate_random_data(topic, 10, 100)
    let msgs2 = DataGen.generate_random_data(topic, 10, 100)
    let msgs3 = DataGen.generate_random_data(topic, 1000, 100)
    let msgs4 = DataGen.generate_random_data(topic, 10, 100)
    let msgs5 = DataGen.generate_random_data(topic2, 10, 100)
    let msgs6 = DataGen.generate_random_data(topic2, 10, 100)
    let msgs7 = DataGen.generate_random_data(topic2, 1000, 100)
    let msgs8 = DataGen.generate_random_data(topic2, 10, 100)

    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs5, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs6, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs7, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs8, state.topics_state)

    h.assert_eq[USize](pending_buffer.size(), 1)
    (let size, let num, _) = pending_buffer(0)
    h.assert_eq[U64](num, 0)

    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs2, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs3, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs4, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs5, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs6, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs7, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs8, state.topics_state)


    h.assert_eq[USize](pending_buffer.size(), 4)
    while pending_buffer.size() > 0 do
      try
        (let s, let n, let m) = pending_buffer.shift()
        h.assert_true(s < 32768)
        let produce_request: Array[ByteSeq] val = api_to_use.encode_request(9,
          conf, consume m)
        let rb = Reader
        rb.append(produce_request)
        h.assert_eq[USize](s.usize(), rb.size())
      else
        logger(Error) and logger.log(Error,
          "Error checking size of request. Shouldn't happen.")
        error
      end
    end

    pending_buffer.clear()
    pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage
      val]]]))

    for (part_id, p_msgs) in msgs.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end
    for (part_id, p_msgs) in msgs2.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end
    for (part_id, p_msgs) in msgs3.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end
    for (part_id, p_msgs) in msgs4.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end

    h.assert_eq[USize](pending_buffer.size(), 4)
    while pending_buffer.size() > 0 do
      try
        (let s, let n, let m) = pending_buffer.shift()
        h.assert_true(s < 32768)
        let produce_request: Array[ByteSeq] val = api_to_use.encode_request(9,
          conf, consume m)
        let rb = Reader
        rb.append(produce_request)
        h.assert_eq[USize](s.usize(), rb.size())
      else
        logger(Error) and logger.log(Error,
          "Error checking size of request.  Shouldn't happen.")
        error
      end
    end

    pending_buffer.clear()
    pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage
      val]]]))

    for (part_id, p_msgs) in msgs.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs2, state.topics_state)
    for (part_id, p_msgs) in msgs3.pairs() do
      for m in p_msgs.values() do
        api_to_use.combine_and_split_by_message_size_single(conf,
          pending_buffer, topic, part_id, m, state.topics_state)
      end
    end
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic,
      msgs4, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs5, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs6, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs7, state.topics_state)
    api_to_use.combine_and_split_by_message_size(conf, pending_buffer, topic2,
      msgs8, state.topics_state)

    h.assert_eq[USize](pending_buffer.size(), 4)
    while pending_buffer.size() > 0 do
      try
        (let s, let n, let m) = pending_buffer.shift()
        h.assert_true(s < 32768)
        let produce_request: Array[ByteSeq] val = api_to_use.encode_request(9,
          conf, consume m)
        let rb = Reader
        rb.append(produce_request)
        h.assert_eq[USize](s.usize(), rb.size())
      else
        logger(Error) and logger.log(Error,
          "Error checking size of request.  Shouldn't happen.")
        error
      end
    end


primitive KeyComparator
  fun eq(a: (ByteSeq | None), b: (ByteSeq | None)): Bool ? =>
    if (a is None) and (b isnt None) then
      return false
    end

    if (a isnt None) and (b is None) then
      return false
    end

    if (a is None) and (b is None) then
      return true
    end

    let a' = a as Array[U8] val
    let b' = b as Array[U8] val

    ByteSeqComparator.eq(a', b')

primitive ByteSeqComparator
  fun eq(a: ByteSeq, b: ByteSeq): Bool ? =>
    if a.size() != b.size() then
      return false
    end

    for i in Range[USize](0, a.size()) do
      if a(i) != b(i) then
        return false
      end
    end

    true

primitive ArrayComparator[A: (Equatable[A] #read & Stringable #read)]
  fun eq(a: Array[A], b: Array[A]): Bool ? =>
    if a.size() != b.size() then
      return false
    end

    for (i, v) in a.pairs() do
      if v != b(i) then
        return false
      end
    end

    true


