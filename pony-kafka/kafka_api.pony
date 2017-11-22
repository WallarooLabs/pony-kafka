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

use "custombuffered"
use "custombuffered/codecs"
use "collections"
use "customlogger"
use "time"
use "compression"

// internal classes for kafka request/response related object
// TODO: clean up and make these more sane/consistent
class _KafkaBroker is Equatable[_KafkaBroker box]
  var node_id: I32
  let host: String
  let port: I32
  var rack: String

  new create(node_id': I32, host': String, port': I32, rack': String = "") =>
    node_id = node_id'
    host = host'
    port = port'
    rack = rack'

  fun string(): String =>
    "KafkaBroker: [ "
      + "node_id = " + node_id.string()
      + ", host = " + host.string()
      + ", port = " + port.string()
      + ", rack = " + rack.string()
      + " ]\n"

  fun ref set_node_id(node_id': I32) =>
    node_id = node_id'

  fun ref set_rack(rack': String) =>
    rack = rack'

  fun hash(): U64 =>
    host.hash() xor port.hash()

  fun eq(that: _KafkaBroker box): Bool =>
    (host == that.host) and
    (port == that.port)

class _KafkaTopicProduceResponse
  let topic: String
  let partition_responses: Map[I32, _KafkaTopicProducePartitionResponse]

  new create(topic': String,
    partition_responses': Map[I32, _KafkaTopicProducePartitionResponse])
  =>
    topic = topic'
    partition_responses = partition_responses'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partition_responses.values() do
      parts_str.>append(", ").>append(p.string())
    end

    "KafkaTopicProduceResponse: [ "
      + "topic = " + topic.string()
      + ", partition_responses = " + parts_str
      + " ]\n"

class _KafkaTopicProducePartitionResponse
  let partition_id: I32
  let error_code: I16
  let offset: I64
  let timestamp: (I64 | None)

  new create(partition_id': I32, error_code': I16, offset': I64,
    timestamp': (I64 | None) = None)
  =>
    partition_id = partition_id'
    error_code = error_code'
    offset = offset'
    timestamp = timestamp'

  fun string(): String =>
    "KafkaTopicPartitionResponse: [ "
      + "partition_id = " + partition_id.string()
      + ", error_code = " + error_code.string()
      + ", offset = " + offset.string()
      + ", timestamp = " + timestamp.string()
      + " ]\n"


class _KafkaTopicFetchResult
  let topic: String
  let partition_responses: Map[I32, _KafkaTopicPartitionResponse]

  new create(topic': String,
    partition_responses': Map[I32, _KafkaTopicPartitionResponse])
  =>
    topic = topic'
    partition_responses = partition_responses'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partition_responses.values() do
      parts_str.>append(", ").>append(p.string())
    end

    "KafkaTopicFetchResult: [ "
      + "topic = " + topic.string()
      + ", partition_responses = " + parts_str
      + " ]\n"


class _KafkaTopicPartitionResponse
  let partition_id: I32
  let error_code: I16
  let largest_offset_seen: I64
  let high_watermark: I64
  let messages: Array[KafkaMessage iso] val

  new create(partition_id': I32, error_code': I16, high_watermark': I64,
    largest_offset_seen': I64, messages': Array[KafkaMessage iso] val)
  =>
    partition_id = partition_id'
    error_code = error_code'
    largest_offset_seen = largest_offset_seen'
    high_watermark = high_watermark'
    messages = messages'

  fun string(): String =>
    var msgs_str = recover ref String end
    for m in messages.values() do
      msgs_str.>append(", ").>append(m.string())
    end

    "KafkaTopicPartitionResponse: [ "
      + "partition_id = " + partition_id.string()
      + ", error_code = " + error_code.string()
      + ", largest_offset_seen = " + largest_offset_seen.string()
      + ", high_watermark = " + high_watermark.string()
      + ", messages = " + msgs_str
      + " ]\n"

class val KafkaTopicPartition is Equatable[KafkaTopicPartition box]
  let topic: String
  let partition_id: I32

  new val create(topic': String, partition_id': I32) =>
    topic = topic'
    partition_id = partition_id'

  fun hash(): U64 =>
    topic.hash() xor partition_id.hash()

  fun eq(that: KafkaTopicPartition box): Bool =>
    (topic == that.topic) and
    (partition_id == that.partition_id)

primitive Millis
  fun from_wall_clock(wall: (I64, I64)): U64 =>
    ((wall._1 * 1000000000) + wall._2).u64()/1000

type KafkaMessageTimestampType is (KafkaTimestampNotAvailable |
  KafkaCreateTimestamp | KafkaLogAppendTimestamp)

primitive KafkaTimestampNotAvailable
  fun string(): String => "Message timestamp not available"

primitive KafkaCreateTimestamp
  fun apply(): I8 => 0
  fun string(): String => "Message timestamp is message create time"

primitive KafkaLogAppendTimestamp
  fun string(): String => "Message timestamp is log append time"
  fun apply(): I8 => 1

class ProducerKafkaMessage
  let _key: (ByteSeq | Array[ByteSeq] val | None)
  let _key_size: USize
  let _value: (ByteSeq | Array[ByteSeq] val)
  let _value_size: USize
  var _timestamp: (I64 | None)
  var _timestamp_type: KafkaMessageTimestampType
  let _created_by: KafkaProducer tag
  let _opaque: Any tag

  new create(created_by: KafkaProducer tag, opaque: Any tag,
    value: (ByteSeq | Array[ByteSeq] val), value_size: USize,
    key: (ByteSeq | Array[ByteSeq] val | None) = None, key_size: USize = 0,
    timestamp: I64 = Millis.from_wall_clock(Time.now()).i64(),
    timestamp_type: KafkaMessageTimestampType = KafkaCreateTimestamp)
  =>
    _key = key
    _key_size = key_size
    _value = value
    _value_size = value_size
    _created_by = created_by

    _timestamp = timestamp
    _timestamp_type = timestamp_type
    _opaque = opaque

  fun get_opaque(): Any tag => _opaque

  fun get_value(): (ByteSeq | Array[ByteSeq] val) => _value

  fun get_value_size(): USize => _value_size

  fun get_key(): (ByteSeq | Array[ByteSeq] val | None) => _key

  fun get_key_size(): USize => _key_size

  fun get_timestamp(): ((I64 | None), KafkaMessageTimestampType) =>
    (_timestamp, _timestamp_type)

  fun string(): String =>
    "ProducerKafkaMessage: [ "
      + "timestamp = " + _timestamp.string()
      + ", timestamp_type = " + _timestamp_type.string()
      + ", key size = " + _key_size.string()
      + ", value size = " + _value_size.string()
      + " ]\n"

  fun val _get_created_by(): KafkaProducer tag => _created_by

  fun val _send_delivery_report(delivery_report: KafkaProducerDeliveryReport) =>
    _created_by.kafka_message_delivery_report(delivery_report)

class KafkaMessage
  let _key: (Array[U8] val | None)
  let _value: Array[U8] val
  var _offset: I64
  let _crc: I32
  let _magic_byte: I8
  let _attributes: I8
  var _timestamp: (I64 | None)
  var _timestamp_type: KafkaMessageTimestampType
  let _created_by: KafkaBrokerConnection tag
  let _topic_partition: KafkaTopicPartition

  new _create(key: (Array[U8] val | None), value: Array[U8] val,
    created_by: KafkaBrokerConnection tag, offset: I64, crc: I32,
    magic_byte: I8, attributes: I8, timestamp: (I64 | None),
    timestamp_type: KafkaMessageTimestampType,
    topic_partition: KafkaTopicPartition)
  =>
    _key = key
    _value = value
    _offset = offset
    _crc = crc
    _magic_byte = magic_byte
    _attributes = attributes
    _timestamp = timestamp
    _timestamp_type = timestamp_type
    _created_by = created_by
    _topic_partition = topic_partition

  fun get_value(): Array[U8] val => _value

  fun get_key(): (Array[U8] val | None) => _key

  fun get_offset(): I64 => _offset

  fun get_topic(): String => _topic_partition.topic

  fun get_partition_id(): I32 => _topic_partition.partition_id

  fun get_timestamp(): ((I64 | None), KafkaMessageTimestampType) =>
    (_timestamp, _timestamp_type)

  fun _get_topic_partition(): KafkaTopicPartition => _topic_partition

  fun string(): String =>
    let m = String.from_array(_value)
    let k_s = match _key
              | let k: Array[U8] val => k.size()
              else
                0
              end

    "KafkaMessage: [ "
      + "offset = " + _offset.string()
      + ", crc = " + _crc.string()
      + ", magic_byte = " + _magic_byte.string()
      + ", attributes = " + _attributes.string()
      + ", timestamp = " + _timestamp.string()
      + ", timestamp_type = " + _timestamp_type.string()
      + ", key size = " + k_s.string()
      + ", value size = " + _value.size().string()
//      + if _attributes == 0 then ", value = " + m else "" end
      + " ]\n"

  fun ref _update_offset(offset: I64) => _offset = offset

  fun val _get_created_by(): KafkaBrokerConnection tag => _created_by

/* this is related to consumer offset tracking and might get revived when group consumer support is added
  fun val send_consume_successful() =>
    _created_by.message_consumed(this, true)
*/

/* this is related to consumer offset tracking and might get revived when group consumer support is added
  fun val send_consume_failed() =>
    _created_by.message_consumed(this, false)
*/

class _KafkaTopicOffset
  let topic: String
  let partitions_offset: Array[_KafkaTopicPartitionOffset]

  new create(topic': String,
    partitions_offset': Array[_KafkaTopicPartitionOffset])
  =>
    topic = topic'
    partitions_offset = partitions_offset'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partitions_offset.values() do
      parts_str.>append(", ").>append(p.string())
    end
    "KafkaTopicOffsets: [ "
      + "topic = " + topic.string()
      + ", partitions_offset = " + parts_str
      + " ]\n"

class _KafkaTopicPartitionOffset
  let partition_id: I32
  let error_code: I16
  let timestamp: (I64 | None)
  let offset: I64

  new create(partition_id': I32, error_code': I16, offset': I64,
    timestamp': (I64 | None) = None)
  =>
    partition_id = partition_id'
    error_code = error_code'
    timestamp = timestamp'
    offset = offset'

  fun string(): String =>
    "KafkaTopicPartitionOffsets: [ "
      + "partition_id = " + partition_id.string()
      + ", error_code = " + error_code.string()
      + ", timestamp = " + timestamp.string()
      + ", offset = " + offset.string()
      + " ]\n"

class _KafkaTopicMetadata
  let topic_error_code: I16
  let topic: String
  let is_internal: (Bool | None)
  let partitions_metadata: Map[I32, _KafkaTopicPartitionMetadata val] val

  new iso create(topic_error_code': I16, topic': String,
    partitions_metadata': Map[I32, _KafkaTopicPartitionMetadata val] val,
    is_internal': (Bool | None) = None)
  =>
    topic_error_code = topic_error_code'
    topic = topic'
    is_internal = is_internal'
    partitions_metadata = partitions_metadata'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partitions_metadata.values() do
      parts_str.>append(", ").>append(p.string())
    end
    "KafkaTopicMetadata: [ "
      + "topic_error_code = " + topic_error_code.string()
      + ", topic = " + topic.string()
      + ", is_internal = " + is_internal.string()
      + ", partitions_metadata = " + parts_str
      + " ]\n"

class _KafkaTopicPartitionMetadata
  let partition_error_code: I16
  let partition_id: I32
  let leader: I32
  let replicas: Array[I32] val
  let isrs: Array[I32] val
  var request_timestamp: I64
  var error_code: I16 = 0
  var timestamp: (I64 | None) = None
  var request_offset: I64 = 0

  new iso create(partition_error_code': I16, partition_id': I32, leader': I32,
    replicas': Array[I32] val, isrs': Array[I32] val,
    request_timestamp': I64 = -2)
  =>
    partition_error_code = partition_error_code'
    partition_id = partition_id'
    leader = leader'
    replicas = replicas'
    isrs = isrs'
    request_timestamp = request_timestamp'

  fun string(): String =>
    var replicas_str = recover ref String end
    for r in replicas.values() do
      replicas_str.>append(", ").>append(r.string())
    end
    var isrs_str = recover ref String end
    for i in isrs.values() do
      isrs_str.>append(", ").>append(i.string())
    end
    "KafkaTopicPartitionMetadata: [ "
      + "partition_error_code = " + partition_error_code.string()
      + ", partition_id = " + partition_id.string()
      + ", leader = " + leader.string()
      + ", replicas = [ " + replicas_str
      + " ], isrs = [ " + isrs_str
      + " ], request_timestamp = " + request_timestamp.string()
      + " ]\n"

class _KafkaMetadata
  let brokers: Array[_KafkaBroker val] val
  let controller_id: (I32 | None)
  let cluster_id: String
  let topics_metadata: Map[String, _KafkaTopicMetadata val] val

  new val create(brokers': Array[_KafkaBroker val] val,
    topics_metadata': Map[String, _KafkaTopicMetadata val] val,
    controller_id': (I32 | None) = None, cluster_id': String = "")
  =>
    brokers = brokers'
    controller_id = controller_id'
    cluster_id = cluster_id'
    topics_metadata = topics_metadata'

  fun string(): String =>
    var brokers_str = recover ref String end
    for b in brokers.values() do
      brokers_str.>append(", ").>append(b.string())
    end
    var topm_str = recover ref String end
    for t in topics_metadata.values() do
      topm_str.>append(", ").>append(t.string())
    end
    "KafkaMetadata: [ "
      + "brokers = " + brokers_str
      + ", topics_metadata = " + topm_str
      + ", controller_id = " + controller_id.string()
      + ", cluster_id = " + cluster_id.string()
      + " ]\n"

// based on
// https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_lz4.c#L109
primitive KafkaLZ4Compression
  fun break_frame_header(logger: Logger[String], data: Array[U8] ref) ? =>
    let magic = [as U8: 0x04; 0x22; 0x4d; 0x18]

    if data.size() < 7 then
      logger(Error) and logger.log(Error,
        "Can't create bad LZ4 framing. Not enough data.")
      error
    end

    if not ((magic(0)? == data(0)?) and (magic(1)? == data(1)?) and (magic(2)? ==
      data(2)?) and (magic(3)? == data(3)?)) then
      logger(Error) and logger.log(Error,
        "Can't create bad LZ4 framing. Magic isn't correct.")
      error
    end

    var offset: USize = 4

    var flag = data(offset)?
    offset = offset + 2 // skip BD

    if ((flag >> 3) and 1) == 1 then
      offset = offset + 8
    end

    if data.size() < offset then
      logger(Error) and logger.log(Error,
        "Can't create bad LZ4 framing. Not enough data.")
      error
    end

    var hc = data(offset)?

    var bad_hc: U8 = (((XXHash.hash32(data, 0, 0, offset)? as U32) >> 8) and
      0xff).u8()

    if hc != bad_hc then
      data(offset)? = bad_hc
    end

  // TODO: Figure out way to avoid the buffer copy
  fun unbreak_frame_header(logger: Logger[String], data: Array[U8] val):
    Array[U8] val ?
  =>
    let magic = [as U8: 0x04; 0x22; 0x4d; 0x18]

    if data.size() < 7 then
      logger(Error) and logger.log(Error,
        "Can't fix bad LZ4 framing. Not enough data.")
      error
    end

    if not ((magic(0)? == data(0)?) and (magic(1)? == data(1)?) and (magic(2)? ==
      data(2)?) and (magic(3)? == data(3)?)) then
      logger(Error) and logger.log(Error,
        "Can't fix bad LZ4 framing. Magic isn't correct.")
      error
    end

    var offset: USize = 4

    var flag = data(offset)?
    offset = offset + 2 // skip BD

    if ((flag >> 3) and 1) == 1 then
      offset = offset + 8
    end

    if data.size() < offset then
      logger(Error) and logger.log(Error,
        "Can't fix bad LZ4 framing. Not enough data.")
      error
    end

    var hc = data(offset)?

    var good_hc: U8 = (((XXHash.hash32(data, 0, 4, offset - 4)? as U32) >> 8) and
       0xff).u8()

    if hc != good_hc then
      let d = recover data.clone() end
      d(offset)? = good_hc
      consume d
    else
      data
    end


// primitive for kafka message set codec
primitive _KafkaMessageSetCodecV0V1
  fun encode(conf: KafkaConfig val, wb: Writer,
    msgs: Array[ProducerKafkaMessage val] box, version: I8,
    compression: KafkaTopicCompressionType,
    compressed_message_set: Bool = false)
  =>
    let logger = conf.logger
    let wb_msgs = recover ref Writer end

    var offset: I64 = 0

    match compression
    | KafkaNoTopicCompression =>
      for msg in msgs.values() do
        encode_message(wb_msgs, msg, offset, version)
        offset = offset + 1
      end
    | KafkaGzipTopicCompression =>
      ifdef "no-zlib" then
        logger(Warn) and logger.log(Warn, "GZip/Zlib compression support not " +
          "compiled. Falling back to uncompressed messages.")
        for msg in msgs.values() do
          encode_message(wb_msgs, msg, offset, version)
          offset = offset + 1
        end
      else
        logger(Fine) and logger.log(Fine, "compressing messages using gzip.")
        encode(conf, wb_msgs, msgs, version, KafkaNoTopicCompression, true)
        let uncompress_message_set_size: USize = wb_msgs.size()
        let uncompress_message_set: Array[ByteSeq] val = wb_msgs.done()
        try
          let producer = msgs(0)?._get_created_by()
          (let maybe_timestamp, _) = msgs(0)?.get_timestamp()
          let timestamp = maybe_timestamp as I64
          let cd = ZlibCompressor.compress_array(logger, uncompress_message_set,
             uncompress_message_set_size)?
          let compressed_data = consume val cd
          let msg: ProducerKafkaMessage val = recover
            ProducerKafkaMessage(producer, None, compressed_data,
            compressed_data.size() where timestamp = timestamp) end
          encode_message(wb_msgs, msg, 0, version, KafkaGzipTopicCompression)
        else
          logger(Error) and logger.log(Error, "Error compressing messages " +
            "using GZip. Falling back to uncompressed messages.")
          wb_msgs.writev(uncompress_message_set)
        end
      end
    | KafkaSnappyTopicCompression =>
      ifdef "no-snappy" then
        logger(Warn) and logger.log(Warn, "Snappy compression support not " +
          "compiled. Falling back to uncompressed messages.")
        for msg in msgs.values() do
          encode_message(wb_msgs, msg, offset, version)
          offset = offset + 1
        end
      else
        logger(Fine) and logger.log(Fine, "compressing messages using snappy.")
        encode(conf, wb_msgs, msgs, version, KafkaNoTopicCompression, true)
        let uncompress_message_set_size: USize = wb_msgs.size()
        let uncompress_message_set: Array[ByteSeq] val = wb_msgs.done()
        try
          let producer = msgs(0)?._get_created_by()
          (let maybe_timestamp, _) = msgs(0)?.get_timestamp()
          let timestamp = maybe_timestamp as I64
          let cd = if conf.use_snappy_java_framing then
              logger(Info) and logger.log(Info,
                "Using java compatible Snappy compression.")
              SnappyCompressor.compress_array_java(logger,
                uncompress_message_set, uncompress_message_set_size)?
            else
              logger(Info) and logger.log(Info,
                "Not using java compatible Snappy compression.")
              SnappyCompressor.compress_array(logger, uncompress_message_set,
                uncompress_message_set_size)?
            end
          let compressed_data = consume val cd
          let msg: ProducerKafkaMessage val = recover
            ProducerKafkaMessage(producer, None, compressed_data,
            compressed_data.size() where timestamp = timestamp) end
          encode_message(wb_msgs, msg, 0, version, KafkaSnappyTopicCompression)
        else
          logger(Error) and logger.log(Error, "Error compressing messages " +
            "using Snappy. Falling back to uncompressed messages.")
          wb_msgs.writev(uncompress_message_set)
        end
      end
    | KafkaLZ4TopicCompression =>
      ifdef "no-lz4" then
        logger(Warn) and logger.log(Warn, "LZ4 compression support not " +
          "compiled. Falling back to uncompressed messages.")
        for msg in msgs.values() do
          encode_message(wb_msgs, msg, offset, version)
          offset = offset + 1
        end
      else
        logger(Fine) and logger.log(Fine, "compressing messages using LZ4.")
        encode(conf, wb_msgs, msgs, version, KafkaNoTopicCompression, true)
        let uncompress_message_set_size: USize = wb_msgs.size()
        let uncompress_message_set: Array[ByteSeq] val = wb_msgs.done()
        try
          let producer = msgs(0)?._get_created_by()
          (let maybe_timestamp, _) = msgs(0)?.get_timestamp()
          let timestamp = maybe_timestamp as I64
          let lz4_prefs = LZ4FPreferences
          lz4_prefs.frame_info.block_mode = LZ4FBlockIndependent()
          let compressed_data = LZ4Compressor.compress_array(logger,
            uncompress_message_set, uncompress_message_set_size, lz4_prefs)?
          let cd = if version == 0 then
              logger(Fine) and logger.log(Fine, "Breaking LZ4 header checksum" +
                " for kafka backwards compatibility.")
                recover iso
                  let d = consume ref compressed_data
                  KafkaLZ4Compression.break_frame_header(logger, d)?
                  consume d
                end
            else
              consume compressed_data
            end
          let compressed_data' = consume val cd
          let msg: ProducerKafkaMessage val = recover
            ProducerKafkaMessage(producer, None, compressed_data',
            compressed_data'.size() where timestamp = timestamp) end
          encode_message(wb_msgs, msg, 0, version, KafkaLZ4TopicCompression)
        else
          logger(Error) and logger.log(Error, "Error compressing messages " +
            "using LZ4. Falling back to uncompressed messages.")
          wb_msgs.writev(uncompress_message_set)
        end
      end
    else
      logger(Warn) and logger.log(Warn, "Unknown compression type requested. " +
        "Falling back to uncompressed messages.")
      for msg in msgs.values() do
        encode_message(wb_msgs, msg, offset, version)
        offset = offset + 1
      end
    end

    if not compressed_message_set then
      _KafkaI32Codec.encode(wb, wb_msgs.size().i32())
    end
    wb.writev(wb_msgs.done())

  fun encode_message(wb: Writer, msg: ProducerKafkaMessage val, offset: I64,
    version: I8,
    compression: KafkaTopicCompressionType = KafkaNoTopicCompression)
  =>
    let magic_byte = version
    let attributes = compression()

    let wb_temp = recover ref Writer end

    _KafkaI8Codec.encode(wb_temp, magic_byte)
    _KafkaI8Codec.encode(wb_temp, attributes)

    if version > 0 then
      (let timestamp, _) = msg.get_timestamp()
      match timestamp
      | let x: I64 => _KafkaI64Codec.encode(wb_temp, x)
      | None => _KafkaI64Codec.encode(wb_temp, -1)
      end
    end

    _KafkaByteArrayCodec.encode(wb_temp, msg.get_key(), msg.get_key_size())
    _KafkaByteArrayCodec.encode(wb_temp, msg.get_value(), msg.get_value_size())

    @printf[I32]("Value: %lu\n".cstring(), msg.get_value())

    let encoded_msg_size = wb_temp.size()
    let temp = wb_temp.done()
    let encoded_msg: Array[ByteSeq] val = consume temp

    let crc = Crc32.crc32_array(encoded_msg).i32()

    ifdef debug then
      try
        let mrb = recover Reader end
        mrb.append(encoded_msg)
        let msg_data = mrb.block(mrb.size())?
        let computed_crc = Crc32.crc32(consume msg_data)
        if crc != computed_crc.i32() then
          @printf[I32]((
            "In message encode: Error message crc did not match. read: " + crc.string() +
            ", calulated: " + computed_crc.i32().string() + "\n").cstring())
        end
      else
          @printf[I32]((
            "In message encode: Error reading message to check crc\n").cstring())
      end
    end

    let message_size = encoded_msg_size + 4

    _KafkaI64Codec.encode(wb, offset)
    _KafkaI32Codec.encode(wb, message_size.i32())
    _KafkaI32Codec.encode(wb, crc)
    wb.writev(encoded_msg)

  fun encoded_size(conf: KafkaConfig val, size: I32,
    msgs: Array[ProducerKafkaMessage val] val, version: I8): (I32,
    Array[ProducerKafkaMessage val] val, Array[ProducerKafkaMessage val] val)
  =>
    var total_size: I32 = size
    for (i, msg) in msgs.pairs() do
      let msg_size = encoded_message_size(msg, version)

      if (total_size + msg_size) < conf.max_message_size then
        total_size = total_size + msg_size
      else
        // we would exceed max message size so split the array
        return (total_size, msgs.trim(0, i), msgs.trim(i))
      end
    end

    (total_size, msgs, msgs)

  fun encoded_message_size(msg: ProducerKafkaMessage val, version: I8): I32 =>
    _KafkaI64Codec.encoded_size() // for offset
      + _KafkaI32Codec.encoded_size() // for message_size
      + _KafkaI32Codec.encoded_size() // for crc
      + _KafkaI8Codec.encoded_size() // for magic_byte
      + _KafkaI8Codec.encoded_size() // for attributes
      + if version > 0 then _KafkaI64Codec.encoded_size() else 0 end // ts
      + _KafkaByteArrayCodec.encoded_size(msg.get_key_size()) // key size
      + _KafkaByteArrayCodec.encoded_size(msg.get_value_size()) // value size


  fun decode(broker_conn: KafkaBrokerConnection tag, logger: Logger[String],
    topic_partition: KafkaTopicPartition, msg_data: Array[U8] val,
    part_state: (_KafkaTopicPartitionState | None),
    log_append_timestamp: (I64 | None) = None,
    check_crc: Bool = true,
    err_str: String = "Error decoding message set"):
    (I64, Array[KafkaMessage iso] iso^) ?
  =>
    let rb = recover ref Reader end
    rb.append(msg_data)

    var messages = recover Array[KafkaMessage iso] end

    var largest_offset_seen: I64 = -1

    while rb.size() > 12 do
      let offset = _KafkaI64Codec.decode(logger, rb,
        "error decoding offset in message set")?
      let msg_size = _KafkaI32Codec.decode(logger, rb,
        "error decoding message_size in message set")?

      if (msg_size.usize() - 4) > rb.size() then
        logger(Fine) and logger.log(Fine,
          "Received partial message. Reader doesn't have Message size of: " +
          (msg_size.usize() - 4).string() + " data in it. it has " +
          rb.size().string() + " left.")
        rb.skip(rb.size())?

        // # messages == 0 and partial message encountered; increase max bytes
        // automagically as a temporary thing to ensure progress can be made
        if messages.size() == 0 then
          match part_state
          | let ps: _KafkaTopicPartitionState => ps.max_bytes = msg_size * 2
          end
        end
        break
      end

      let crc = _KafkaI32Codec.decode(logger, rb,
        "error decoding crc in message")?

      let remaining_msg_data: Array[U8] val =
        rb.read_contiguous_bytes(msg_size.usize() - 4)?
      (let ls, let msgs) = decode_message(broker_conn, logger, topic_partition,
        offset, crc, remaining_msg_data, part_state, log_append_timestamp,
        check_crc, err_str)?
      let tmp_msgs = consume ref msgs
      while tmp_msgs.size() > 0 do
        // TODO: figure out a way to avoid the shift
        let m = tmp_msgs.shift()?
        messages.push(consume m)
      end
      if ls > largest_offset_seen then
        largest_offset_seen = ls
      end
    end

    (largest_offset_seen, consume messages)

  fun decode_message(broker_conn: KafkaBrokerConnection tag,
    logger: Logger[String], topic_partition: KafkaTopicPartition,
    offset: I64, crc: I32, msg_data: Array[U8] val,
    part_state: (_KafkaTopicPartitionState | None), log_append_timestamp: (I64 | None),
    check_crc: Bool,
    err_str: String = "Error decoding message"):
    (I64, Array[KafkaMessage iso] iso^) ?
  =>
    let rb = recover ref Reader end
    rb.append(msg_data)

    if check_crc then
      let computed_crc = Crc32.crc32(msg_data)
      if crc != computed_crc.i32() then
        // TODO: send this as an error report instead of throwing an error (i.e. proper error handling)
        logger(Error) and logger.log(Error,
          "Error message crc did not match. read: " + crc.string() +
          ", calulated: " + computed_crc.i32().string())
        logger(Error) and logger.log(Error, err_str)
        error
      end
    end

    let magic_byte = _KafkaI8Codec.decode(logger, rb,
      "error decoding magic byte in message")?
    let attributes = _KafkaI8Codec.decode(logger, rb,
      "error decoding attributes in message")?

    let compression = attributes and 0x7

    let timestamp =
      if magic_byte > 0 then
        _KafkaI64Codec.decode(logger, rb, "error decoding timestamp in message")?
      else
        None
      end

    let timestamp_type = match attributes and 0x8
      | KafkaCreateTimestamp() => KafkaCreateTimestamp
      | KafkaLogAppendTimestamp() => KafkaLogAppendTimestamp
      else
        // should never happen
        logger(Error) and logger.log(Error,
          "Error unable to determine timestamp type.")
        logger(Error) and logger.log(Error, err_str)
        error
      end

    // NOTE: defaulting to None seems like the lesser of two evils in terms of
    // performance impact vs creating a new object for every message for keys.
    let key = _KafkaByteArrayCodec.decode_default_none(logger, rb,
      "error decoding key block in message")?

    let value = _KafkaByteArrayCodec.decode(logger, rb,
      "error decoding value block in message")?

    var return_los: I64 = 0
    var return_msgs: Array[KafkaMessage iso] iso = recover return_msgs.create()
      end

    match compression
    | KafkaNoTopicCompression() =>
      logger(Fine) and logger.log(Fine,
        "Decoded uncompressed message with offset: " + offset.string() + ".")
      (let final_timestamp, let final_timestamp_type) =
        match log_append_timestamp
        | let i: I64 =>
          (i, KafkaLogAppendTimestamp)
        else
          (timestamp, timestamp_type)
        end
      return_los = offset
      return_msgs = recover [recover KafkaMessage._create(consume key,
        consume value, broker_conn, offset, crc, magic_byte, attributes,
        final_timestamp, final_timestamp_type, topic_partition) end] end
    | KafkaGzipTopicCompression() =>
      ifdef "no-zlib" then
        logger(Error) and logger.log(Error, "GZip/Zlib compression support " +
          "not compiled. Cannot decompress messages for offset: " +
          offset.string() + "!")
        error
      else
        try
          logger(Fine) and logger.log(Fine, "Decompressing GZip compressed " +
            "messages from message with offset: " + offset.string() + ".")
          let decompressed_data = ZlibDecompressor.decompress(logger, value)?
          (return_los, return_msgs) = decode(broker_conn, logger,
            topic_partition, consume val decompressed_data, part_state, if
            timestamp_type is KafkaLogAppendTimestamp then timestamp end,
            check_crc, "error decoding gzip compressed messages")?
        else
          logger(Error) and logger.log(Error,
            "Gzip decompression error! Cannot decompress messages!")
          error
        end
      end
    | KafkaSnappyTopicCompression() =>
      ifdef "no-snappy" then
        logger(Error) and logger.log(Error, "Snappy compression support not " +
          "compiled. Cannot decompress messages for offset: " +
          offset.string() + "!")
        error
      else
        try
          logger(Fine) and logger.log(Fine, "Decompressing Snappy compressed " +
            "messages from message with offset: " + offset.string() + ".")
          let decompressed_data = SnappyDecompressor.decompress_java(logger,
            value)?
          (return_los, return_msgs) = decode(broker_conn, logger,
            topic_partition, consume val decompressed_data, part_state, if
            timestamp_type is KafkaLogAppendTimestamp then timestamp end,
            check_crc, "error decoding snappy compressed messages")?
        else
          logger(Error) and logger.log(Error,
            "Snappy decompression error! Cannot decompress messages!")
          error
        end
      end
    | KafkaLZ4TopicCompression() =>
      ifdef "no-lz4" then
        logger(Error) and logger.log(Error, "LZ4 compression support not " +
          "compiled. Cannot decompress messages for offset: " +
          offset.string() + "!")
        error
      else
        try
          logger(Fine) and logger.log(Fine, "Decompressing LZ4 compressed " +
            "messages from message with offset: " + offset.string() + ".")
          let decompressed_data = if magic_byte == 0 then
            logger(Fine) and logger.log(Fine, "Unbreaking LZ4 header " +
              "checksum for kafka backwards compatibility.")
            let fixed = KafkaLZ4Compression.unbreak_frame_header(logger, value)?
            LZ4Decompressor.decompress(logger, fixed)?
          else
            LZ4Decompressor.decompress(logger, value)?
          end
          (return_los, return_msgs) = decode(broker_conn, logger,
            topic_partition, consume val decompressed_data, part_state, if
            timestamp_type is KafkaLogAppendTimestamp then timestamp end,
            check_crc, "error decoding lz4 compressed messages")?
        else
          logger(Error) and logger.log(Error,
            "LZ4 decompression error! Cannot decompress messages!")
          error
        end
      end
    else
      logger(Error) and logger.log(Error, "Unknown compression type " +
        "requested! Cannot decompress messages for offset: " +
        offset.string() + "!")
      error
    end

    if (compression == KafkaNoTopicCompression()) or (magic_byte == 0) then
      (return_los, consume return_msgs)
    else
      let adjustment = offset - return_los
      logger(Fine) and logger.log(Fine, "Applying adjustment to offsets for " +
        "inner message set. Outer offset: " + offset.string() + ", last inner: "
        + return_los.string() + ", adjustment: " + adjustment.string())
      return_msgs = recover
          let tmp_msgs = consume ref return_msgs
          for i in tmp_msgs.keys() do
            tmp_msgs(i)?._update_offset(tmp_msgs(i)?.get_offset() + adjustment)
          end
          consume tmp_msgs
        end
      (return_los + adjustment, consume return_msgs)
    end

// primitves for basic kafka protocol types (array, string, integer types, etc)
primitive _KafkaByteArrayCodec
  fun encode(wb: Writer ref, bytes: ByteSeq, num_bytes: USize) =>
    if num_bytes > 0 then
      _KafkaI32Codec.encode(wb, num_bytes.i32())
      wb.write(bytes)
    else
      _KafkaI32Codec.encode(wb, -1)
    end

  fun encode(wb: Writer ref, bytes: Array[ByteSeq] val, num_bytes: USize) =>
    if num_bytes > 0 then
      _KafkaI32Codec.encode(wb, num_bytes.i32())
      wb.writev(bytes)
    else
      _KafkaI32Codec.encode(wb, -1)
    end

  fun encode(wb: Writer ref, bytes: None, num_bytes: USize) =>
    _KafkaI32Codec.encode(wb, -1)

  fun decode_default_none(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding byte array buffer"):
    (Array[U8] val | None) ?
  =>
    let bytes_size = _KafkaI32Codec.decode(logger, rb,
      "Error decoding bytes array size")?

    if (bytes_size == -1) then
      None
    else
      try
        rb.read_contiguous_bytes(bytes_size.usize())?
      else
        logger(Error) and logger.log(Error, err_str)
        logger(Error) and logger.log(Error, "Error reading bytes array buffer")
        error
      end
    end

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding byte array buffer"): Array[U8] val ?
  =>
    let bytes_size = _KafkaI32Codec.decode(logger, rb,
      "Error decoding bytes array size")?

    if (bytes_size == -1) then
      recover val Array[U8] end
    else
      try
        rb.read_contiguous_bytes(bytes_size.usize())?
      else
        logger(Error) and logger.log(Error, err_str)
        logger(Error) and logger.log(Error, "Error reading bytes array buffer")
        error
      end
    end

  fun encoded_size(num_bytes: USize): I32 =>
    if num_bytes > 0 then
      _KafkaI32Codec.encoded_size() + num_bytes.i32()
    else
      _KafkaI32Codec.encoded_size()
    end

primitive _KafkaStringCodec
  fun encode(wb: Writer ref, str: String) =>
    if str.size() > 0 then
      _KafkaI16Codec.encode(wb, str.size().i16())
      wb.write(str)
    else
      _KafkaI16Codec.encode(wb, -1)
    end

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding string buffer"): String ?
  =>
    let str_size = _KafkaI16Codec.decode(logger, rb,
      "Error decoding string size")?

    if (str_size == -1) then
      ""
    else
      try
        String.from_array(rb.read_contiguous_bytes(str_size.usize())?)
      else
        logger(Error) and logger.log(Error, err_str)
        logger(Error) and logger.log(Error, "Error reading string buffer")
        error
      end
    end

  fun encoded_size(str: String): I32 =>
    if str.size() > 0 then
      _KafkaI16Codec.encoded_size() + str.size().i32()
    else
      _KafkaI16Codec.encoded_size()
    end


primitive _KafkaI64Codec
  fun encode(wb: Writer ref, i: I64) =>
    BigEndianEncoder.i64(wb, i)

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding i64"): I64 ?
  =>
    try
      BigEndianDecoder.i64(rb)?
    else
      logger(Error) and logger.log(Error, err_str)
      error
    end

  fun encoded_size(): I32 => 8

primitive _KafkaI32Codec
  fun encode(wb: Writer ref, i: I32) =>
    BigEndianEncoder.i32(wb, i)

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding i32"): I32 ?
  =>
    try
      BigEndianDecoder.i32(rb)?
    else
      logger(Error) and logger.log(Error, err_str)
      error
    end

  fun encoded_size(): I32 => 4

primitive _KafkaI16Codec
  fun encode(wb: Writer ref, i: I16) =>
    BigEndianEncoder.i16(wb, i)

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding i16"): I16 ?
  =>
    try
      BigEndianDecoder.i16(rb)?
    else
      logger(Error) and logger.log(Error, err_str)
      error
    end

  fun encoded_size(): I32 => 2

primitive _KafkaI8Codec
  fun encode(wb: Writer ref, i: I8) =>
    BigEndianEncoder.u8(wb, i.u8())

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding i8"): I8 ?
  =>
    try
      BigEndianDecoder.i8(rb)?
    else
      logger(Error) and logger.log(Error, err_str)
      error
    end

  fun encoded_size(): I32 => 1

primitive _KafkaBooleanCodec
  fun encode(wb: Writer ref, b: Bool) =>
    _KafkaI8Codec.encode(wb, if b then 1 else 0 end)

  fun decode(logger: Logger[String], rb: Reader ref,
    err_str: String = "Error decoding boolean"): Bool ?
  =>
    let b = _KafkaI8Codec.decode(logger, rb, err_str)?

    b == 1

  fun encoded_size(): I32 => 1

// primitive to encode kafka request header
primitive _KafkaRequestHeader
  fun encode(wb: Writer ref, api_key: I16, api_version: I16,
    correlation_id: I32, conf: KafkaConfig val)
  =>
    _KafkaI16Codec.encode(wb, api_key)
    _KafkaI16Codec.encode(wb, api_version)
    _KafkaI32Codec.encode(wb, correlation_id)
    _KafkaStringCodec.encode(wb, conf.client_name)

  fun decode(logger: Logger[String], rb: Reader): (I16, I16, I32, String) ? =>
    let header_api_key = _KafkaI16Codec.decode(logger, rb,
      "Error decoding api_key")?
    let header_api_version = _KafkaI16Codec.decode(logger, rb,
      "Error decoding api_version")?
    let correlation_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding correlation_id")?
    let client_name = _KafkaStringCodec.decode(logger, rb,
      "Error decoding client_name")?

    (header_api_key, header_api_version, correlation_id, client_name)

  fun encoded_size(client_name: String val): I32 =>
    _KafkaI16Codec.encoded_size() + _KafkaI16Codec.encoded_size() +
      _KafkaI32Codec.encoded_size() +
      _KafkaStringCodec.encoded_size(client_name)

// primitive to decode kafka response header
primitive _KafkaResponseHeader
  fun decode(logger: Logger[String], rb: Reader): I32 ? =>
    _KafkaI32Codec.decode(logger, rb, "Error decoding correlation id")?

// trait for kafka api codecs
trait val _KafkaApi
  fun apply(): (I16, I16) => (api_key(), version())

  fun string(): String

  fun api_key(): I16

  fun version(): I16

  fun min_broker_version(): String

trait val _KafkaProduceApi is _KafkaApi
  fun api_key(): I16 => 0

  fun string(): String => "Produce (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun combine_and_split_by_message_size_single(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String, partition_id: I32,
    msg: ProducerKafkaMessage val, topics_state: Map[String, _KafkaTopicState])

  fun combine_and_split_by_message_size(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val,
    topics_state: Map[String, _KafkaTopicState])

  fun encode_request(correlation_id: I32, conf: KafkaConfig val,
    msgs: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]):
    Array[ByteSeq] iso^

  fun decode_request(broker_conn: KafkaBrokerConnection tag,
    logger: Logger[String], rb: Reader):
    (I16, I32, Map[String, Map[I32, Array[KafkaMessage iso] val] val] val) ?

  fun decode_response(logger: Logger[String], rb: Reader):
    (Map[String, _KafkaTopicProduceResponse], I32) ?

primitive _KafkaProduceV0 is _KafkaProduceApi
  fun version(): I16 => 0

  fun min_broker_version(): String => "0.0.0"

  fun combine_and_split_by_message_size_single(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String, partition_id: I32,
    msg: ProducerKafkaMessage val, topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size_single(conf,
      pending_buffer, topic, partition_id, msg, topics_state where
      message_set_version = 0)

  fun _combine_and_split_by_message_size_single(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String, partition_id: I32,
    msg: ProducerKafkaMessage val, topics_state: Map[String, _KafkaTopicState],
    message_set_version: I8)
  =>
    let encoded_msg_size = _KafkaMessageSetCodecV0V1.encoded_message_size(msg,
      message_set_version)

    // get last entry from pending_buffer or else start from scratch if it is
    // empty
    (var size: I32, var num_msgs: U64, var msgs_tmp: Map[String, Map[I32,
      Array[ProducerKafkaMessage val]]]) =
      try
        pending_buffer(pending_buffer.size() - 1)?
      else
        conf.logger(Error) and conf.logger.log(Error,
          "Error getting data from pending buffer. This should never happen.")
        (_KafkaRequestHeader.encoded_size(conf.client_name)
          + _KafkaI32Codec.encoded_size() // overall message size for framing
          + _KafkaI16Codec.encoded_size() // conf.produce_acks
          + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
          + _KafkaI32Codec.encoded_size() // # topics array size
        , 0
        , Map[String, Map[I32, Array[ProducerKafkaMessage val]]])
      end

    if size == 0 then
      size = _KafkaRequestHeader.encoded_size(conf.client_name)
          + _KafkaI32Codec.encoded_size() // overall message size for framing
          + _KafkaI16Codec.encoded_size() // conf.produce_acks
          + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
          + _KafkaI32Codec.encoded_size() // # topics array size
    end


    // NOTE: This will only be called if this broker is supposed to be handling
    // the message
    try
      let topic_state = topics_state(topic)?
      let part_state = topic_state.partitions_state(partition_id)?

      // add size for encoding topic if needed
      if not msgs_tmp.contains(topic) then
        msgs_tmp(topic) = Map[I32, Array[ProducerKafkaMessage val]]

        size = size
          + _KafkaStringCodec.encoded_size(topic) // size for topic name
          + _KafkaI32Codec.encoded_size() // for # of partitions in topic
      end

      try
        // add size for encoding partition if needed
        if not msgs_tmp(topic)?.contains(partition_id) then
          size = size
               + _KafkaI32Codec.encoded_size() // partition_id
               + _KafkaI32Codec.encoded_size() // size of encoded message set
          msgs_tmp(topic)?(partition_id) = Array[ProducerKafkaMessage val]
        end

        if (size + encoded_msg_size) > conf.max_message_size then
          conf.logger(Fine) and conf.logger.log(Fine,
            "Single: messages overflowed. splitting.")
          // put in pending buffer
          let tm = msgs_tmp = Map[String, Map[I32, Array[ProducerKafkaMessage
            val]]]
          pending_buffer(pending_buffer.size() - 1)? = (size, num_msgs, tm)

          num_msgs = 0

          // reset size
          size = _KafkaRequestHeader.encoded_size(conf.client_name)
            + _KafkaI32Codec.encoded_size() // overall message size for framing
            + _KafkaI16Codec.encoded_size() // conf.produce_acks
            + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
            + _KafkaI32Codec.encoded_size() // # topics array size
            + _KafkaStringCodec.encoded_size(topic) // size for topic name
            + _KafkaI32Codec.encoded_size() // # of partitions in topic
            + _KafkaI32Codec.encoded_size() // partition_id
            + _KafkaI32Codec.encoded_size() // size of encoded message set

          msgs_tmp(topic) = Map[I32, Array[ProducerKafkaMessage val]]

          msgs_tmp(topic)?(partition_id) = Array[ProducerKafkaMessage val]

          pending_buffer.push((size, num_msgs, msgs_tmp))
        end

        size = size + encoded_msg_size
        msgs_tmp(topic)?(partition_id)?.push(msg)
      else
        conf.logger(Error) and conf.logger.log(Error,
          "Error adding messages to buffer. This should never happen.")
      end

      num_msgs = num_msgs + 1

    else
      conf.logger(Error) and conf.logger.log(Error, "Error looking up topic: " +
         topic + " in topics_state or partition: " + partition_id.string() + "
        in partitions_state. This should never happen.")
    end

    try
      pending_buffer(pending_buffer.size() - 1)? = (size, num_msgs, msgs_tmp)
    else
      conf.logger(Error) and conf.logger.log(Error,
        "Error updateding pending buffer. This should never happen.")
    end

  fun combine_and_split_by_message_size(conf: KafkaConfig val, pending_buffer:
    Array[(I32, U64, Map[String, Map[I32, Array[ProducerKafkaMessage val]]])],
    topic: String, msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val,
    topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size(conf, pending_buffer,
      topic, msgs, topics_state where message_set_version = 0)

  fun _combine_and_split_by_message_size(conf: KafkaConfig val, pending_buffer:
    Array[(I32, U64, Map[String, Map[I32, Array[ProducerKafkaMessage val]]])],
    topic: String, topic_msgs: Map[I32, Array[ProducerKafkaMessage val] iso]
    val, topics_state: Map[String, _KafkaTopicState], message_set_version: I8)
  =>
    // get last entry from pending_buffer or else start from scratch if it is
    // empty
    (var size: I32, var num_msgs: U64, var msgs_tmp: Map[String, Map[I32,
      Array[ProducerKafkaMessage val]]]) =
      try
        pending_buffer(pending_buffer.size() - 1)?
      else
        conf.logger(Error) and conf.logger.log(Error,
          "Error getting data from pending buffer. This should never happen.")
        (_KafkaRequestHeader.encoded_size(conf.client_name)
          + _KafkaI32Codec.encoded_size() // overall message size for framing
          + _KafkaI16Codec.encoded_size() // conf.produce_acks
          + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
          + _KafkaI32Codec.encoded_size() // # topics array size
        , 0
        , Map[String, Map[I32, Array[ProducerKafkaMessage val]]])
      end

    if size == 0 then
      size = _KafkaRequestHeader.encoded_size(conf.client_name)
          + _KafkaI32Codec.encoded_size() // overall message size for framing
          + _KafkaI16Codec.encoded_size() // conf.produce_acks
          + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
          + _KafkaI32Codec.encoded_size() // # topics array size
    end

    try
      let topic_state = topics_state(topic)?
      var num_partitions_included: USize = 0

      for (part_id, part_msgs) in topic_msgs.pairs() do
        try
          let part_state = topic_state.partitions_state(part_id)?
          // TODO: Reconfirm logic for when not current leader but
          // leader change (will only happen on former leader)
          if (not part_state.current_leader) and
            (not part_state.leader_change) then
            // Not current leader and not old leader of a leaderless partition
            conf.logger(Fine) and conf.logger.log(Fine, "Skipping partition: " +
               part_id.string() + " in topic: " + topic +
               " because not current leader or leader change scenario.")
            continue
          end

          // If we're in a leader change, buffer messages for the partition
          if part_state.leader_change then
            // TODO: Implement buffering of messages
            // buffer messages for when new leader is elected
            let lcpm = part_state.leader_change_pending_messages
            part_msgs.copy_to(lcpm, 0, lcpm.size(), part_msgs.size())

            conf.logger(Fine) and conf.logger.log(Fine, "Buffering partition: "
              + part_id.string() + " in topic: " + topic +
               " because leader change is happening.")
            continue
          end
        else
          conf.logger(Error) and conf.logger.log(Error,
            "Error looking up partition: " + part_id.string() + " in topic: " +
            topic + " in partitions_state. This should never happen.")
          continue
        end
        if num_partitions_included == 0 then
          if not msgs_tmp.contains(topic) then
            msgs_tmp(topic) = Map[I32, Array[ProducerKafkaMessage val]]

            size = size
               + _KafkaStringCodec.encoded_size(topic) // size for topic name
               + _KafkaI32Codec.encoded_size() // for # of partitions in topic
          end
        end
        num_partitions_included = num_partitions_included + 1
        (size, var msgs_that_fit, var msgs_that_overflow) =
          _KafkaMessageSetCodecV0V1.encoded_size(conf, size, part_msgs where
          version = message_set_version)

        try
          if not msgs_tmp(topic)?.contains(part_id) then
            size = size
                 + _KafkaI32Codec.encoded_size() // partition_id
                 + _KafkaI32Codec.encoded_size() // size of encoded message set
            msgs_tmp(topic)?(part_id) = Array[ProducerKafkaMessage val]
          end
          msgs_tmp(topic)?(part_id)?.append(msgs_that_fit)
        else
          conf.logger(Error) and conf.logger.log(Error,
            "Error adding messages to buffer. This should never happen.")
        end

        num_msgs = num_msgs + msgs_that_fit.size().u64()

        // if we'll overflow
        while msgs_that_overflow.size() != msgs_that_fit.size() do
          conf.logger(Fine) and conf.logger.log(Fine,
            msgs_that_overflow.size().string() +
            " messages overflowed. splitting.")
          // put in pending buffer
          let tm = msgs_tmp = Map[String, Map[I32, Array[ProducerKafkaMessage
            val]]]
          pending_buffer(pending_buffer.size() - 1)? = (size, num_msgs, tm)

          num_msgs = 0

          // reset size
          size = _KafkaRequestHeader.encoded_size(conf.client_name)
            + _KafkaI32Codec.encoded_size() // overall message size for framing
            + _KafkaI16Codec.encoded_size() // conf.produce_acks
            + _KafkaI32Codec.encoded_size() // conf.produce_timeout_ms
            + _KafkaI32Codec.encoded_size() // # topics array size
            + _KafkaStringCodec.encoded_size(topic) // size for topic name
            + _KafkaI32Codec.encoded_size() // # of partitions in topic
            + _KafkaI32Codec.encoded_size() // partition_id
            + _KafkaI32Codec.encoded_size() // size of encoded message set

          (size, msgs_that_fit, msgs_that_overflow) =
            _KafkaMessageSetCodecV0V1.encoded_size(conf, size,
            msgs_that_overflow where version = message_set_version)

          msgs_tmp(topic) = Map[I32, Array[ProducerKafkaMessage val]]

          try
            msgs_tmp(topic)?(part_id) = Array[ProducerKafkaMessage val]
            msgs_tmp(topic)?(part_id)?.append(msgs_that_fit)
            num_msgs = num_msgs + msgs_that_fit.size().u64()
          else
            conf.logger(Error) and conf.logger.log(Error,
              "Error adding messages to buffer. This should never happen.")
          end

          pending_buffer.push((size, num_msgs, msgs_tmp))
        end
      end
    else
      conf.logger(Error) and conf.logger.log(Error, "Error looking up topic: " +
         topic + " in topics_state. This should never happen.")
    end

    try
      pending_buffer(pending_buffer.size() - 1)? = (size, num_msgs, msgs_tmp)
    else
      conf.logger(Error) and conf.logger.log(Error,
        "Error updateding pending buffer. This should never happen.")
    end

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, msgs:
    Map[String, Map[I32, Array[ProducerKafkaMessage val]]]): Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    encode_request_body(wb, conf, msgs where message_set_version = 0)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun encode_request_body(wb: Writer, conf: KafkaConfig val, msgs: Map[String,
    Map[I32, Array[ProducerKafkaMessage val]]], message_set_version: I8)
  =>
    _KafkaI16Codec.encode(wb, conf.produce_acks)
    _KafkaI32Codec.encode(wb, conf.produce_timeout_ms.i32())

    // Topics/partitions are valid and no repeats due to use of
    // KafkaProducerMapping and maps
    _KafkaI32Codec.encode(wb, msgs.size().i32())
    for (topic, topic_msgs) in msgs.pairs() do
      let compression = try
          conf.topics(topic)?.compression
        else
          conf.logger(Error) and conf.logger.log(Error, "Topic: " + topic +
            " is not a valid producer topic (this should never happen). " +
            "Ignoring.")
          continue
        end

      _KafkaStringCodec.encode(wb, topic)

      _KafkaI32Codec.encode(wb, topic_msgs.size().i32())

      for (part_id, part_msgs) in topic_msgs.pairs() do
        _KafkaI32Codec.encode(wb, part_id)

        _KafkaMessageSetCodecV0V1.encode(conf, wb, part_msgs,
          message_set_version, compression)
      end
    end

  fun decode_request(broker_conn: KafkaBrokerConnection tag,
    logger: Logger[String], rb: Reader):
    (I16, I32, Map[String, Map[I32, Array[KafkaMessage iso] val] val] val) ?
  =>
    let produce_acks = _KafkaI16Codec.decode(logger, rb,
      "Error decoding produce_acks")?
    let produce_timeout = _KafkaI32Codec.decode(logger, rb,
      "Error decoding produce_timeout")?

    var num_topics = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics produced num elements")?

    let msgs: Map[String, Map[I32, Array[KafkaMessage iso] val] val] iso =
      recover msgs.create() end

    while num_topics > 0 do
      let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?

      let topic_msgs: Map[I32, Array[KafkaMessage iso] val] iso = recover
        topic_msgs.create() end

      var num_partitions = _KafkaI32Codec.decode(logger, rb,
        "Error decoding partition requests num elements")?
      logger(Fine) and logger.log(Fine, "Have " + num_partitions.string() +
        " partitions for topic " + topic)

      while num_partitions > 0 do
        let partition_id = _KafkaI32Codec.decode(logger, rb,
          "error decoding partition")?

        let message_set_size = _KafkaI32Codec.decode(logger, rb,
          "error decoding message set size")?

        if message_set_size.usize() > rb.size() then
          logger(Error) and logger.log(Error,
            "Reader doesn't have Message set size of: " +
            message_set_size.string() + " data in it. it has " +
            rb.size().string() + " left.")
          error
        end

        (let largest_offset_seen, let messages) =
          _KafkaMessageSetCodecV0V1.decode(broker_conn, logger,
          KafkaTopicPartition(topic, partition_id),
          rb.block(message_set_size.usize())?, None
          where err_str = "error decoding messages")?

        topic_msgs(partition_id) = consume val messages

        num_partitions = num_partitions - 1
      end

      msgs(topic) = consume topic_msgs

      num_topics = num_topics - 1
    end


    (produce_acks, produce_timeout, consume val msgs)

  fun decode_response(logger: Logger[String], rb: Reader):
    (Map[String, _KafkaTopicProduceResponse], I32) ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topic produce response num elements")?

    let topics_produce_responses = Map[String, _KafkaTopicProduceResponse]

    while num_elems > 0 do
      (let topic, let produce_response) = decode_topic_produce_response(logger,
        rb)?
      topics_produce_responses.insert(topic, produce_response)?
      num_elems = num_elems - 1
    end

    (topics_produce_responses, -1)

  fun decode_topic_produce_response(logger: Logger[String], rb: Reader):
    (String, _KafkaTopicProduceResponse) ?
  =>
    let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition responses num elements")?

    let topics_partition_responses = Map[I32,
      _KafkaTopicProducePartitionResponse]

    while num_elems > 0 do
      (let part, let part_resp) =
        decode_topic_produce_partition_response(logger, rb)?
      topics_partition_responses.insert(part, part_resp)?
      num_elems = num_elems - 1
    end

    (topic, _KafkaTopicProduceResponse(topic, topics_partition_responses))

  fun decode_topic_produce_partition_response(logger: Logger[String],
    rb: Reader): (I32, _KafkaTopicProducePartitionResponse) ?
  =>
    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition_id")?
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error_code")?
    let offset = _KafkaI64Codec.decode(logger, rb, "Error decoding offset")?

    (partition_id, _KafkaTopicProducePartitionResponse(partition_id, error_code,
       offset))

primitive _KafkaProduceV1 is _KafkaProduceApi
  fun version(): I16 => 1

  fun min_broker_version(): String => "0.9.0"

  fun combine_and_split_by_message_size_single(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String, partition_id: I32,
    msg: ProducerKafkaMessage val, topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size_single(conf,
      pending_buffer, topic, partition_id, msg, topics_state where
      message_set_version = 0)

  fun combine_and_split_by_message_size(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val,
    topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size(conf, pending_buffer,
      topic, msgs, topics_state where message_set_version = 0)

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, msgs:
    Map[String, Map[I32, Array[ProducerKafkaMessage val]]]): Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    _KafkaProduceV0.encode_request_body(wb, conf, msgs where message_set_version
       = 0)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun decode_request(broker_conn: KafkaBrokerConnection tag,
    logger: Logger[String], rb: Reader):
    (I16, I32, Map[String, Map[I32, Array[KafkaMessage iso] val] val] val) ?
  =>
    _KafkaProduceV0.decode_request(broker_conn, logger, rb)?

  fun decode_response(logger: Logger[String], rb: Reader):
    (Map[String, _KafkaTopicProduceResponse], I32) ?
  =>
    (let topic_produce_responses, _) = _KafkaProduceV0.decode_response(logger,
      rb)?

    let throttle_time = _KafkaI32Codec.decode(logger, rb,
      "Error decoding throttle time")?

    (topic_produce_responses, throttle_time)

primitive _KafkaProduceV2 is _KafkaProduceApi
  fun version(): I16 => 2

  fun min_broker_version(): String => "0.10.0"

  fun combine_and_split_by_message_size_single(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String, partition_id: I32,
    msg: ProducerKafkaMessage val, topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size_single(conf,
      pending_buffer, topic, partition_id, msg, topics_state where
      message_set_version = 1)

  fun combine_and_split_by_message_size(conf: KafkaConfig val,
    pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])], topic: String,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val,
    topics_state: Map[String, _KafkaTopicState])
  =>
    _KafkaProduceV0._combine_and_split_by_message_size(conf, pending_buffer,
      topic, msgs, topics_state where message_set_version = 1)

  fun encode_request(correlation_id: I32, conf: KafkaConfig val,
    msgs: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]):
    Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    _KafkaProduceV0.encode_request_body(wb, conf, msgs where message_set_version
       = 1)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun decode_request(broker_conn: KafkaBrokerConnection tag,
    logger: Logger[String], rb: Reader):
    (I16, I32, Map[String, Map[I32, Array[KafkaMessage iso] val] val] val) ?
  =>
    _KafkaProduceV0.decode_request(broker_conn, logger, rb)?

  fun decode_response(logger: Logger[String], rb: Reader):
    (Map[String, _KafkaTopicProduceResponse], I32) ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topic produce response num elements")?

    let topics_produce_responses = Map[String, _KafkaTopicProduceResponse]

    while num_elems > 0 do
      (let topic, let produce_response) = decode_topic_produce_response(logger,
        rb)?
      topics_produce_responses.insert(topic, produce_response)?
      num_elems = num_elems - 1
    end

    let throttle_time = _KafkaI32Codec.decode(logger, rb,
      "Error decoding throttle time")?

    (topics_produce_responses, throttle_time)

  fun decode_topic_produce_response(logger: Logger[String], rb: Reader):
    (String, _KafkaTopicProduceResponse) ?
  =>
    let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition responses num elements")?

    let topics_partition_responses = Map[I32,
      _KafkaTopicProducePartitionResponse]

    while num_elems > 0 do
      (let part, let part_resp) =
        decode_topic_produce_partition_response(logger, rb)?
      topics_partition_responses.insert(part, part_resp)?
      num_elems = num_elems - 1
    end

    (topic, _KafkaTopicProduceResponse(topic, topics_partition_responses))

  fun decode_topic_produce_partition_response(logger: Logger[String],
    rb: Reader): (I32, _KafkaTopicProducePartitionResponse) ?
  =>
    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition_id")?
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error_code")?
    let offset = _KafkaI64Codec.decode(logger, rb, "Error decoding offset")?
    let timestamp = _KafkaI64Codec.decode(logger, rb,
      "Error decoding timestamp")?

    (partition_id, _KafkaTopicProducePartitionResponse(partition_id, error_code,
       offset, timestamp))

trait val _KafkaFetchApi is _KafkaApi
  fun api_key(): I16 => 1

  fun string(): String => "Fetch (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState]): (Array[ByteSeq] iso^ | None)

  fun decode_response(broker_conn: KafkaBrokerConnection tag, check_crc: Bool,
    logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (I32, Map[String, _KafkaTopicFetchResult]) ?

primitive _KafkaFetchV0 is _KafkaFetchApi
  fun version(): I16 => 0

  fun min_broker_version(): String => "0.0.0"

  // topics_state needs to be writeable due to the need to reset max_bytes on
  // each partition in case it was temporarily raised
  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState]): (Array[ByteSeq] iso^ | None)
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    let num_topics_encoded = encode_request_body(wb, conf, topics_state)

    if num_topics_encoded == 0 then
      None
    else
      let wb_msg = recover ref Writer end
      _KafkaI32Codec.encode(wb_msg, wb.size().i32())
      wb_msg.writev(wb.done())
      wb_msg.done()
    end

  fun encode_request_body(wb: Writer, conf: KafkaConfig val,
    topics_state: Map[String, _KafkaTopicState]): I32
  =>
    conf.logger(Fine) and conf.logger.log(Fine,
      "Encoding request body for fetch request")
    let wb_topics = recover ref Writer end
    var num_topics_encoded: I32 = 0
    for (topic, topic_state) in topics_state.pairs() do
      if not conf.consumer_topics.contains(topic) then
        continue
      end
      conf.logger(Fine) and conf.logger.log(Fine, "Encoding request topic: " +
        topic)
      let num_partitions_encoded = encode_topic_partitions(conf, wb_topics,
        topic, topic_state.partitions_state)
      if num_partitions_encoded > 0 then
        num_topics_encoded = num_topics_encoded + 1
      end
    end

    if num_topics_encoded > 0 then
      _KafkaI32Codec.encode(wb, conf.replica_id)
      // don't let kafka block for more than 1 ms so we can keep doing other
      // meaningful work even when no data is available
      // TODO: maybe this should instead be 0 so kafka doesn't block at all?
      _KafkaI32Codec.encode(wb, 1)
      _KafkaI32Codec.encode(wb, conf.min_fetch_bytes.i32())
      _KafkaI32Codec.encode(wb, num_topics_encoded)
      wb.writev(wb_topics.done())
    end

    num_topics_encoded

  fun encode_topic_partitions(conf: KafkaConfig val, wb: Writer, topic: String,
    parts_state: Map[I32, _KafkaTopicPartitionState]): I32
  =>
    let wb_partitions = recover ref Writer end
    var num_partitions_encoded: I32 = 0
    for (part_id, part_state) in parts_state.pairs() do
      if (not part_state.current_leader) or part_state.paused then
        continue
      end
      if num_partitions_encoded == 0 then
        _KafkaStringCodec.encode(wb, topic)
      end
      num_partitions_encoded = num_partitions_encoded + 1
      _KafkaI32Codec.encode(wb_partitions, part_state.partition_id)
      _KafkaI64Codec.encode(wb_partitions, part_state.request_offset)
      _KafkaI32Codec.encode(wb_partitions, part_state.max_bytes)
      conf.logger(Fine) and conf.logger.log(Fine, "Encoding request partition: "
         + part_state.partition_id.string() + ", request_offset: " +
        part_state.request_offset.string() + ", max_bytes: " +
        part_state.max_bytes.string())

      // reset fetch max bytes for partition in case it was temporarily
      // increased
      part_state.max_bytes = conf.partition_fetch_max_bytes
    end

    if num_partitions_encoded > 0 then
      _KafkaI32Codec.encode(wb, num_partitions_encoded)
      wb.writev(wb_partitions.done())
    end

    num_partitions_encoded

  fun decode_response(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool,
    logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (I32, Map[String, _KafkaTopicFetchResult]) ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topic fetch result num elements")?

    let topics_fetch_results = Map[String, _KafkaTopicFetchResult]

    while num_elems > 0 do
      (let topic, let fetch_result) = decode_topic_fetch_result(broker_conn,
        check_crc, logger, rb, topics_state)?
      topics_fetch_results.insert(topic, fetch_result)?
      num_elems = num_elems - 1
    end

    (-1, topics_fetch_results)

  fun decode_topic_fetch_result(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool, logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (String, _KafkaTopicFetchResult) ?
  =>
    let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?
    let topic_state = topics_state(topic)?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition responses num elements")?

    let topics_partition_responses = Map[I32, _KafkaTopicPartitionResponse]

    while num_elems > 0 do
      (let part, let part_resp) = decode_topic_partition_response(broker_conn,
        check_crc, logger, rb, topic, topic_state.partitions_state)?
      topics_partition_responses.insert(part, part_resp)?
      num_elems = num_elems - 1
    end

    (topic, _KafkaTopicFetchResult(topic, topics_partition_responses))

  fun decode_topic_partition_response(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool,
    logger: Logger[String], rb: Reader, topic: String, parts_state: Map[I32,
    _KafkaTopicPartitionState]): (I32, _KafkaTopicPartitionResponse) ?
  =>
    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition")?
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error code")?
    let high_watermark = _KafkaI64Codec.decode(logger, rb,
      "Error decoding high watermark")?
    let message_set_size = _KafkaI32Codec.decode(logger, rb,
      "error decoding message set size")?

    if message_set_size.usize() > rb.size() then
      logger(Error) and logger.log(Error,
        "Reader doesn't have Message set size of: " + message_set_size.string()
        + " data in it. it has " + rb.size().string() + " left.")
      error
    end

    (let largest_offset_seen, let messages) =
      _KafkaMessageSetCodecV0V1.decode(broker_conn, logger,
      KafkaTopicPartition(topic, partition_id),
      rb.read_contiguous_bytes(message_set_size.usize())?,
      parts_state(partition_id)? where err_str = "error decoding messages", check_crc = check_crc)?

    (partition_id, _KafkaTopicPartitionResponse(partition_id, error_code,
      high_watermark, largest_offset_seen, consume val messages))

primitive _KafkaFetchV1 is _KafkaFetchApi
  fun version(): I16 => 1

  fun min_broker_version(): String => "0.9.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState]): (Array[ByteSeq] iso^ | None)
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    let num_topics_encoded = _KafkaFetchV0.encode_request_body(wb, conf,
      topics_state)

    if num_topics_encoded == 0 then
      None
    else
      let wb_msg = recover ref Writer end
      _KafkaI32Codec.encode(wb_msg, wb.size().i32())
      wb_msg.writev(wb.done())
      wb_msg.done()
    end

  fun decode_response(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool,
    logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (I32, Map[String, _KafkaTopicFetchResult]) ?
  =>
    let throttle_time_ms = _KafkaI32Codec.decode(logger, rb,
      "Error decoding throttle time")?
    (_, let topics_fetch_results) = _KafkaFetchV0.decode_response(broker_conn,
      check_crc, logger, rb, topics_state)?

    (throttle_time_ms, topics_fetch_results)

primitive _KafkaFetchV2 is _KafkaFetchApi
  fun version(): I16 => 2

  fun min_broker_version(): String => "0.10.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState]): (Array[ByteSeq] iso^ | None)
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    let num_topics_encoded = _KafkaFetchV0.encode_request_body(wb, conf,
      topics_state)

    if num_topics_encoded == 0 then
      None
    else
      let wb_msg = recover ref Writer end
      _KafkaI32Codec.encode(wb_msg, wb.size().i32())
      wb_msg.writev(wb.done())
      wb_msg.done()
    end

  fun decode_response(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool,
    logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (I32, Map[String, _KafkaTopicFetchResult]) ?
  =>
    _KafkaFetchV1.decode_response(broker_conn, check_crc, logger, rb, topics_state)?

primitive _KafkaFetchV3 is _KafkaFetchApi
  fun version(): I16 => 3

  fun min_broker_version(): String => "0.10.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState]): (Array[ByteSeq] iso^ | None)
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    let num_topics_encoded = encode_request_body(wb, conf, topics_state)

    if num_topics_encoded == 0 then
      None
    else
      let wb_msg = recover ref Writer end
      _KafkaI32Codec.encode(wb_msg, wb.size().i32())
      wb_msg.writev(wb.done())
      wb_msg.done()
    end

  fun encode_request_body(wb: Writer, conf: KafkaConfig val,
    topics_state: Map[String, _KafkaTopicState]): I32
  =>
    conf.logger(Fine) and conf.logger.log(Fine,
      "Encoding request body for fetch request")
    let wb_topics = recover ref Writer end
    var num_topics_encoded: I32 = 0
    for (topic, topic_state) in topics_state.pairs() do
      if not conf.consumer_topics.contains(topic) then
        continue
      end
      conf.logger(Fine) and conf.logger.log(Fine, "Encoding request topic: " +
        topic)
      let num_partitions_encoded = _KafkaFetchV0.encode_topic_partitions(conf,
        wb, topic, topic_state.partitions_state)
      if num_partitions_encoded > 0 then
        num_topics_encoded = num_topics_encoded + 1
      end
    end

    if num_topics_encoded > 0 then
      _KafkaI32Codec.encode(wb, conf.replica_id)
      // don't let kafka block for more than 1 ms so we can keep doing other
      // meaningful work even when no data is available
      // TODO: maybe this should instead be 0 so kafka doesn't block at all?
      _KafkaI32Codec.encode(wb, 1)
      _KafkaI32Codec.encode(wb, conf.min_fetch_bytes.i32())
      _KafkaI32Codec.encode(wb, conf.max_fetch_bytes.i32())
      _KafkaI32Codec.encode(wb, num_topics_encoded)
      wb.writev(wb_topics.done())
    end

    num_topics_encoded

  fun decode_response(broker_conn: KafkaBrokerConnection tag,
    check_crc: Bool,
    logger: Logger[String], rb: Reader,
    topics_state: Map[String, _KafkaTopicState]):
    (I32, Map[String, _KafkaTopicFetchResult]) ?
  =>
    _KafkaFetchV1.decode_response(broker_conn, check_crc, logger, rb, topics_state)?

trait val _KafkaOffsetsApi is _KafkaApi
  fun api_key(): I16 => 2

  fun string(): String => "Offsets (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState] box): Array[ByteSeq] iso^

  fun decode_request(logger: Logger[String], rb: Reader): (I32, Map[String,
    Map[I32, (I64, I32)] val] val) ?

  fun decode_response(logger: Logger[String], rb: Reader):
    Array[_KafkaTopicOffset] ?

primitive _KafkaOffsetsV0 is _KafkaOffsetsApi
  fun version(): I16 => 0

  fun min_broker_version(): String => "0.0.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val,
    topics_state: Map[String, _KafkaTopicState] box): Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    encode_request_body(wb, conf, topics_state)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun encode_request_body(wb: Writer, conf: KafkaConfig val,
    topics_state: Map[String, _KafkaTopicState] box)
  =>
    let wb_topics = recover ref Writer end
    var num_topics_encoded: I32 = 0
    for (topic, topic_state) in topics_state.pairs() do
      let num_partitions_encoded = encode_topic_partitions(wb_topics, topic,
        topic_state.partitions_state)
      if num_partitions_encoded > 0 then
        num_topics_encoded = num_topics_encoded + 1
      end
    end

    _KafkaI32Codec.encode(wb, conf.replica_id)
    _KafkaI32Codec.encode(wb, num_topics_encoded)
    wb.writev(wb_topics.done())

  fun encode_topic_partitions(wb: Writer, topic: String,
    parts_state: Map[I32, _KafkaTopicPartitionState] box): I32
  =>
    let wb_partitions = recover ref Writer end
    var num_partitions_encoded: I32 = 0
    for (part_id, part_state) in parts_state.pairs() do
      if (not part_state.current_leader) then
        continue
      end
      if num_partitions_encoded == 0 then
        // this is intentional and must happen before the num partitions and
        // partitions array is encoded
        _KafkaStringCodec.encode(wb, topic)
      end
      num_partitions_encoded = num_partitions_encoded + 1
      _KafkaI32Codec.encode(wb_partitions, part_state.partition_id)
      _KafkaI64Codec.encode(wb_partitions, part_state.request_timestamp)
      _KafkaI32Codec.encode(wb_partitions, 1 /* always request only 1 offset */)
    end

    if num_partitions_encoded > 0 then
      _KafkaI32Codec.encode(wb, num_partitions_encoded)
      wb.writev(wb_partitions.done())
    end

    num_partitions_encoded

  fun decode_request(logger: Logger[String], rb: Reader): (I32, Map[String,
    Map[I32, (I64, I32)] val] val) ? =>
    let replica_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding replica_id")?

    var num_topics = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics offsets num elements")?

    let offset_topics: Map[String, Map[I32, (I64, I32)] val] iso = recover
      offset_topics.create() end

    while num_topics > 0 do
      let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?

      let offset_topic_partitions: Map[I32, (I64, I32)] iso = recover
        offset_topic_partitions.create() end

      var num_partitions = _KafkaI32Codec.decode(logger, rb,
        "Error decoding partition requests num elements")?
      logger(Fine) and logger.log(Fine, "Have " + num_partitions.string() +
        " partitions for topic " + topic)

      while num_partitions > 0 do
        let partition_id = _KafkaI32Codec.decode(logger, rb,
          "error decoding partition")?
        let request_timestamp = _KafkaI64Codec.decode(logger, rb,
          "error decoding request_timestamp")?
        let num_offsets_requested = _KafkaI32Codec.decode(logger, rb,
          "error decoding num_offsets_requested")?

        offset_topic_partitions(partition_id) = (request_timestamp,
          num_offsets_requested)

        num_partitions = num_partitions - 1
      end

      offset_topics(topic) = consume offset_topic_partitions

      num_topics = num_topics - 1
    end

    (replica_id, consume val offset_topics)

  fun decode_response(logger: Logger[String], rb: Reader):
    Array[_KafkaTopicOffset] ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics offsets num elements")?

    let topics_offsets = Array[_KafkaTopicOffset]

    while num_elems > 0 do
      topics_offsets.push(decode_topic_offset(logger, rb)?)
      num_elems = num_elems - 1
    end

    topics_offsets

  fun decode_topic_offset(logger: Logger[String], rb: Reader):
    _KafkaTopicOffset ?
  =>
    let topic = _KafkaStringCodec.decode(logger, rb,
      "Error decoding topic name")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition offsets num elements")?

    let partitions_offset = Array[_KafkaTopicPartitionOffset]

    while num_elems > 0 do
      partitions_offset.push(decode_partition_offset(logger, rb)?)
      num_elems = num_elems - 1
    end

    _KafkaTopicOffset(topic, partitions_offset)

  fun decode_partition_offset(logger: Logger[String], rb: Reader):
    _KafkaTopicPartitionOffset ?
  =>
    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition")?
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error_code")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding offsets num elements")?

    var offset: I64 = 0

    // keep only one offset (should be easy since we always only request one)
    while num_elems > 0 do
      offset = _KafkaI64Codec.decode(logger, rb, "Error decoding offset")?
      num_elems = num_elems - 1
    end

    _KafkaTopicPartitionOffset(partition_id, error_code, offset)

primitive _KafkaOffsetsV1 is _KafkaOffsetsApi
  fun version(): I16 => 1

  fun min_broker_version(): String => "0.10.1.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState] box): Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    encode_request_body(wb, conf, topics_state)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun encode_request_body(wb: Writer, conf: KafkaConfig val, topics_state:
    Map[String, _KafkaTopicState] box) =>
    let wb_topics = recover ref Writer end
    var num_topics_encoded: I32 = 0
    for (topic, topic_state) in topics_state.pairs() do
      let num_partitions_encoded = encode_topic_partitions(wb_topics, topic,
        topic_state.partitions_state)
      if num_partitions_encoded > 0 then
        num_topics_encoded = num_topics_encoded + 1
      end
    end

    _KafkaI32Codec.encode(wb, conf.replica_id)
    _KafkaI32Codec.encode(wb, num_topics_encoded)
    wb.writev(wb_topics.done())

  fun encode_topic_partitions(wb: Writer, topic: String, parts_state: Map[I32,
    _KafkaTopicPartitionState] box): I32
  =>
    let wb_partitions = recover ref Writer end
    var num_partitions_encoded: I32 = 0
    for (part_id, part_state) in parts_state.pairs() do
      if (not part_state.current_leader) then
        continue
      end
      if num_partitions_encoded == 0 then
        // this is intentional and must happen before the num partitions and
        // partitions array is encoded
        _KafkaStringCodec.encode(wb, topic)
      end
      num_partitions_encoded = num_partitions_encoded + 1
      _KafkaI32Codec.encode(wb_partitions, part_state.partition_id)
      _KafkaI64Codec.encode(wb_partitions, part_state.request_timestamp)
    end

    if num_partitions_encoded > 0 then
      _KafkaI32Codec.encode(wb, num_partitions_encoded)
      wb.writev(wb_partitions.done())
    end

    num_partitions_encoded

  fun decode_request(logger: Logger[String], rb: Reader):
    (I32, Map[String, Map[I32, (I64, I32)] val] val) ?
  =>
    let replica_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding replica_id")?

    var num_topics = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics offsets num elements")?

    let offset_topics: Map[String, Map[I32, (I64, I32)] val] iso = recover
      offset_topics.create() end

    while num_topics > 0 do
      let topic = _KafkaStringCodec.decode(logger, rb, "error decoding topic")?

      let offset_topic_partitions: Map[I32, (I64, I32)] iso = recover
        offset_topic_partitions.create() end

      var num_partitions = _KafkaI32Codec.decode(logger, rb,
        "Error decoding partition requests num elements")?
      logger(Fine) and logger.log(Fine, "Have " + num_partitions.string() +
        " partitions for topic " + topic)

      while num_partitions > 0 do
        let partition_id = _KafkaI32Codec.decode(logger, rb,
          "error decoding partition")?
        let request_timestamp = _KafkaI64Codec.decode(logger, rb,
          "error decoding request_timestamp")?
        let num_offsets_requested: I32 = 1

        offset_topic_partitions(partition_id) = (request_timestamp,
          num_offsets_requested)

        num_partitions = num_partitions - 1
      end

      offset_topics(topic) = consume offset_topic_partitions

      num_topics = num_topics - 1
    end

    (replica_id, consume val offset_topics)

  fun decode_response(logger: Logger[String], rb: Reader):
    Array[_KafkaTopicOffset] ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics offsets num elements")?

    let topics_offsets = Array[_KafkaTopicOffset]

    while num_elems > 0 do
      topics_offsets.push(decode_topic_offset(logger, rb)?)
      num_elems = num_elems - 1
    end

    topics_offsets

  fun decode_topic_offset(logger: Logger[String], rb: Reader):
    _KafkaTopicOffset ? =>
    let topic = _KafkaStringCodec.decode(logger, rb,
      "Error decoding topic name")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition offsets num elements")?

    let partitions_offset = Array[_KafkaTopicPartitionOffset]

    while num_elems > 0 do
      partitions_offset.push(decode_partition_offset(logger, rb)?)
      num_elems = num_elems - 1
    end

    _KafkaTopicOffset(topic, partitions_offset)

  fun decode_partition_offset(logger: Logger[String], rb: Reader):
    _KafkaTopicPartitionOffset ?
  =>
    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition")?
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error_code")?
    let timestamp = _KafkaI64Codec.decode(logger, rb,
      "Error decoding timestamp")?

    let offset = _KafkaI64Codec.decode(logger, rb, "Error decoding offset")?

    _KafkaTopicPartitionOffset(partition_id, error_code, offset)

trait val _KafkaMetadataApi is _KafkaApi
  fun api_key(): I16 => 3

  fun string(): String => "Metadata (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val): Array[ByteSeq]
     iso^

  fun decode_response(logger: Logger[String], rb: Reader): _KafkaMetadata val ?

primitive _KafkaMetadataV0 is _KafkaMetadataApi
  fun version(): I16 => 0

  fun min_broker_version(): String => "0.0.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val):
    Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    encode_request_body(wb, conf)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun encode_request_body(wb: Writer, conf: KafkaConfig val) =>
    _KafkaI32Codec.encode(wb, conf.topics.size().i32())
    for topic in conf.topics.keys() do
      _KafkaStringCodec.encode(wb, topic)
    end

  fun decode_response(logger: Logger[String], rb: Reader): _KafkaMetadata val ?
  =>
    let brokers = decode_brokers(logger, rb)?
    let topics_metadata = decode_topics_metadata(logger, rb)?
    _KafkaMetadata(brokers, topics_metadata)

  fun decode_topics_metadata(logger: Logger[String], rb: Reader):
    Map[String, _KafkaTopicMetadata val] val ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics metadata num elements")?

    let topics_metadata = recover iso Map[String, _KafkaTopicMetadata val] end

    while num_elems > 0 do
      (let topic, let topic_meta) = decode_topic_metadata(logger, rb)?
      topics_metadata.insert(topic, topic_meta)?
      num_elems = num_elems - 1
    end

    topics_metadata

  fun decode_topic_metadata(logger: Logger[String], rb: Reader):
    (String, _KafkaTopicMetadata val) ?
  =>
    let topic_error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding topic error code")?

    let topic = _KafkaStringCodec.decode(logger, rb)?

    let partitions_metadata = decode_partitions_metadata(logger, rb)?

    (topic, _KafkaTopicMetadata(topic_error_code, topic, partitions_metadata))

  fun decode_partitions_metadata(logger: Logger[String], rb: Reader):
    Map[I32, _KafkaTopicPartitionMetadata val] val ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partitions metadata num elements")?

    let partitions_metadata = recover iso Map[I32, _KafkaTopicPartitionMetadata
      val] end

    while num_elems > 0 do
      (let part_id, let part_meta) = decode_partition_metadata(logger, rb)?
      partitions_metadata.insert(part_id, part_meta)?
      num_elems = num_elems - 1
    end

    partitions_metadata

  fun decode_partition_metadata(logger: Logger[String], rb: Reader):
    (I32, _KafkaTopicPartitionMetadata val) ?
  =>
    let partition_error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding partition error code")?

    let partition_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding partition_id")?

    let leader = _KafkaI32Codec.decode(logger, rb, "Error decoding leader")?

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding replicas num elements")?

    let replicas = recover iso Array[I32] end

    while num_elems > 0 do
      replicas.push(_KafkaI32Codec.decode(logger, rb, "Error decoding replica")?)
      num_elems = num_elems - 1
    end

    num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding isrs num elements")?

    let isrs = recover iso Array[I32] end

    while num_elems > 0 do
      isrs.push(_KafkaI32Codec.decode(logger, rb, "Error decoding isr")?)
      num_elems = num_elems - 1
    end

    (partition_id, _KafkaTopicPartitionMetadata(partition_error_code,
      partition_id, leader, consume replicas, consume isrs))

  fun decode_brokers(logger: Logger[String], rb: Reader):
    Array[_KafkaBroker val] val ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding brokers num elements")?

    let brokers = recover iso Array[_KafkaBroker val] end

    while num_elems > 0 do
      brokers.push(decode_broker(logger, rb)?)
      num_elems = num_elems - 1
    end

    brokers

  fun decode_broker(logger: Logger[String], rb: Reader): _KafkaBroker val ? =>
    let node_id = _KafkaI32Codec.decode(logger, rb, "Error decoding node_id")?

    let host = _KafkaStringCodec.decode(logger, rb)?

    let port = _KafkaI32Codec.decode(logger, rb, "Error decoding port")?

    recover val _KafkaBroker(node_id, host, port) end

primitive _KafkaMetadataV1 is _KafkaMetadataApi
  fun version(): I16 => 1

  fun min_broker_version(): String => "0.0.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val):
    Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    encode_request_body(wb, conf)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun encode_request_body(wb: Writer, conf: KafkaConfig val) =>
    if conf.topics.size() == 0 then
      _KafkaI32Codec.encode(wb, -1)
    else
      _KafkaI32Codec.encode(wb, conf.topics.size().i32())

      for topic in conf.topics.keys() do
        _KafkaStringCodec.encode(wb, topic)
      end
    end

  fun decode_response(logger: Logger[String], rb: Reader): _KafkaMetadata val ?
  =>
    let brokers = decode_brokers(logger, rb)?
    let controller_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding controller_id")?

    let topics_metadata = decode_topics_metadata(logger, rb)?

    _KafkaMetadata(brokers, topics_metadata, controller_id)

  fun decode_topics_metadata(logger: Logger[String], rb: Reader):
    Map[String, _KafkaTopicMetadata val] val ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics metadata num elements")?

    let topics_metadata = recover iso Map[String, _KafkaTopicMetadata val] end

    while num_elems > 0 do
      (let topic, let topic_meta) = decode_topic_metadata(logger, rb)?
      topics_metadata.insert(topic, topic_meta)?
      num_elems = num_elems - 1
    end

    topics_metadata

  fun decode_topic_metadata(logger: Logger[String], rb: Reader):
    (String, _KafkaTopicMetadata val) ?
  =>
    let topic_error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding topic error code")?

    let topic = _KafkaStringCodec.decode(logger, rb)?

    let is_internal = _KafkaBooleanCodec.decode(logger, rb)?

    let partitions_metadata =
      _KafkaMetadataV0.decode_partitions_metadata(logger, rb)?

    (topic, _KafkaTopicMetadata(topic_error_code, topic, partitions_metadata,
      is_internal))

  fun decode_brokers(logger: Logger[String], rb: Reader):
    Array[_KafkaBroker val] val ?
  =>
    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding topics brokers num elements")?

    let brokers = recover iso Array[_KafkaBroker val] end

    while num_elems > 0 do
      brokers.push(decode_broker(logger, rb)?)
      num_elems = num_elems - 1
    end

    brokers

  fun decode_broker(logger: Logger[String], rb: Reader): _KafkaBroker val ? =>
    let node_id = _KafkaI32Codec.decode(logger, rb, "Error decoding node_id")?

    let host = _KafkaStringCodec.decode(logger, rb)?

    let port = _KafkaI32Codec.decode(logger, rb, "Error decoding port")?


    let rack = _KafkaStringCodec.decode(logger, rb)?

    recover val _KafkaBroker(node_id, host, port, rack) end


primitive _KafkaMetadataV2 is _KafkaMetadataApi
  fun version(): I16 => 2

  fun min_broker_version(): String => "0.0.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val):
    Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)

    _KafkaMetadataV1.encode_request_body(wb, conf)

    let wb_msg = recover ref Writer end
    _KafkaI32Codec.encode(wb_msg, wb.size().i32())
    wb_msg.writev(wb.done())
    wb_msg.done()

  fun decode_response(logger: Logger[String], rb: Reader): _KafkaMetadata val ?
  =>
    let brokers = _KafkaMetadataV1.decode_brokers(logger, rb)?
    let cluster_id = _KafkaStringCodec.decode(logger, rb)?
    let controller_id = _KafkaI32Codec.decode(logger, rb,
      "Error decoding controller_id")?

    let topics_metadata = _KafkaMetadataV1.decode_topics_metadata(logger, rb)?

    _KafkaMetadata(brokers, topics_metadata, controller_id, cluster_id)


// TODO: a lot of these are not implemented yet; implement them
primitive _KafkaLeaderAndIsrV0 is _KafkaApi
  fun api_key(): I16 => 4

  fun version(): I16 => 0

  fun string(): String => "LeaderAndIsr (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaStopReplicaV0 is _KafkaApi
  fun api_key(): I16 => 5

  fun version(): I16 => 0

  fun string(): String => "StopReplica (" + api_key().string() + ") (Version: "
    + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaUpdateMetadataV0 is _KafkaApi
  fun api_key(): I16 => 6

  fun version(): I16 => 0

  fun string(): String => "UpdateMetadata (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaUpdateMetadataV1 is _KafkaApi
  fun api_key(): I16 => 6

  fun version(): I16 => 1

  fun string(): String => "UpdateMetadata (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaUpdateMetadataV2 is _KafkaApi
  fun api_key(): I16 => 6

  fun version(): I16 => 2

  fun string(): String => "UpdateMetadata (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaUpdateMetadataV3 is _KafkaApi
  fun api_key(): I16 => 6

  fun version(): I16 => 3

  fun string(): String => "UpdateMetadata (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaControlledShutdownV1 is _KafkaApi
  fun api_key(): I16 => 7

  fun version(): I16 => 1

  fun string(): String => "ControlledShutdown (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.0.0"

primitive _KafkaOffsetCommitV0 is _KafkaApi
  fun api_key(): I16 => 8

  fun version(): I16 => 0

  fun string(): String => "OffsetCommit (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.8.1"

primitive _KafkaOffsetCommitV1 is _KafkaApi
  fun api_key(): I16 => 8

  fun version(): I16 => 1

  fun string(): String => "OffsetCommit (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.8.2"

primitive _KafkaOffsetCommitV2 is _KafkaApi
  fun api_key(): I16 => 8

  fun version(): I16 => 2

  fun string(): String => "OffsetCommit (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.9.0"

primitive _KafkaOffsetFetchV0 is _KafkaApi
  fun api_key(): I16 => 9

  fun version(): I16 => 0

  fun string(): String => "OffsetFetch (" + api_key().string() + ") (Version: "
    + version().string() + ")"

  fun min_broker_version(): String => "0.8.2"

primitive _KafkaOffsetFetchV1 is _KafkaApi
  fun api_key(): I16 => 9

  fun version(): I16 => 1

  fun string(): String => "OffsetFetch (" + api_key().string() + ") (Version: "
    + version().string() + ")"

  fun min_broker_version(): String => "0.8.2"

primitive _KafkaOffsetFetchV2 is _KafkaApi
  fun api_key(): I16 => 9

  fun version(): I16 => 2

  fun string(): String => "OffsetFetch (" + api_key().string() + ") (Version: "
    + version().string() + ")"

  fun min_broker_version(): String => "0.8.2"

primitive _KafkaGroupCoordinatorV0 is _KafkaApi
  fun api_key(): I16 => 10

  fun version(): I16 => 0

  fun string(): String => "GroupCoordinator (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.8.2"

primitive _KafkaJoinGroupV0 is _KafkaApi
  fun api_key(): I16 => 11

  fun version(): I16 => 0

  fun string(): String => "JoinGroup (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun min_broker_version(): String => "0.9.0"

primitive _KafkaJoinGroupV1 is _KafkaApi
  fun api_key(): I16 => 11

  fun version(): I16 => 1

  fun string(): String => "JoinGroup (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun min_broker_version(): String => "0.10.1.0"

primitive _KafkaHeartbeatV0 is _KafkaApi
  fun api_key(): I16 => 12

  fun version(): I16 => 0

  fun string(): String => "Heartbeat (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun min_broker_version(): String => "0.9.0.0"

primitive _KafkaLeaveGroupV0 is _KafkaApi
  fun api_key(): I16 => 13

  fun version(): I16 => 0

  fun string(): String => "LeaveGroup (" + api_key().string() + ") (Version: " +
     version().string() + ")"

  fun min_broker_version(): String => "0.9.0.0"

primitive _KafkaSyncGroupV0 is _KafkaApi
  fun api_key(): I16 => 14

  fun version(): I16 => 0

  fun string(): String => "SyncGroup (" + api_key().string() + ") (Version: " +
    version().string() + ")"

  fun min_broker_version(): String => "0.9.0.0"

primitive _KafkaDescribeGroupsV0 is _KafkaApi
  fun api_key(): I16 => 15

  fun version(): I16 => 0

  fun string(): String => "DescribeGroups (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.9.0.0"

primitive _KafkaListGroupsV0 is _KafkaApi
  fun api_key(): I16 => 16

  fun version(): I16 => 0

  fun string(): String => "ListGroups (" + api_key().string() + ") (Version: " +
     version().string() + ")"

  fun min_broker_version(): String => "0.9.0.0"

primitive _KafkaSaslHandshakeV0 is _KafkaApi
  fun api_key(): I16 => 17

  fun version(): I16 => 0

  fun string(): String => "SaslHandshake (" + api_key().string() +
    ") (Version: " + version().string() + ")"

  fun min_broker_version(): String => "0.10.0.0"

trait val _KafkaApiVersionsApi is _KafkaApi
  fun api_key(): I16 => 18

  fun string(): String => "ApiVersions (" + api_key().string() + ") (Version: "
    + version().string() + ")"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val):
    Array[ByteSeq] iso^

  fun decode_response(logger: Logger[String], rb: Reader):
    Array[(I16, I16, I16)] ?

primitive _KafkaApiVersionsV0 is _KafkaApiVersionsApi
  fun version(): I16 => 0

  fun min_broker_version(): String => "0.10.0.0"

  fun encode_request(correlation_id: I32, conf: KafkaConfig val):
    Array[ByteSeq] iso^
  =>
    let wb = recover ref Writer end
    let size: I32 = 10 + conf.client_name.size().i32()
    _KafkaI32Codec.encode(wb, size)
    _KafkaRequestHeader.encode(wb, api_key(), version(), correlation_id, conf)
    wb.done()

  fun decode_response(logger: Logger[String], rb: Reader):
    Array[(I16, I16, I16)] ?
  =>
    let error_code = _KafkaI16Codec.decode(logger, rb,
      "Error decoding error code")?

    match error_code
    | ErrorNone() => None
    else
      logger(Error) and logger.log(Error, "Error from kafka server: " +
        error_code.string())
      error
    end

    var num_elems = _KafkaI32Codec.decode(logger, rb,
      "Error decoding broker api versions num elements")?

    let broker_supported_api_versions = Array[(I16, I16, I16)]

    while num_elems > 0 do
      broker_supported_api_versions.push(decode_api_version(logger, rb)?)
      num_elems = num_elems - 1
    end

    broker_supported_api_versions

  fun decode_api_version(logger: Logger[String], rb: Reader): (I16, I16, I16) ?
  =>
    let resp_api_key = _KafkaI16Codec.decode(logger, rb,
      "Error decoding api key")?

    let min_api_version = _KafkaI16Codec.decode(logger, rb,
      "Error decoding min api version")?

    let max_api_version = _KafkaI16Codec.decode(logger, rb,
      "Error decoding max api version")?

    (resp_api_key, min_api_version, max_api_version)

primitive _KafkaCreateTopicsV0 is _KafkaApi
  fun api_key(): I16 => 19

  fun version(): I16 => 0

  fun string(): String => "CreateTopics (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.10.0.0"

primitive _KafkaCreateTopicsV1 is _KafkaApi
  fun api_key(): I16 => 19

  fun version(): I16 => 1

  fun string(): String => "CreateTopics (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.10.0.0"

primitive _KafkaDeleteTopicsV0 is _KafkaApi
  fun api_key(): I16 => 20

  fun version(): I16 => 0

  fun string(): String => "DeleteTopics (" + api_key().string() + ") (Version: "
     + version().string() + ")"

  fun min_broker_version(): String => "0.10.0.0"
