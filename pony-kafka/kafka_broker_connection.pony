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

use "customnet"
use "customlogger"
use "net"
use "collections"

// define what a kafka broker connection is
// this is a trait so any actor type could in theory be a kafka broker
// connection
trait KafkaBrokerConnection is CustomTCPConnection
  // behavior to update the internal brokers list
  be _update_brokers_list(brokers_list: Map[I32, (_KafkaBroker val,
    KafkaBrokerConnection tag)] val)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).update_brokers_list(brokers_list)
    end

  // behavior to send messages to kafka brokers
  be send_kafka_messages(topic: String, msgs_to_send: Map[I32,
    Array[ProducerKafkaMessage val] iso] val, auth: _KafkaProducerAuth)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).send_kafka_messages(this, topic, msgs_to_send, auth)?
    end

  be send_kafka_message(topic: String, partition_id: I32,
    msg_to_send: ProducerKafkaMessage val, auth: _KafkaProducerAuth)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).send_kafka_message(this, topic, partition_id,
        msg_to_send, auth)?
    end

  be message_consumed(msg: KafkaMessage val, success: Bool) =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).message_consumed(msg, success)
    end

  be _update_consumer_message_handler(topic: String,
    consumer_handler: KafkaConsumerMessageHandler val)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._update_consumer_message_handler(topic, consumer_handler)
    end

  be _update_consumers(topic_consumers: Map[String, Array[KafkaConsumer tag]
    val] val)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._update_consumers(topic_consumers)
    end

  be _consumer_pause(topic: String, partition_id: I32) =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._consumer_pause(this, topic, partition_id)
    end

  be _consumer_pause_all() =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._consumer_pause_all(this)
    end

  be _consumer_resume(topic: String, partition_id: I32) =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._consumer_resume(this, topic, partition_id)
    end

  be _consumer_resume_all() =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._consumer_resume_all(this)
    end

  be _consume_messages() =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).consume_messages(this)?
    end

  be _refresh_metadata() =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler).refresh_metadata(this)?
    end

  be _send_pending_messages() =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._send_pending_messages(this)?
    end

  be _leader_change_throttle_ack(topics_to_throttle: Map[String, Set[I32] iso]
    val)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._leader_change_throttle_ack(topics_to_throttle, this)
    end

  // update metadata based on what other broker connections got from kafka
  be _update_metadata(meta: _KafkaMetadata val) =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._update_metadata(meta, this)
    end

  be _leader_change_msgs(meta: _KafkaMetadata val, topic: String,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val)
  =>
    try
      ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)._leader_change_msgs(meta, topic, msgs, this)
    end

  be write(data: ByteSeq) =>
    """
    Write a single sequence of bytes.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error,
        "Cannot write directly on a Kafka Broker Connection. Ignoring request.")
    end

  be queue(data: ByteSeq) =>
    """
    Queue a single sequence of bytes on linux.
    Do nothing on windows.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error,
        "Cannot queue directly on a Kafka Broker Connection. Ignoring request.")
    end

  be writev(data: ByteSeqIter) =>
    """
    Write a sequence of sequences of bytes.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error, "Cannot writev directly on a Kafka " +
        "Broker Connection. Ignoring request.")
    end

  be queuev(data: ByteSeqIter) =>
    """
    Queue a sequence of sequences of bytes on linux.
    Do nothing on windows.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error, "Cannot queuev directly on a Kafka " +
        "Broker Connection. Ignoring request.")
    end

  be send_queue() =>
    """
    Write pending queue to network on linux.
    Do nothing on windows.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error, "Cannot send_queue directly on a " +
        "Kafka Broker Connection. Ignoring request.")
    end

  be set_notify(notify: CustomTCPConnectionNotify iso) =>
    """
    Change the notifier.
    """
    try
      let handler = ((get_handler() as CustomTCPConnectionHandler).notify as
        _KafkaHandler)
      let logger = handler.get_conf().logger
      logger(Error) and logger.log(Error, "Cannot set notify directly on a " +
        "Kafka Broker Connection. Ignoring request.")
    end


// factory for creating kafka broker connections on demand
// used in combination with the main broker connection trait to make kafka
// connection types arbitrary
trait KafkaBrokerConnectionFactory
  fun apply(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag

  fun ip4(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag

  fun ip6(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag


// simple kafka broker connection factory implementation to create simple kafka
// broker connections
class SimpleKafkaBrokerConnectionFactory is KafkaBrokerConnectionFactory
  new val create() =>
    None

  fun apply(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag
  =>
    SimpleKafkaBrokerConnection(auth, consume notify, host,
      service, from, init_size, max_size)

  fun ip4(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag
  =>
    SimpleKafkaBrokerConnection.ip4(auth, consume notify, host,
      service, from, init_size, max_size)

  fun ip6(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384): KafkaBrokerConnection tag
  =>
    SimpleKafkaBrokerConnection.ip6(auth, consume notify, host,
      service, from, init_size, max_size)


// simple kafka broker connection
// all logic for the broker connection comes from the trait
// the actur just need to implement `get_handler` and have an appropriate
// internal handler variable
actor SimpleKafkaBrokerConnection is KafkaBrokerConnection
  var _handler: TCPConnectionHandler = MockTCPConnectionHandler

  fun ref get_handler(): TCPConnectionHandler =>
    _handler

  new create(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv4 or IPv6. If `from` is a non-empty string, the connection
    will be made from the specified interface.
    """
    _handler = CustomTCPConnectionHandler(this, auth, consume notify, host,
      service, from, init_size, max_size)

  new ip4(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv4.
    """
    _handler = CustomTCPConnectionHandler.ip4(this, auth, consume notify, host,
      service, from, init_size, max_size)

  new ip6(auth: TCPConnectionAuth, notify: CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv6.
    """
    _handler = CustomTCPConnectionHandler.ip6(this, auth, consume notify, host,
      service, from, init_size, max_size)

  new _accept(listen: TCPListener, notify: CustomTCPConnectionNotify iso, fd:
    U32,
    init_size: USize = 64, max_size: USize = 16384)
  =>
    """
    A new connection accepted on a server.
    """
    _handler = CustomTCPConnectionHandler.accept(this, listen, consume notify,
      fd, init_size, max_size)


actor _MockKafkaBrokerConnection is KafkaBrokerConnection
  var _handler: TCPConnectionHandler = MockTCPConnectionHandler

  fun ref get_handler(): TCPConnectionHandler =>
    _handler
