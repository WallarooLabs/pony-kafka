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
use "time"
use "customnet"
use "customlogger"
use "fsm"

// states for kafka handler internal state machine for initialization
primitive _KafkaPhaseStart is FsmState
  fun string(): String => "_KafkaPhaseStart"

primitive _KafkaPhaseUpdateApiVersions is FsmState
  fun string(): String => "_KafkaPhaseUpdateApiVersions"

primitive _KafkaPhaseSkipUpdateApiVersions is FsmState
  fun string(): String => "_KafkaPhaseSkipUpdateApiVersions"

primitive _KafkaPhaseUpdateMetadata is FsmState
  fun string(): String => "_KafkaPhaseUpdateMetadata"

primitive _KafkaPhaseUpdateOffsets is FsmState
  fun string(): String => "_KafkaPhaseUpdateOffsets"

primitive _KafkaPhaseDone is FsmState
  fun string(): String => "_KafkaPhaseDone"

primitive _KafkaPhaseUpdateApiVersionsReconnect is FsmState
  fun string(): String => "_KafkaPhaseUpdateApiVersionsReconnect"

primitive _KafkaPhaseUpdateMetadataReconnect is FsmState
  fun string(): String => "_KafkaPhaseUpdateMetadataReconnect"

primitive _KafkaPhaseSkipUpdateApiVersionsReconnect is FsmState
  fun string(): String => "_KafkaPhaseSkipUpdateApiVersionsReconnect"


// kafka handler internal state
class _KafkaState
  var controller_id: (I32 | None) = None
  var cluster_id: String = ""
  let topics_state: Map[String, _KafkaTopicState] = topics_state.create()
  let consumer_unacked_offsets: Map[KafkaTopicPartition, Set[I64] iso] =
    consumer_unacked_offsets.create()

  new create()
  =>
    None

  fun string(): String =>
    var topm_str = recover ref String end
    for t in topics_state.values() do
      topm_str.>append(", ").>append(t.string())
    end
    "KafkaState: [ "
      + "topics_state = " + topm_str
      + ", controller_id = " + controller_id.string()
      + ", cluster_id = " + cluster_id.string()
      + " ]\n"

// topic internal state
class _KafkaTopicState
  var topic_error_code: I16
  let topic: String
  var is_internal: (Bool | None)
  let partitions_state: Map[I32, _KafkaTopicPartitionState] =
    partitions_state.create()
  var consumers: Array[KafkaConsumer tag] val = recover val consumers.create()
    end
  var message_handler: KafkaConsumerMessageHandler

  new create(topic': String, message_handler': KafkaConsumerMessageHandler,
    topic_error_code': I16 = 0, is_internal': (Bool | None) = None)
  =>
    topic_error_code = topic_error_code'
    topic = topic'
    is_internal = is_internal'
    message_handler = message_handler'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partitions_state.values() do
      parts_str.>append(", ").>append(p.string())
    end
    "KafkaTopicState: [ "
      + "topic_error_code = " + topic_error_code.string()
      + ", topic = " + topic.string()
      + ", is_internal = " + is_internal.string()
      + ", partitions_state = " + parts_str
      + " ]\n"

class _KafkaTopicPartitionState
  var partition_error_code: I16
  let partition_id: I32
  var leader: I32
  var replicas: Array[I32] val
  var isrs: Array[I32] val
  var request_timestamp: I64
  var error_code: I16 = 0
  var timestamp: (I64 | None) = None
  var request_offset: I64 = -1
  var max_bytes: I32
  var paused: Bool = true
  var current_leader: Bool = false
  // Messages already sent to broker that we get a failure response for
  var leader_change_sent_messages: Array[ProducerKafkaMessage val] =
    leader_change_sent_messages.create()
  // Message that haven't been sent to a broker yet
  var leader_change_pending_messages: Array[ProducerKafkaMessage val] =
    leader_change_pending_messages.create()
  var leader_change: Bool = false
  var leader_change_receiver: Bool = false

  new create(partition_error_code': I16, partition_id': I32, leader': I32,
    replicas': Array[I32] val, isrs': Array[I32] val, request_timestamp': I64 =
    -2, max_bytes': I32 = 32768)
  =>
    partition_error_code = partition_error_code'
    partition_id = partition_id'
    leader = leader'
    replicas = replicas'
    isrs = isrs'
    request_timestamp = request_timestamp'
    max_bytes = max_bytes'

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
      + ", current_leader = " + current_leader.string()
      + ", replicas = [ " + replicas_str
      + " ], isrs = [ " + isrs_str
      + " ], request_timestamp = " + request_timestamp.string()
      + " ]\n"

// actual kafka handler logic
class _KafkaHandler is CustomTCPConnectionNotify
  var _correlation_id: I32 = 1
  var _header: Bool = true
  let _header_length: USize = 4
  let _pending_buffer: Array[(I32, U64, Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]])] = _pending_buffer.create(64)
  let _requests_buffer: Array[(I32, _KafkaApi, (None | (I32, U64, Map[String,
    Map[I32, Array[ProducerKafkaMessage val]]])))] = _requests_buffer.create(64)
  var _broker_apis_to_use: Map[I16, _KafkaApi] = _broker_apis_to_use.create()
  var _api_versions_supported: Bool = true

  let _state: _KafkaState = _KafkaState

  var _brokers: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val =
    recover val _brokers.create() end
  let _connection_broker_id: I32

  let _conf: KafkaConfig val
  let _metadata_only: Bool
  let _kafka_client: KafkaClient

  let _timers: Timers = Timers
  let _reconnect_closed_delay: U64
  let _reconnect_failed_delay: U64
  var _num_reconnects_total: U8
  var _num_reconnects_left: I16

  let _statemachine: Fsm[KafkaBrokerConnection ref]

  var send_batch_timer: (Timer tag | None) = None
  var metadata_refresh_timer: (Timer tag | None) = None
  var metadata_refresh_request_outstanding: Bool = false
  var fetch_data_timer: (Timer tag | None) = None

  var _all_topics_partitions_paused: Bool = true
  var _currently_throttled: Bool = false

  let _name: String
  let _rb: IsoReader ref = recover ref IsoReader end

  new create(client: KafkaClient, conf: KafkaConfig val,
    topic_consumer_handlers: Map[String, KafkaConsumerMessageHandler val] val,
    connection_broker_id: I32 = -1, reconnect_closed_delay: U64 = 100_000_000,
    reconnect_failed_delay: U64 = 1_000_000_000, num_reconnects: U8 = 250)
  =>
    _conf = conf
    _kafka_client = client
    _connection_broker_id = connection_broker_id
    _metadata_only = (connection_broker_id < 0)
    _reconnect_closed_delay = reconnect_closed_delay
    _reconnect_failed_delay = reconnect_failed_delay
    _num_reconnects_total = num_reconnects
    _num_reconnects_left = num_reconnects.i16()

    _name = _conf.client_name + "#" + _connection_broker_id.string() + ": "

    // initialize pending buffer
    _pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage
      val]]]))

    for (topic, consumer_handler) in topic_consumer_handlers.pairs() do
      _state.topics_state(topic) = _KafkaTopicState(topic,
        consumer_handler.clone())
    end

    // default metadata request version to use
    _broker_apis_to_use(_KafkaMetadataV0.api_key()) = _KafkaMetadataV0

    _statemachine = Fsm[KafkaBrokerConnection ref](_conf.logger)

    // simple intialization phase state machine
    try
      _statemachine.add_allowed_state(_KafkaPhaseStart)?
      _statemachine.add_allowed_state(_KafkaPhaseSkipUpdateApiVersions)?
      _statemachine.add_allowed_state(_KafkaPhaseUpdateApiVersions)?
      _statemachine.add_allowed_state(_KafkaPhaseUpdateMetadata)?
      _statemachine.add_allowed_state(_KafkaPhaseUpdateOffsets)?
      _statemachine.add_allowed_state(_KafkaPhaseDone)?
      _statemachine.add_allowed_state(_KafkaPhaseUpdateApiVersionsReconnect)?
      _statemachine.add_allowed_state(_KafkaPhaseSkipUpdateApiVersionsReconnect)?
      _statemachine.add_allowed_state(_KafkaPhaseUpdateMetadataReconnect)?

      _statemachine.valid_transition(FsmStateAny, _KafkaPhaseStart,
        {ref(old_state: FsmState val, state_machine: Fsm[KafkaBrokerConnection
        ref], conn: KafkaBrokerConnection ref)(kh = this) => None} ref)?

      _statemachine.valid_transition(FsmStateAny, _KafkaPhaseUpdateApiVersions,
        {ref(old_state: FsmState val, state_machine: Fsm[KafkaBrokerConnection
        ref], conn: KafkaBrokerConnection ref)(kh = this) =>
        kh._write_to_network(conn, kh.update_broker_api_versions())} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersions,
        _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine:
        Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection ref)(kh = this)
        => kh.refresh_metadata(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersions,
        _KafkaPhaseSkipUpdateApiVersions, {ref(old_state: FsmState val,
        state_machine: Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection
        ref)(kh = this) => kh.set_fallback_api_versions(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseSkipUpdateApiVersions,
        _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine:
        Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection ref)(kh = this)
        => kh.refresh_metadata(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseStart,
        _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine:
        Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection ref)(kh = this)
        => kh.refresh_metadata(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateMetadata,
        _KafkaPhaseUpdateOffsets, {ref(old_state: FsmState val, state_machine:
        Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection ref)(kh = this)
        => kh.refresh_offsets(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateOffsets, _KafkaPhaseDone,
        {ref(old_state: FsmState val, state_machine: Fsm[KafkaBrokerConnection
        ref], conn: KafkaBrokerConnection ref)(kh = this) =>
        kh.done_and_consume(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseDone,
        _KafkaPhaseUpdateApiVersionsReconnect, {ref(old_state: FsmState val,
        state_machine: Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection
        ref)(kh = this) =>
        kh._write_to_network(conn, kh.update_broker_api_versions())} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersionsReconnect,
        _KafkaPhaseUpdateMetadataReconnect, {ref(old_state: FsmState val,
        state_machine: Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection
        ref)(kh = this) => kh.refresh_metadata(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersionsReconnect,
        _KafkaPhaseSkipUpdateApiVersionsReconnect, {ref(old_state: FsmState val,
         state_machine: Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection
        ref)(kh = this) => kh.set_fallback_api_versions(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseSkipUpdateApiVersionsReconnect,
        _KafkaPhaseUpdateMetadataReconnect, {ref(old_state: FsmState val,
        state_machine: Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection
        ref)(kh = this) => kh.refresh_metadata(conn)} ref)?

      _statemachine.valid_transition(_KafkaPhaseUpdateMetadataReconnect,
        _KafkaPhaseDone, {ref(old_state: FsmState val, state_machine:
        Fsm[KafkaBrokerConnection ref], conn: KafkaBrokerConnection ref)(kh = this)
        => kh.done_and_consume_reconnect(conn)} ref)?

      _statemachine.initialize(_KafkaPhaseStart, {(old_state: FsmState val,
        new_state: FsmState val, state_machine: Fsm[KafkaBrokerConnection ref],
        data: KafkaBrokerConnection ref)(kh = this, logger = _conf.logger, name =
        _name) => logger(Error) and logger.log(Error, name +
        "Error transitioning states from " + old_state.string() + " to " +
        new_state.string())} ref)?

    else
      _conf.logger(Error) and _conf.logger.log(Error, _name +
        "Error initializing state machine!")
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorInitializingFSM,
        "N/A", -1))
    end

  fun ref _update_consumers(topic_consumers: Map[String, Array[KafkaConsumer
    tag] val] val) =>
    for (topic, consumers) in topic_consumers.pairs() do
      try
        _state.topics_state(topic)?.consumers = consumers
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error updating consumers for topic: " + topic +
          ". This should never happen."),
          topic, -1))
        return
      end
    end

  fun _get_conf(): KafkaConfig val => _conf
  fun _get_client(): KafkaClient => _kafka_client
  fun _get_name(): String => _name

  fun ref done_and_consume(conn: KafkaBrokerConnection ref) =>
    _kafka_client._unthrottle_producers(_connection_broker_id)
    _kafka_client._broker_initialized(_connection_broker_id)
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Done with initialization and metadata/offsets update")

    metadata_refresh_request_outstanding = false
    match metadata_refresh_timer
    | None =>
      // start metadata refresh timer
      let timer = Timer(_KafkaRefreshMetadataTimerNotify(conn),
        _conf.refresh_metadata_interval)
      metadata_refresh_timer = timer
      _timers(consume timer)
    end

    if not _all_topics_partitions_paused then
      consume_messages(conn)
    end

  fun ref done_and_consume_reconnect(conn: KafkaBrokerConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Done with reconnect and metadata update")

    metadata_refresh_request_outstanding = false
    match metadata_refresh_timer
    | None =>
      // start metadata refresh timer
      let timer = Timer(_KafkaRefreshMetadataTimerNotify(conn),
        _conf.refresh_metadata_interval)
      metadata_refresh_timer = timer
      _timers(consume timer)
    end

    if not _all_topics_partitions_paused then
      consume_messages(conn)
    end

  fun ref _update_consumer_message_handler(topic: String, consumer_handler:
    KafkaConsumerMessageHandler val)
  =>
    try
      _state.topics_state(topic)?.message_handler = consumer_handler.clone()
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error updating message handler for topic: " + topic +
        ". This should never happen."),
        topic, -1))
    end

  fun ref _consumer_pause(conn: KafkaBrokerConnection ref, topic: String,
    partition_id: I32)
  =>
    // set to paused
    // if everything is paused, next attempt to fetch_data
    // will set _all_topics_partitions_paused = true
    try
      _state.topics_state(topic)?.partitions_state(partition_id)?.paused = true
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error pausing consumer for topic: " + topic + " and partition: " +
        partition_id.string() + "."),
        topic, -1))
    end

  fun ref _consumer_pause_all(conn: KafkaBrokerConnection ref) =>
    // set all to paused
    for (topic, ts) in _state.topics_state.pairs() do
      for (partition_id, ps) in ts.partitions_state.pairs() do
        ps.paused = true
      end
    end

  fun ref _consumer_resume(conn: KafkaBrokerConnection ref, topic: String,
    partition_id: I32) =>
    // set to unpaused
    try
      _state.topics_state(topic)?.partitions_state(partition_id)?.paused = false

      let was_totally_paused = _all_topics_partitions_paused = false

      // if we're fully initialized and we used to be completely paused
      if (_statemachine.current_state() is _KafkaPhaseDone) and
        (was_totally_paused == true) then
        consume_messages(conn)
      end
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error resuming consumer for topic: " + topic + " and partition: " +
        partition_id.string() + "."),
        topic, partition_id))
    end

  fun ref _consumer_resume_all(conn: KafkaBrokerConnection ref) =>
    let was_totally_paused = _all_topics_partitions_paused = false

    // set all to unpaused
    for (topic, ts) in _state.topics_state.pairs() do
      for (partition_id, ps) in ts.partitions_state.pairs() do
        ps.paused = false
      end
    end

    // if we're fully initialized and we used to be completely paused
    if (_statemachine.current_state() is _KafkaPhaseDone) and
      (was_totally_paused == true) then
      consume_messages(conn)
    end

/*
  // TODO: LIKELY BITROTTED
  fun ref message_consumed(msg: KafkaMessage val, success: Bool) =>
    // What to do if `success` is `false`? Can we do anything meaningful aside
    //  from ensuring that largest offset not successfully consumed is not
    //  committed?
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "received message consumed. Success: " + success.string() + ", msg: " +
      msg.string())
    if success then
      // TODO: Does it matter if we get an ack for something that we're not
      //  tracking any longer or at all?
      try
        _state.consumer_unacked_offsets(msg._get_topic_partition())?.unset(msg.
          get_offset())
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "error removing message from track unacked offset collection. msg: " +
          msg.string()),
          "N/A", -1))
      end
    end
*/

  fun ref received(conn: CustomTCPConnection ref, data: Array[U8] iso,
    times: USize): Bool
  =>
    let network_received_timestamp: U64 = Time.nanos()
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Kafka client received " + data.size().string() + " bytes")

    ifdef "enable-kafka-network-sniffing" then
      match _conf.network_sniffer
      | let ns: KafkaNetworkSniffer tag =>
        let copy = recover iso
          let c: Array[U8] ref = Array[U8]
          let s = data.size()
          var i: USize = 0
          while i < s do
            try
              c.push(data(i)?)
            end
            i = i + 1
          end
          c
        end

        ns.data_received(_connection_broker_id, consume copy)
      end
    end

    if _header then
      try
        let payload_size: USize = payload_length(consume data)?

        conn.get_handler().expect(payload_size)
        _header = false
      else
        // error decoding payload size.. ignore and wait for more data
        // error already logged in payload_length function
        conn.get_handler().expect(_header_length)
        _header = true
      end
      true
    else
      let kconn = try conn as KafkaBrokerConnection
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name + "Error"
          + " casting conn to KafkaBrokerConnection. This"
          + " should never happen."),
          "N/A", -1))
        return true
      end

      decode_response(kconn, consume data, network_received_timestamp)

      conn.get_handler().expect(_header_length)
      _header = true

      ifdef linux then
        true
      else
        false
      end
    end

  fun ref accepted(conn: CustomTCPConnection ref) =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Kafka client accepted a connection")
    conn.get_handler().expect(_header_length)

  fun ref before_reconnecting(conn: CustomTCPConnection ref) =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Kafka client attempting to reconnect...")

  fun ref connecting(conn: CustomTCPConnection ref, count: U32) =>
    (let host, let service) = conn.get_handler().requested_address()
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Kafka client connecting to " + host + ":" + service)

  fun ref connected(conn: CustomTCPConnection ref) =>
    let remote_address = conn.get_handler().remote_address()
    (let host, let service) = try remote_address.name()? else ("UNKNOWN", "UNKNOWN") end

    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Kafka client successfully connected to " + host + ":" + service)
    _num_reconnects_left = _num_reconnects_total.i16()
    try
      initialize_connection(conn as KafkaBrokerConnection)?
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name +
        "Error initializing kafka connection")
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorInitilizingConnection,
        "N/A", -1))
    end

  fun ref expect(conn: CustomTCPConnection ref, qty: USize): USize =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Kafka client expecting " + qty.string() + " bytes")
    qty

  fun ref connect_failed(conn: CustomTCPConnection ref) =>
    (let host, let service) = conn.get_handler().requested_address()
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Kafka client failed connection to " + host + ":" + service)

    // tell all other brokers to update metadata since we might not be able to reconnect
    // back to this broker to update metadata
    for (_, (_, broker_tag)) in _brokers.pairs() do
      if broker_tag isnt conn then
        broker_tag._refresh_metadata()
      end
    end

    if _num_reconnects_left > 0 then
      // TODO: implement backoff retries
      _conf.logger(Warn) and _conf.logger.log(Warn, _name +
        "Will attempt to reconnect in " + _reconnect_failed_delay.string() + ". " + _num_reconnects_left.string() + " attempts remaining.")
      let t = Timer(_ReconnectTimerNotify(conn), _reconnect_failed_delay, 0)
      _timers(consume t)
      _num_reconnects_left = _num_reconnects_left - 1
    else
      _conf.logger(Warn) and _conf.logger.log(Warn, _name +
        "No reconnect attempts left. Will not attempt to reconnect.")
      try
        _kafka_client._recoverable_error(KafkaErrorReport(ClientErrorNoBrokerConnection(conn as KafkaBrokerConnection),
          "N/A", -1), true)
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name + "Error"
          + " casting conn as KafkaBrokerConnection. This should never happen."),
          "N/A", -1))
      end
    end

  fun ref auth_failed(conn: CustomTCPConnection ref) =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Kafka client failed authentication")

  fun ref closed(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Kafka client closed connection")

    // clean up pending requests since they're invalid now and need to be
    // retried (for produce requests only)
    while _requests_buffer.size() > 0 do
      // TODO: double check logic on order this needs to be done in
      (let sent_correlation_id, let request_type, let extra_request_data) =
        try
          _requests_buffer.pop()?
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "Error popping item from requests_buffer. This should never happen."),
            "N/A", -1))
          return
        end

      match request_type
      // if it was a produce request, put it back on the pending buffer
      | let produce_api: _KafkaProduceApi
        =>
          match consume extra_request_data
          | (let s: I32, let n: U64, let m: Map[String, Map[I32,
            Array[ProducerKafkaMessage val]]]) =>
            _pending_buffer.unshift((s, n, consume m))
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name + "Error"
              + " extra_request_data for produce api in requests_buffer didn't match. This"
              + " should never happen."),
              "N/A", -1))
          end
      end
    end

    // terminate timers
    match fetch_data_timer
    | let t: Timer tag =>
      _timers.cancel(t)
      fetch_data_timer = None
    end

    metadata_refresh_request_outstanding = false
    match metadata_refresh_timer
    | let t: Timer tag =>
      _timers.cancel(t)
      metadata_refresh_timer = None
    end

    _kafka_client._throttle_producers(_connection_broker_id)

    // tell all other brokers to update metadata since we might not be able to reconnect
    // back to this broker to update metadata
    for (_, (_, broker_tag)) in _brokers.pairs() do
      if broker_tag isnt conn then
        broker_tag._refresh_metadata()
      end
    end

    // set to -1 if it's an intentional don't reconnect scenario
    if _num_reconnects_left == -1 then
      return
    end

    if _num_reconnects_left > 0 then
      // TODO: implement backoff retries
      _conf.logger(Warn) and _conf.logger.log(Warn, _name +
        "Will attempt to reconnect in " + _reconnect_closed_delay.string() + ". " + _num_reconnects_left.string() + " attempts remaining.")
      let t = Timer(_ReconnectTimerNotify(conn), _reconnect_closed_delay, 0)
      _timers(consume t)
      _num_reconnects_left = _num_reconnects_left - 1
    else
      _conf.logger(Warn) and _conf.logger.log(Warn, _name +
        "No reconnect attempts left. Will not attempt to reconnect.")
      try
        _kafka_client._recoverable_error(KafkaErrorReport(ClientErrorNoBrokerConnection(conn as KafkaBrokerConnection),
          "N/A", -1), true)
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name + "Error"
          + " casting conn as KafkaBrokerConnection. This should never happen."),
          "N/A", -1))
      end
    end

  fun ref throttled(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Kafka client is throttled")
    _currently_throttled = true
    _kafka_client._throttle_producers(_connection_broker_id)

  fun ref unthrottled(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Kafka client is unthrottled")
    _currently_throttled = false
    _kafka_client._unthrottle_producers(_connection_broker_id)

  fun dispose() =>
    _conf.logger(Info) and _conf.logger.log(Info, _name +
      "Disposing any active reconnect timers...")
    _timers.dispose()

  // correlation id is assigned to each request sent to the broker. Brokers
  // always send back correlation id with each response and process all requests
  // sequentially.
  fun ref _next_correlation_id(): I32 =>
    _correlation_id = _correlation_id + 1

  fun ref payload_length(data: Array[U8] iso): USize ? =>
    _rb.append(consume data)
    try
      let n = BigEndianDecoder.i32(_rb)?.usize()
      _rb.clear()
      n
    else
      // throw error so that we can ignore it in `received` and wait for more data
      _conf.logger(Warn) and _conf.logger.log(Warn, _name +
        "Error decoding payload_length... Ignoring.")
      error
    end

  fun ref initialize_connection(conn: KafkaBrokerConnection ref) ? =>
    match (_statemachine.current_state(), _api_versions_supported)
    | (_KafkaPhaseUpdateApiVersions, _) =>
      // if we're reconnecting when we had requested ApiVersions, we need to
      // fall back to a fixed set of functionality because broker doesn't
      // support ApiVersions call
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersions, conn)?
    | (_KafkaPhaseUpdateApiVersionsReconnect, _) =>
      // if we're reconnecting when we had requested ApiVersions, we need to
      // fall back to a fixed set of functionality because broker doesn't
      // support ApiVersions call
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersionsReconnect,
        conn)?
    | (_KafkaPhaseStart, true) =>
      _statemachine.transition_to(_KafkaPhaseUpdateApiVersions, conn)?
    | (_KafkaPhaseStart, false) =>
      // if ApiVersions is not supported, we need to fall back to a fixed set of
      // functionality
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersions, conn)?
    | (_KafkaPhaseDone, _) =>
      // if we're already fully initialized
      // retry the ApiVersions call in case broker was upgraded
      _api_versions_supported = true
      _statemachine.transition_to(_KafkaPhaseUpdateApiVersionsReconnect, conn)?
    else
      // we're not fully initialized but we're past ApiVersions
      if not (_statemachine.current_state() is _KafkaPhaseUpdateApiVersions)
        and _api_versions_supported then
        _statemachine.transition_to(_KafkaPhaseUpdateApiVersions, conn)?
      else
        _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)?
      end
    end
    conn.get_handler().expect(_header_length)

  fun ref set_fallback_api_versions(conn: KafkaBrokerConnection ref) =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Got disconnected requesting broker api versions")
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Falling back to defaults (version 0.8)")
    _api_versions_supported = false
    // TODO: encapsulate this and make fallback version configurable
    _broker_apis_to_use.clear()
    _broker_apis_to_use(_KafkaProduceV0.api_key()) = _KafkaProduceV0
    _broker_apis_to_use(_KafkaFetchV0.api_key()) = _KafkaFetchV0
    _broker_apis_to_use(_KafkaOffsetsV0.api_key()) = _KafkaOffsetsV0
    _broker_apis_to_use(_KafkaMetadataV0.api_key()) = _KafkaMetadataV0
    _broker_apis_to_use(_KafkaOffsetCommitV0.api_key()) = _KafkaOffsetCommitV0
    _broker_apis_to_use(_KafkaOffsetFetchV0.api_key()) = _KafkaOffsetFetchV0

    // Update metadata
    match _statemachine.current_state()
    | _KafkaPhaseSkipUpdateApiVersions =>
      try
        _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)?
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
          "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseUpdateMetadata.string()),
          "N/A", -1))
        return
      end
    | _KafkaPhaseSkipUpdateApiVersionsReconnect =>
      try
        _statemachine.transition_to(_KafkaPhaseUpdateMetadataReconnect, conn)?
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
          "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseUpdateMetadataReconnect.string()),
          "N/A", -1))
        return
      end
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error we're in state: " + _statemachine.current_state().string() +
        " in set_fallback_api_versions. This should never happen."),
        "N/A", -1))
    end


  // Rules for kafka requests
  // * Only one fetch request at a time (based on a timer; fetch requests
  // timeout wait is always done in client side and requests to broker always
  // request data immediately in order to allow other requests [produce,
  // metadata, etc] to be done in the meantime due to kafka's pipelined
  // processing of requests)
  // * Only one metadata update request at a time (based on a timer to learn of
  // changes to topics we care about (new partitions) and brokers being added)
  // * Only one update offsets request at a time; does this even need to occur
  // outside of reconnect/initial connect?
  // * Multiple produce requests at a time
  // * Produce requests go through a queue which is limited by max outstanding
  // requests
  // * Produce requests are the only ones that need to be broken up into
  // multiple requests due to size
  // * fetch/metadata update/update offsets can skip ahead of produces requests
  // in the queue
  // * TODO: figure out how group consumer related stuff fits in


  // send request to produce messages to kafka broker.. this comes from
  // KafkaProducers by use of the KafkaProducerMapping
  // auth: KafkaProducerAuth is a guard to ensure people don't try and send
  // messages without using the KafkaProducerMapping
  fun ref send_kafka_message(conn: KafkaBrokerConnection ref, topic: String,
    partition_id: I32, msg: ProducerKafkaMessage val, auth: _KafkaProducerAuth)
  =>
    let produce_api = try _broker_apis_to_use(_KafkaProduceV0.api_key())? as
        _KafkaProduceApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting produce api to use. This should never happen."),
          "N/A", -1))
        return
      end

    try
      produce_api.combine_and_split_by_message_size_single(_conf, _pending_buffer,
        topic, partition_id, msg, _state.topics_state)?
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error in combine_and_split_by_message_size_single. This should never happen."),
        "N/A", -1))
      return
    end

    _maybe_process_pending_buffer(conn)

  fun ref send_kafka_messages(conn: KafkaBrokerConnection ref, topic: String,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val, auth:
    _KafkaProducerAuth)
  =>
    let produce_api = try _broker_apis_to_use(_KafkaProduceV0.api_key())? as
        _KafkaProduceApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting produce api to use. This should never happen."),
          "N/A", -1))
        return
      end

    try
      produce_api.combine_and_split_by_message_size(_conf, _pending_buffer, topic,
         msgs, _state.topics_state)?
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error in combine_and_split_by_message_size. This should never happen."),
        "N/A", -1))
      return
    end

    _maybe_process_pending_buffer(conn)

  fun ref _maybe_process_pending_buffer(conn: KafkaBrokerConnection ref) =>
    (_, let num_msgs, _) = try _pending_buffer(0)? else return end
    if num_msgs == 0 then
      return
    end

    // check if batch send timer is active
    match send_batch_timer
    | let t: Timer tag =>
      // check if we've accumulated max_message_size of data to send (we'll have
      // more than 1 pending_buffer entry if yes)
      if _pending_buffer.size() > 1 then
        // send everything but last buffer entry since it almost definitely has
        // room for more data to accumulate
        process_pending_buffer(conn, true)

        // cancel and recreate timer for remaining messages to be sent
        _timers.cancel(t)
        let timer = Timer(_KafkaSendPendingMessagesTimerNotify(conn),
          _conf.max_produce_buffer_time)
        send_batch_timer = timer
        _timers(consume timer)
      else
        // check if we've reached max_produce_buffer_messages or not
        if num_msgs >= _conf.max_produce_buffer_messages then
          // send all messages
          process_pending_buffer(conn)

          // cancel timer because we're sending all messages now
          _timers.cancel(t)
          send_batch_timer = None
        end
      end
    else
      // if no batch send timer then check if we're supposed to be batching or
      // not
      if _conf.max_produce_buffer_time > 0 then
        // if yes, create timer to send later
        let timer = Timer(_KafkaSendPendingMessagesTimerNotify(conn),
          _conf.max_produce_buffer_time)
        send_batch_timer = timer
        _timers(consume timer)
      else
        // if no, send messages
        process_pending_buffer(conn)
      end
    end

  fun ref _send_pending_messages(conn: KafkaBrokerConnection ref) =>
    send_batch_timer = None
    process_pending_buffer(conn)

  fun ref process_pending_buffer(conn: KafkaBrokerConnection ref,
    send_all_but_one: Bool = false)
  =>
    (_, let n, _) = try _pending_buffer(0)? else return end
    if n == 0 then
      return
    end

    let limit: USize = if send_all_but_one then 1 else 0 end
    while (_requests_buffer.size() < _conf.max_inflight_requests) and
      (_pending_buffer.size() > limit) do
      // TODO: Figure out a way to avoid shift... maybe use a ring buffer?
      (let size, let num_msgs, let msgs) = try _pending_buffer.shift()?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Error shifting _pending_buffer. This should never happen."),
              "N/A", -1))
            return
          end

      let produce_request = produce_messages(size, num_msgs, consume msgs)
      match consume produce_request
      | let pr: Array[ByteSeq] val =>
        _write_to_network(conn, consume pr)
      end
    end
    if _pending_buffer.size() == 0 then
      // initialize pending buffer
      _pending_buffer.push((0, 0, Map[String, Map[I32,
        Array[ProducerKafkaMessage val]]]))
    end

  fun box _write_to_network(conn: KafkaBrokerConnection ref, data: ByteSeqIter) =>
    conn.get_handler().writev(data)

    ifdef "enable-kafka-network-sniffing" then
      match _conf.network_sniffer
      | let ns: KafkaNetworkSniffer tag =>
        ns.data_sent(_connection_broker_id, data)
      end
    end


  fun ref produce_messages(size: I32, num_msgs: U64, msgs: Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]]): (Array[ByteSeq] val | None)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "producing messages")
    let produce_api = try _broker_apis_to_use(_KafkaProduceV0.api_key())? as
        _KafkaProduceApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting produce api to use. This should never happen."),
          "N/A", -1))
        return
      end

    let correlation_id = _next_correlation_id()

    // Don't store in _requests_buffer for produce requests which don't get a
    // response from the broker (acks == 0)
    if _conf.produce_acks != 0 then
      _requests_buffer.push((correlation_id, produce_api, (size, num_msgs,
        msgs)))
    end

    let encoded_msg = produce_api.encode_request(correlation_id, _conf, msgs)

    // Send delivery reports for produce requests which don't get a response
    // from the broker (acks == 0)
    // These are fire and forget
    if _conf.produce_acks == 0 then
      for (t, tm) in msgs.pairs() do
        for (p, pm) in tm.pairs() do
          for m in pm.values() do
            m._send_delivery_report(KafkaProducerDeliveryReport(ErrorNone, t, p,
               -1, -1, m.get_opaque()))
          end
        end
      end
    end

    encoded_msg

  // try and fetch messages from kafka
  fun ref consume_messages(conn: KafkaBrokerConnection ref) =>
    if(_conf.consumer_topics.size() > 0) then
      let fetch_request = fetch_messages()
      match consume fetch_request
      | let fr: Array[ByteSeq] iso =>
        _write_to_network(conn, consume fr)
      else
        // there are no topics/partitiions to fetch data for that are unpaused
        _all_topics_partitions_paused = true
      end
    end

  fun ref fetch_messages(): (Array[ByteSeq] iso^ | None) =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "fetching messages")
    let fetch_api = try _broker_apis_to_use(_KafkaFetchV0.api_key())? as
        _KafkaFetchApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting fetch api to use. This should never happen."),
          "N/A", -1))
        return None
      end

    let correlation_id = _next_correlation_id()

    let fetch_request = fetch_api.encode_request(correlation_id, _conf, _state.topics_state)

    // only add correlation id to requests buffer if there was actually a fetch request
    if fetch_request isnt None then
      _requests_buffer.push((correlation_id, fetch_api, None))
    end

    consume fetch_request

  // update brokers list based on what main kafka client actor knows
  fun ref update_brokers_list(brokers_list: Map[I32, (_KafkaBroker val,
    KafkaBrokerConnection tag)] val)
  =>
    _brokers = brokers_list
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Brokers list updated")

  // try and request which api versions are supported by the broker
  fun ref update_broker_api_versions(): Array[ByteSeq] iso^
  =>
    let correlation_id = _next_correlation_id()
    _requests_buffer.push((correlation_id, _KafkaApiVersionsV0, None))
    _KafkaApiVersionsV0.encode_request(correlation_id, _conf)

  fun ref refresh_metadata(conn: KafkaBrokerConnection ref) =>
    // don't request updated metadata if we already have an outstanding request
    if not metadata_refresh_request_outstanding then
      // cancel any outstanding timer for metadata refresh
      // we will create a new one when we get a response
      match metadata_refresh_timer
      | let t: Timer tag =>
        _timers.cancel(t)
        metadata_refresh_timer = None
      end

      metadata_refresh_request_outstanding = true

      let metadata_request = request_metadata()
      match consume metadata_request
      | let mr: Array[ByteSeq] iso =>
        _write_to_network(conn, consume mr)
      end
    end

  fun ref request_metadata(): (Array[ByteSeq] iso^ | None)
  =>

    let metadata_api = try _broker_apis_to_use(_KafkaMetadataV0.api_key())? as
        _KafkaMetadataApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting metadata api to use. This should never happen. current state is: " + _statemachine.current_state().string()),
          "N/A", -1))
        return None
      end

    // TODO: figure out why this happens and fix it!
    if _broker_apis_to_use.size() == 1 then
      _conf.logger(Warn) and _conf.logger.log(Warn, _name + "WARNING!!!!! Metadata requested but still in state: " +  _statemachine.current_state().string())
    end

    let correlation_id = _next_correlation_id()

    _requests_buffer.push((correlation_id, metadata_api, None))
    metadata_api.encode_request(correlation_id, _conf)

  fun ref refresh_offsets(conn: KafkaBrokerConnection ref) =>
    let offsets_request = request_offsets()
    match consume offsets_request
    | let ofr: Array[ByteSeq] iso =>
      _write_to_network(conn, consume ofr)
    end

  fun ref request_offsets(): (Array[ByteSeq] iso^ | None)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Requesting offsets")
    let offsets_api = try _broker_apis_to_use(_KafkaOffsetsV0.api_key())? as
        _KafkaOffsetsApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error getting offsets api to use. This should never happen."),
          "N/A", -1))
        return None
      end

    let correlation_id = _next_correlation_id()

    _requests_buffer.push((correlation_id, offsets_api, None))
    offsets_api.encode_request(correlation_id, _conf, _state.topics_state)

  // update internal state using metadata received
  fun ref _update_metadata(meta: _KafkaMetadata val, conn: KafkaBrokerConnection
    ref)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "Updating metadata...")
    let topics_to_throttle: Map[String, Set[I32] iso] iso =
      recover topics_to_throttle.create() end
    let topics_to_unthrottle: Map[String, Set[I32] iso] iso =
      recover topics_to_unthrottle.create() end
    var new_partition_added: Bool = false
    var have_valid_partition: Bool = false
    for topic_meta in meta.topics_metadata.values() do
      let topic = topic_meta.topic
      let topic_state = try _state.topics_state(topic)?
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "Unable to get topic state for topic: " + topic +
            "! This should never happen."),
            topic, -1))
          return
        end

      // TODO: add error handling for invalid topics and other possible errors
      let kafka_topic_error = MapKafkaError(_conf.logger, topic_meta.topic_error_code)
      match kafka_topic_error
      | ErrorNone => None
      | ErrorLeaderNotAvailable =>
        // refresh metadata if connection is ready for that
        match _statemachine.current_state()
        | _KafkaPhaseStart |
          _KafkaPhaseUpdateApiVersions |
          _KafkaPhaseSkipUpdateApiVersions
        => None
        else
          // safe to force a refresh of metadata
          refresh_metadata(conn)
        end
      else
        // fall through for ErrorUnknownTopicOrPartition, ErrorInvalidTopicException and ErrorTopicAuthorizationFailed
        // and anything else if it happens
        _conf.logger(Error) and _conf.logger.log(Error, _name +
          "Encountered topic error for topic: " + topic +
          "! Error: " + kafka_topic_error.string())
        _kafka_client._unrecoverable_error(KafkaErrorReport(kafka_topic_error,
          topic, -1))
      end

      topic_state.topic_error_code = topic_meta.topic_error_code
      topic_state.is_internal = topic_meta.is_internal

      for part_meta in topic_meta.partitions_metadata.values() do
        var current_partition_new: Bool = false
        let partition_id = part_meta.partition_id
        let part_state =
          try
            topic_state.partitions_state(partition_id)?
          else
            current_partition_new = true
            new_partition_added = true

            let ps = _KafkaTopicPartitionState(part_meta.partition_error_code,
              partition_id, part_meta.leader, part_meta.replicas,
              part_meta.isrs, part_meta.request_timestamp,
              _conf.partition_fetch_max_bytes)
            topic_state.partitions_state(partition_id) = ps
            ps
          end

        // have at least one valid partition to get offsets for
        have_valid_partition = true

        // TODO: add error handling for partition errors
        let kafka_partition_error =
          MapKafkaError(_conf.logger, part_meta.partition_error_code)
        match kafka_partition_error
        | ErrorNone => None
        | ErrorLeaderNotAvailable =>
          // refresh metadata if connection is ready for that
          match _statemachine.current_state()
          | _KafkaPhaseStart |
            _KafkaPhaseUpdateApiVersions |
            _KafkaPhaseSkipUpdateApiVersions
          => None
          else
            // safe to force a refresh of metadata
            refresh_metadata(conn)
          end
        else
          _conf.logger(Error) and _conf.logger.log(Error, _name +
            "Encountered topic error for topic: " + topic +
            ", partition: " + partition_id.string() + "! Error: "
            + kafka_partition_error.string())
          _kafka_client._unrecoverable_error(KafkaErrorReport(
            kafka_partition_error, topic, partition_id))
        end

        part_state.partition_error_code = part_meta.partition_error_code
        let old_leader = part_state.leader = part_meta.leader
        part_state.replicas = part_meta.replicas
        part_state.isrs = part_meta.isrs
        part_state.request_timestamp = part_meta.request_timestamp

        if part_meta.leader == _connection_broker_id then
          if current_partition_new or (old_leader == part_meta.leader) then
            part_state.current_leader = true
          else
            // we're not current leader until after we get handoff from old leader
            part_state.current_leader = false
            part_state.leader_change = true
            part_state.leader_change_receiver = true
          end
        elseif (part_meta.leader != _connection_broker_id) and
          (part_state.current_leader == true) then
          // broker will automagically pause fetch requests for this
          // partition because leader will be set to -1
          if not topics_to_throttle.contains(topic) then
            topics_to_throttle(topic) = recover Set[I32] end
          end
          try
            topics_to_throttle(topic)?.set(partition_id)
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to set topics_to_throttle for topic: " + topic +
              " and partition: " + partition_id.string() + "!"),
            topic, partition_id))
            return
          end
          handle_partition_leader_change(conn,
            kafka_partition_error, topic, partition_id, Array[ProducerKafkaMessage val], part_state)
        else
          part_state.current_leader = false
        end

        // Due to kafka brokers processing requests sequentially there is no
        // need to worry about there being outstanding produce requests for
        // a partition that haven't been accounted for as part of
        // `part_state.leader_change_sent_messages`.
        if part_state.leader_change and (part_state.leader != -1) and (not part_state.leader_change_receiver) then
          if part_state.current_leader then
            // The leader didn't actually end up changing
            _conf.logger(Warn) and _conf.logger.log(Warn, _name +
              "Leader didn't actually change for topic: " + topic + ", partition: " + partition_id.string() + ".")

            // Make sure we don't think we're still changing leaders any
            // longer
            part_state.leader_change = false

            // Tell kafka client to unthrottle
            if not topics_to_unthrottle.contains(topic) then
              topics_to_unthrottle(topic) = recover Set[I32] end
            end
            try
              topics_to_unthrottle(topic)?.set(partition_id)
            else
              _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                "Unable to set topics_to_unthrottle for topic: " + topic +
                " and partition: " + partition_id.string() + "!"),
              topic, partition_id))
              return
            end

            if (part_state.leader_change_sent_messages.size() > 0) or
              (part_state.leader_change_pending_messages.size() > 0) then
              // Put `part_state.leader_change_sent_messages` and
              // `part_state.leader_change_pending_messages` back into
              // _pending_buffer
              let lcsm = part_state.leader_change_sent_messages =
                part_state.leader_change_sent_messages.create()
              let lcpm = part_state.leader_change_pending_messages =
                part_state.leader_change_pending_messages.create()
              let partition_msgs = recover iso
                  Map[I32, Array[ProducerKafkaMessage val] iso]
                end
              let lc_msgs = recover iso Array[ProducerKafkaMessage val] end
              for m in lcsm.values() do
                lc_msgs.push(m)
              end
              for m in lcpm.values() do
                lc_msgs.push(m)
              end
              partition_msgs(partition_id) = consume lc_msgs
              let produce_api = try _broker_apis_to_use(_KafkaProduceV0.api_key())?
                  as _KafkaProduceApi
                else
                  _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                    "Unable to get produce api from _broker_apis_to_use!" +
                    " This should never happen."),
                    topic, partition_id))
                  return
                end

              try
                produce_api.combine_and_split_by_message_size(_conf,
                  _pending_buffer, topic, consume val partition_msgs
                  , _state.topics_state)?
              else
                _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                  "Error in combine_and_split_by_message_size. This should never happen."),
                  "N/A", -1))
                return
              end
            end
          else
            // The leader did actually end up changing to a different broker
            _conf.logger(Warn) and _conf.logger.log(Warn, _name +
              "Leader did actually change for topic: " + topic + ", partition: " + partition_id.string() + ". The new leader is: " + part_state.leader.string())

            // Make sure we don't think we're still changing leaders any
            // longer
            part_state.leader_change = false

            (_, let new_leader_broker_tag) = try _brokers(part_state.leader)?
                  else
                    _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                      "Unable to get broker for partition leader! This should never happen."),
                      "N/A", -1))
                    return
                  end

            if (part_state.leader_change_sent_messages.size() > 0) or
              (part_state.leader_change_pending_messages.size() > 0) then
              // Send `part_state.leader_change_sent_messages` and
              // `part_state.leader_change_pending_messages` to new broker
              let lcsm = part_state.leader_change_sent_messages =
                part_state.leader_change_sent_messages.create()
              let lcpm = part_state.leader_change_pending_messages =
                part_state.leader_change_pending_messages.create()
              let partition_msgs = recover iso
                  Map[I32, Array[ProducerKafkaMessage val] iso]
                end
              let lc_msgs = recover iso Array[ProducerKafkaMessage val] end
              for m in lcsm.values() do
                lc_msgs.push(m)
              end
              for m in lcpm.values() do
                lc_msgs.push(m)
              end
              partition_msgs(partition_id) = consume lc_msgs

              // send messages over to other broker
              // TODO: combine for multiple partitions if possible for efficiency
              new_leader_broker_tag._leader_change_msgs(meta, topic, partition_id,
                consume val partition_msgs, part_state.request_offset)
            else
              new_leader_broker_tag._leader_change_msgs(meta, topic, partition_id,
                recover val Map[I32, Array[ProducerKafkaMessage val] iso] end, part_state.request_offset)
            end
          end
        end
      end
    end

    // error encountered
    if topics_to_throttle.size() > 0 then
      _conf.logger(Fine) and _conf.logger.log(Fine, _name +
        "Encountered at least one leader change error. Refreshing metadata and
        throttling producers.")
      refresh_metadata(conn)
      _kafka_client._leader_change_throttle(consume val topics_to_throttle,
        _connection_broker_id)
    end

    // error encountered
    if topics_to_unthrottle.size() > 0 then
      _conf.logger(Fine) and _conf.logger.log(Fine, _name +
        "Encountered at least one leader change resolution. Unthrottling producers.")
      _kafka_client._leader_change_unthrottle(consume val topics_to_unthrottle,
        _connection_broker_id)
    end

    // transition to updating offsets if needed
    match _statemachine.current_state()
    | _KafkaPhaseUpdateMetadata =>
      if new_partition_added or have_valid_partition then
        try
          _statemachine.transition_to(_KafkaPhaseUpdateOffsets, conn)?
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
            "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseUpdateOffsets.string()),
            "N/A", -1))
          return
        end
      end
    | _KafkaPhaseUpdateMetadataReconnect =>
      try
        _statemachine.transition_to(_KafkaPhaseDone, conn)?
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
          "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseDone.string()),
          "N/A", -1))
        return
      end
    | _KafkaPhaseDone =>
      if new_partition_added then
        // refresh offsets if we have a new partition added
        refresh_offsets(conn)
      end
    end

  fun ref _leader_change_msgs(meta: _KafkaMetadata val, topic: String, partition_id: I32,
    msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val, request_offset: I64,
    conn: KafkaBrokerConnection ref)
  =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "New leader: " + _connection_broker_id.string() + " for topic: " + topic + ", partition: " + partition_id.string() + " received handover from old leader. Will resume fetching from offset: " + request_offset.string())

      let topic_state = try _state.topics_state(topic)?
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "Unable to get topic state for topic: " + topic +
            "! This should never happen."),
            topic, partition_id))
          return
        end

      let part_state =
        try
          topic_state.partitions_state(partition_id)?
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "Unable to get partition state for topic: " + topic + " and partition: " + partition_id.string() +
            "! This should never happen."),
            topic, partition_id))
          return
        end

    // Make sure our metadata is up to date on latest leader info
    _update_metadata(meta, conn)

    part_state.request_offset = request_offset
    part_state.current_leader = true
    part_state.leader_change = false
    part_state.leader_change_receiver = false

    // add new messages to pending buffer for next send opportunity
    let produce_api = try _broker_apis_to_use(_KafkaProduceV0.api_key())? as
        _KafkaProduceApi
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Unable to get produce api from _broker_apis_to_use!" +
          " This should never happen."),
          topic, -1))
          return
      end

    try
      produce_api.combine_and_split_by_message_size(_conf, _pending_buffer,
        topic, msgs, _state.topics_state) ?
    else
      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
        "Error in combine_and_split_by_message_size. This should never happen."),
        "N/A", -1))
        return
    end

    // tell kafka client to unthrottle topic/partitions
    let topics_to_unthrottle: Map[String, Set[I32] iso] iso =
      recover topics_to_unthrottle.create() end
    topics_to_unthrottle(topic) = recover Set[I32] end

    for part_id in msgs.keys() do
      try
        topics_to_unthrottle(topic)?.set(part_id)
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Unable to set topics_to_unthrottle for topic: " + topic +
          " and partition: " + part_id.string() + "!"),
        topic, part_id))
        return
      end
    end

    if topics_to_unthrottle.size() > 0 then
      _conf.logger(Fine) and _conf.logger.log(Fine, _name +
        "Encountered at least one leader change resolution. Refreshing metadata and
        unthrottling producers.")
      _kafka_client._leader_change_unthrottle(consume val topics_to_unthrottle,
        _connection_broker_id)
    end

    // transition to updating offsets if needed

  // update state based on which offsets kafka says are available
  fun ref update_offsets(offsets: Array[_KafkaTopicOffset])
  =>
    for topic in offsets.values() do
      for part in topic.partitions_offset.values() do
        // TODO: add error handling for partition errors
        let kafka_partition_error =
          MapKafkaError(_conf.logger, part.error_code)
        match kafka_partition_error
        | ErrorNone => None
        // assume transient/ignorable for now because update metadata should have caught these
        | ErrorUnknownTopicOrPartition | ErrorNotLeaderForPartition => None
        else
          _conf.logger(Error) and _conf.logger.log(Error, _name +
            "Encountered topic error for topic: " + topic.topic +
            ", partition: " + part.partition_id.string() + "! Error: "
            + kafka_partition_error.string())
          _kafka_client._unrecoverable_error(KafkaErrorReport(
            kafka_partition_error, topic.topic, part.partition_id))
          return
        end

        try
          _state.topics_state(topic.topic)?.partitions_state(part.partition_id)?
            .error_code = part.error_code
          if _state.topics_state(topic.topic)?.partitions_state(part.partition_id)?
            .request_offset == -1 then
            _state.topics_state(topic.topic)?.partitions_state(part.partition_id)?
              .request_offset = part.offset
            _state.topics_state(topic.topic)?.partitions_state(part.partition_id)?
              .timestamp = part.timestamp
          end
        else
          _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "Error getting state for topic/partition."),
            topic.topic, part.partition_id))
          return
        end
      end
    end


// On partition leader change:
// * based on fact that all brokers provide same info in response to a metadata
// request
// * old leader broker connection sends notification (via kafka client actor?)
// to all producers that topic X/partition Y are throttled/undergoing a
// leadership change (so the producermapping can buffer requests for that
// topic/partition)
// * old leader broker connection polls to get new leader info (and passes it
// along to other broker connections/kafka client actor)
// * new leader broker connection will request pending produce requests from old
// leader broker connection providing it's leader id and fetch state (i.e.
// offset of last request so it can continue from there; unacked messages, etc)
// * old leader broker connection will send requested info to new leader broker
// connection (after confirming new leader id matches what it has on record) and
// purge this info from it's pending buffer (to avoid multiple/duplicate sends)
// * new leader broker connection will signal to all producers that
// topic X/partition Y are done with leadership change and ready to resume (to
// avoid out of order sends if producers keep sending while leader change occurs
// since there would be no causal message ordering guarantee there)
// * old leader broker connection will forward any consumer level message acks
// to new leader broker connection (this is for offset tracking)
// * what if old broker connection remains leader after election?

// On disconnect:
// * disconnected broker connection sends notification (via kafka client actor?)
// to all producers that topic X/partition Y are throttled for all
// topics/partitions it is leader for
// * on reconnect, signal to all producers that topic X/partition Y are ready to
// resume for all topics/partitions it is leader for (if leader didn't change)
// * if leader changed, will follow partition leader change logic

// on network congestion
// * throttled broker connection sends notification (via kafka client actor?)
// to all producers that topic X/partition Y are throttled for all
// topics/partitions it is leader for
// * on unthrottle broker connection sends notification (via kafka client
// actor?) to all producers that topic X/partition Y are unthrottled for all
// topics/partitions it is leader for

// due to too much buffering?
// * is this needed if we throttle due to network congestion?

  fun ref _leader_change_throttle_ack(topics_to_throttle: Map[String, Set[I32]
    iso] val, conn: KafkaBrokerConnection ref)
  =>
    // TODO: implement throttle ack and send state over to new leader logic
    None

  fun ref process_produce_response(produce_response: Map[String,
    _KafkaTopicProduceResponse], throttle_time: I32, msgs: Map[String, Map[I32,
    Array[ProducerKafkaMessage val]]], conn: KafkaBrokerConnection ref)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "processing produce response and sending delivery reports.")
    // TODO: Add logic to handle errors that can be handled by client
    // automagically
    let topics_to_throttle: Map[String, Set[I32] iso] iso =
      recover topics_to_throttle.create() end
    for (topic, topic_response) in produce_response.pairs() do
      let topic_state = try _state.topics_state(topic)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to get topics_state for topic: " + topic + "!"),
            topic, -1))
            return
          end
      let topic_msgs = try msgs(topic)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to get msgs for topic: " + topic + "!"),
            topic, -1))
            return
          end
      for (part, part_response) in topic_response.partition_responses.pairs() do
        let kafka_error = MapKafkaError(_conf.logger, part_response.error_code)
        let partition_msgs = try topic_msgs(part)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to get msgs for topic: " + topic + " and partition: " + part.string() + "!"),
            topic, part))
            return
          end
        let part_state = try topic_state.partitions_state(part)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to get partition state for topic: " + topic + " and partition: " + part.string() + "!"),
            topic, part))
            return
          end
        match kafka_error
        | ErrorNone =>
          for (i, m) in partition_msgs.pairs() do
            m._send_delivery_report(KafkaProducerDeliveryReport(kafka_error,
              topic, part, part_response.offset + i.i64(),
              part_response.timestamp, m.get_opaque()))
          end
        | ErrorLeaderNotAvailable | ErrorNotLeaderForPartition |
            ErrorUnknownTopicOrPartition
          =>
          // ErrorUnknownTopicOrPartition should only happen if a partition
          // was deleted; treat as transient and retry just in case; if it is
          // permanent, will fail after max retries.
          // TODO: implement max retries
          // broker will automagically pause fetch requests for this
          // partition because leader will be set to -1
          if not topics_to_throttle.contains(topic) then
            topics_to_throttle(topic) = recover Set[I32] end
          end
          try
            topics_to_throttle(topic)?.set(part)
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Unable to set topics_to_throttle for topic: " + topic +
              " and partition: " + part.string() + "!"),
            topic, part))
            return
          end
          handle_partition_leader_change(conn,
            kafka_error, topic, part, partition_msgs, part_state)
        else
          _conf.logger(Error) and _conf.logger.log(Error, _name +
            "Received unexpected error for topic: " + topic +
            " and partition: " + part.string() + " error: " +
            kafka_error.string())
          for (i, m) in partition_msgs.pairs() do
            m._send_delivery_report(KafkaProducerDeliveryReport(kafka_error,
              topic, part, part_response.offset + i.i64(),
              part_response.timestamp, m.get_opaque()))
          end
        end
      end
    end

    // error encountered
    if topics_to_throttle.size() > 0 then
      _conf.logger(Fine) and _conf.logger.log(Fine, _name +
        "Encountered at least one leader change error. Refreshing metadata and
        throttling producers.")
      refresh_metadata(conn)
      _kafka_client._leader_change_throttle(consume val topics_to_throttle,
        _connection_broker_id)
    end

  fun ref handle_partition_leader_change(conn: KafkaBrokerConnection ref,
    kafka_error: KafkaError, topic: String, partition_id: I32, msgs:
    Array[ProducerKafkaMessage val],
    partition_state: _KafkaTopicPartitionState)
  =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name +
      "Leader change occuring for topic: " + topic + ", partition: " + partition_id.string() + ".")

    // save messages to retry later; the rest of the messages in
    // _requests_buffer will eventually accumulate here
    if msgs.size() > 0 then
      let lcsm = partition_state.leader_change_sent_messages
      msgs.copy_to(lcsm, 0, lcsm.size(), msgs.size())
    end

    handle_partition_leader_change_pending_buffer(topic, partition_id,
        partition_state)

    // mark partition as not being current leader
    partition_state.current_leader = false
    if partition_state.leader == _connection_broker_id then
      partition_state.leader = -1 // metadata refresh will tell us the real new
                                  // leader
    end
    partition_state.leader_change = true

    // TODO: decrement retry count
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Encountered error: "
      + kafka_error.string() + " for topic: " + topic +
      " and partition: " + partition_id.string() + "!")

  fun ref handle_partition_leader_change_pending_buffer(topic: String,
    partition_id: I32, partition_state: _KafkaTopicPartitionState)
  =>
    // search through _pending_buffer to find messages for
    // this topic/partition to save into leader_change_pending_messages
    for (pb_size, pb_num_msgs, pb_msgs) in _pending_buffer.values() do
      if pb_num_msgs == 0 then
        continue
      end
      try
        if pb_msgs.contains(topic) then
          let topic_msgs = pb_msgs(topic)?
          if topic_msgs.contains(partition_id) then
            // TODO: add logic to update entries in pending buffer to reflect
            // new size now that messages for this partition have been removed
            // Without the recalc of the new size we don't use bandwidth as
            // efficiently as we would otherwise
            (_, let partition_msgs) = topic_msgs.remove(partition_id)?

            // buffer messages for when new leader is elected
            let lcpm = partition_state.leader_change_pending_messages
            partition_msgs.copy_to(lcpm, 0, lcpm.size(), partition_msgs.size())
          end
        end
      end
    end

  // send fetched messages to KafkaConsumers
  fun ref process_fetched_data(conn: KafkaBrokerConnection ref, fetch_results: Map[String,
    _KafkaTopicFetchResult], network_received_timestamp: U64)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name +
      "processing and distributing fetched data")
    let topics_to_throttle: Map[String, Set[I32] iso] iso =
      recover topics_to_throttle.create() end
    for (topic, topic_result) in fetch_results.pairs() do
      let ts = try _state.topics_state(topic)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
            "error looking up state for topic: " + topic +
            ". This should never happen."),
            topic, -1))
            return
          end
      let mh = ts.message_handler
      for (part, part_result) in topic_result.partition_responses.pairs() do
        while part_result.messages.size() > 0 do
          (let v, var k, let meta_iso) = try part_result.messages.shift()?
              else
                _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                "error shifting messages. This should never happen."),
                topic, part))
                return
              end
          let meta = consume val meta_iso
          let part_request_offset = try ts.partitions_state(part)?.request_offset
            else
              _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                "error getting part_state. This should never happen."),
                topic, part))
              return
            end
          match consume k
          | let key: (Array[U8] iso | None) =>
            if meta.get_offset() >= part_request_offset then
              var consumer: (KafkaConsumer tag | None) = None
              let key' = match consume key
              | let k': None =>
                 consumer = mh(ts.consumers, k', meta)
                 None
              | let k': Array[U8] iso =>
// TODO: Is there any way to keep `key` as an `iso` without copying???????
// This recover currentl doesn't work because `mh` is turned into a `tag` because it's a `ref` class so we can't call apply on it
//                recover
//                  let k'' = consume ref k'
//                  consumer = mh(ts.consumers, k'', meta)
//                  consume k''
//                end
                let k'' = consume val k'
                consumer = mh(ts.consumers, k'', meta)
                k''
              end
              match consumer
              | let c: KafkaConsumer tag =>
/* this is related to consumer offset tracking and might get revived when group consumer support is added
                track_consumer_unacked_message(m)
*/
                c.receive_kafka_message(consume v, consume key', meta, network_received_timestamp)
              else
                _conf.logger(Fine) and _conf.logger.log(Fine, _name +
                  "Ignoring message because consumer_message_handler returned"
                  + " None, message: " + meta.string())
              end
            else
              _conf.logger(Fine) and _conf.logger.log(Fine, _name +
                "Ignoring message because offset is lower than requested. " +
                "Requested offset: " + part_request_offset.string() +
                ", message: " + meta.string())
            end
          | let err: KafkaError =>
            match err
            | let e: (ClientErrorDecode |
                      ClientErrorNoLZ4 |
                      ClientErrorNoSnappy |
                      ClientErrorNoZlib |
                      ClientErrorShouldNeverHappen |
                      ClientErrorUnknownCompression)
            =>
              _kafka_client._unrecoverable_error(KafkaErrorReport(e, meta.get_topic(), meta.get_partition_id()))
            | let e: (ClientErrorCorruptMessage |
                      ClientErrorGZipDecompress |
                      ClientErrorLZ4Decompress |
                      ClientErrorSnappyDecompress)
            =>
              // TODO: Should these be unrecoverable also? Maybe user should decide?
              _kafka_client._recoverable_error(KafkaErrorReport(e, meta.get_topic(), meta.get_partition_id()))
            | let e: ErrorNotLeaderForPartition =>
              // broker will automagically pause fetch requests for this
              // partition because leader will be set to -1
              if not topics_to_throttle.contains(topic) then
                topics_to_throttle(topic) = recover Set[I32] end
              end
              try
                topics_to_throttle(topic)?.set(part)
              else
                _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                  "Unable to set topics_to_throttle for topic: " + topic +
                  " and partition: " + part.string() + "!"),
                topic, part))
                return
              end
              try
                handle_partition_leader_change(conn,
                  e, topic, part, Array[ProducerKafkaMessage val], ts.partitions_state(part)?)
              else
                _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
                  "error getting part_state. This should never happen."),
                  topic, part))
                return
              end
            else
              // fall through for ErrorOffsetOutOfRange, ErrorUnknownTopicOrPartition, ErrorReplicaNotAvailable
              // all others default to unrecoverable
              _kafka_client._unrecoverable_error(KafkaErrorReport(err, meta.get_topic(), meta.get_partition_id()))
            end
          end
        end
      end
    end

    // error encountered
    if topics_to_throttle.size() > 0 then
      _conf.logger(Fine) and _conf.logger.log(Fine, _name +
        "Encountered at least one leader change error. Refreshing metadata and
        throttling producers.")
      refresh_metadata(conn)
      _kafka_client._leader_change_throttle(consume val topics_to_throttle,
        _connection_broker_id)
    end

/* this is related to consumer offset tracking and might get revived when group consumer support is added
// LIKELY BITROTTED
  fun ref track_consumer_unacked_message(msg: KafkaMessage val) =>
    try
       _state.consumer_unacked_offsets(msg._get_topic_partition())?
         .set(msg.get_offset())
    else
      let po = recover Set[I64] end
      po.set(msg.get_offset())
      _state.consumer_unacked_offsets(msg._get_topic_partition()) = consume po
    end
*/

  // update state for next offsets to request based on fetched data
  fun ref update_offsets_fetch_response(fetch_results: Map[String,
    _KafkaTopicFetchResult]): Bool
  =>
    // default to fetching based on timer interval
    var fetch_immediately: Bool = false

    for topic in fetch_results.values() do
      for part in topic.partition_responses.values() do
        if part.largest_offset_seen != -1 then
          try
            _state.topics_state(topic.topic)?.partitions_state(part.partition_id)?
              .request_offset = part.largest_offset_seen + 1
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Error getting state for topic/partition."),
              topic.topic, part.partition_id))
          end

          // we read new data from a topic...
          // fetch immediately because there is likely more data waiting for us to fetch..
          fetch_immediately = true
        end
      end
    end

    fetch_immediately


  // dispatch method for decoding response from kafka after matching it up to
  // appropriate requested correlation id
  fun ref decode_response(conn: KafkaBrokerConnection ref, data: Array[U8] iso,
    network_received_timestamp: U64)
  =>
    (let sent_correlation_id, let request_type, let extra_request_data) =
      try _requests_buffer.shift()?
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
          "Error we received a response to our requests_buffer is empty"
          " in decode_response. This should never happen."),
          "N/A", -1))
        return
      end

    _rb.append(consume data)

    let resp_correlation_id = try _KafkaResponseHeader.decode(_conf.logger, _rb)?
      else
        _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorDecode(_name + "Error decoding correlation id."),
          "N/A", -1))
        return
      end

    if resp_correlation_id != sent_correlation_id then
      _conf.logger(Error) and _conf.logger.log(Error, _name +
        "Correlation ID from kafka server doesn't match: sent: " +
        sent_correlation_id.string() + ", received: " +
        resp_correlation_id.string())

      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorCorrelationIdMismatch,
        "N/A", -1))
      return
    end

    match request_type
    | let api_versions_api: _KafkaApiVersionsApi
      =>
        try
          update_api_versions_to_use(api_versions_api
            .decode_response(_conf.logger, _rb)?)

          // Update metadata
          match _statemachine.current_state()
          | _KafkaPhaseUpdateApiVersions =>
            try
              _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)?
            else
              _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
                "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseUpdateMetadata.string()),
                "N/A", -1))
              return
            end
          | _KafkaPhaseUpdateApiVersionsReconnect =>
            try
              _statemachine.transition_to(_KafkaPhaseUpdateMetadataReconnect, conn)?
            else
              _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
                "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseUpdateMetadataReconnect.string()),
                "N/A", -1))
              return
            end
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name +
              "Error we're in state: " + _statemachine.current_state().string() +
              " in decode_response. This should never happen."),
              "N/A", -1))
            return
          end
        else
          // error decoding api versions response
          _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Error decoding ApiVersions response. Falling back to skip use of ApiVersions.")
          try
            _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersions, conn)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
              "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseSkipUpdateApiVersions.string()),
              "N/A", -1))
            return
          end
        end
    | let metadata_api: _KafkaMetadataApi
      =>
        metadata_refresh_request_outstanding = false

        try
          let metadata = metadata_api.decode_response(_conf.logger, _rb)?
          _conf.logger(Fine) and _conf.logger.log(Fine, _name + metadata.string())

          // send kafka client updated metadata
          _kafka_client._update_metadata(metadata)
        else
          // if we were only supposed to get metadata this is an unrecoverable failure.
          if _metadata_only then
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorDecode(_name + "Error decoding metadata response."),
              "N/A", -1))
          else
            _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Error decoding metadata response.")
          end
        end

        if _statemachine.current_state() is _KafkaPhaseDone then
          // cancel any pre-existing timers (in case we have multiple refresh
          // requests issued)
          match metadata_refresh_timer
          | let t: Timer tag =>
            _timers.cancel(t)
            metadata_refresh_timer = None
          end

          // create a new timer to refresh metadata
          let timer = Timer(_KafkaRefreshMetadataTimerNotify(conn),
            _conf.refresh_metadata_interval)
          metadata_refresh_timer = timer
          _timers(consume timer)
        else
          if _metadata_only then
            _conf.logger(Info) and _conf.logger.log(Info, _name +
              "Done updating metadata. Ready to shut down.")
            _num_reconnects_left = -1
            return
          end
        end
    | let offsets_api: _KafkaOffsetsApi
      =>
        let offsets = try offsets_api.decode_response(_conf.logger, _rb)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorDecode(_name + "Error " +
              "decoding offsets response."),
              "N/A", -1))
            return
          end

        if _conf.logger(Fine) then
          var offsets_str = recover ref String end
          for o in offsets.values() do
            offsets_str.>append(", ").>append(o.string())
          end
          _conf.logger.log(Fine, _name + offsets_str.string())
        end

        update_offsets(offsets)
        if _statemachine.current_state() isnt _KafkaPhaseDone then
          try
            _statemachine.transition_to(_KafkaPhaseDone, conn)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorTransitioningFSMStates(_name +
              "Error transitioning from " + _statemachine.current_state().string() + " to " + _KafkaPhaseDone.string()),
              "N/A", -1))
            return
          end
        end
    | let fetch_api: _KafkaFetchApi
      =>
        _conf.logger(Fine) and _conf.logger.log(Fine, _name +
          "decoding fetched data")
        (let throttle_time_ms, let fetched_data) =
          try
            fetch_api.decode_response(conn, _conf.check_crc, _conf.logger, _rb,
              _state.topics_state)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorDecode(_name + "Error " +
              "decoding fetch response."),
              "N/A", -1))
            return
          end

        if _conf.logger(Fine) then
          var fetched_str = recover ref String end
          for f in fetched_data.values() do
            fetched_str.>append(", ").>append(f.string())
          end
          _conf.logger.log(Fine, _name + fetched_str.string())
        end

        process_fetched_data(conn, fetched_data, network_received_timestamp)
        if update_offsets_fetch_response(fetched_data) then
          // we got some new data in our last request..
          // request again immediately to keep latency down until backpressure kicks in
          consume_messages(conn)
        else
          let timer = Timer(_KafkaFetchRequestTimerNotify(conn),
            _conf.fetch_interval)
          fetch_data_timer = timer
          _timers(consume timer)
        end
    | let produce_api: _KafkaProduceApi
      =>
        _conf.logger(Fine) and _conf.logger.log(Fine, _name +
          "decoding produce response")
        (let produce_response, let throttle_time) =
          try
            produce_api.decode_response(_conf.logger, _rb)?
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorDecode(_name + "Error " +
              "decoding produce response."),
              "N/A", -1))
            return
          end
        (let size, let num_msgs, let msgs) = match extra_request_data
          | (let s: I32, let n: U64, let m: Map[String, Map[I32,
            Array[ProducerKafkaMessage val]]]) => (s, n, m)
          else
            _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorShouldNeverHappen(_name + "Error " +
              "casting extra_request_data in produce response. This should " +
              "never happen."),
              "N/A", -1))
            return
          end

        if _conf.logger(Fine) then
          var produced_str = recover ref String end
          for p in produce_response.values() do
            produced_str.>append(", ").>append(p.string())
          end
          _conf.logger.log(Fine, _name + produced_str.string())
        end

        process_produce_response(produce_response, throttle_time, consume msgs,
          conn)
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name +
        "Unknown kafka request type for response. Correlation ID: " +
        resp_correlation_id.string() + ", request_type: " +
        request_type.string() + ".")

      _kafka_client._unrecoverable_error(KafkaErrorReport(ClientErrorUnknownRequest,
        "N/A", -1))

      return
    end

    // check if batch send timer is active; if not, processing pending buffer so
    // we make progress if we're below max in-flight messages again
    if send_batch_timer is None then
      process_pending_buffer(conn)
    end

    _rb.clear()

  // assign proper api versions to use after getting supported version info from
  // broker
  fun ref update_api_versions_to_use(api_versions_supported: Array[(I16, I16,
    I16)])
  =>
    _broker_apis_to_use.clear()
    for (api_key, min_version, max_version) in api_versions_supported.values()
      do
      var current_version = max_version
      while current_version >= min_version do
        match (api_key, current_version)
        | (_KafkaProduceV0.api_key(), _KafkaProduceV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaProduceV0
          break
        | (_KafkaProduceV1.api_key(), _KafkaProduceV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaProduceV1
          break
        | (_KafkaProduceV2.api_key(), _KafkaProduceV2.version()) =>
          _broker_apis_to_use(api_key) = _KafkaProduceV2
          break
        | (_KafkaFetchV0.api_key(), _KafkaFetchV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaFetchV0
          break
        | (_KafkaFetchV1.api_key(), _KafkaFetchV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaFetchV1
          break
        | (_KafkaFetchV2.api_key(), _KafkaFetchV2.version()) =>
          _broker_apis_to_use(api_key) = _KafkaFetchV2
          break
//        | (_KafkaFetchV3.api_key(), _KafkaFetchV3.version()) =>
//          _broker_apis_to_use(api_key) = _KafkaFetchV3
//          break
        | (_KafkaOffsetsV0.api_key(), _KafkaOffsetsV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetsV0
          break
        | (_KafkaOffsetsV1.api_key(), _KafkaOffsetsV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetsV1
          break
        | (_KafkaMetadataV0.api_key(), _KafkaMetadataV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaMetadataV0
          break
        | (_KafkaMetadataV1.api_key(), _KafkaMetadataV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaMetadataV1
          break
        | (_KafkaMetadataV2.api_key(), _KafkaMetadataV2.version()) =>
          _broker_apis_to_use(api_key) = _KafkaMetadataV2
          break
        | (_KafkaLeaderAndIsrV0.api_key(), _KafkaLeaderAndIsrV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaLeaderAndIsrV0
          break
        | (_KafkaStopReplicaV0.api_key(), _KafkaStopReplicaV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaStopReplicaV0
          break
        | (_KafkaUpdateMetadataV0.api_key(), _KafkaUpdateMetadataV0.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV0
          break
        | (_KafkaUpdateMetadataV1.api_key(), _KafkaUpdateMetadataV1.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV1
          break
        | (_KafkaUpdateMetadataV2.api_key(), _KafkaUpdateMetadataV2.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV2
          break
        | (_KafkaUpdateMetadataV3.api_key(), _KafkaUpdateMetadataV3.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV3
          break
        | (_KafkaControlledShutdownV1.api_key(),
          _KafkaControlledShutdownV1.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaControlledShutdownV1
          break
        | (_KafkaOffsetCommitV0.api_key(), _KafkaOffsetCommitV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0
          break
        | (_KafkaOffsetCommitV1.api_key(), _KafkaOffsetCommitV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0
          break
        | (_KafkaOffsetCommitV2.api_key(), _KafkaOffsetCommitV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0
          break
        | (_KafkaOffsetFetchV0.api_key(), _KafkaOffsetFetchV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetFetchV0
          break
        | (_KafkaOffsetFetchV1.api_key(), _KafkaOffsetFetchV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetFetchV1
          break
        | (_KafkaOffsetFetchV2.api_key(), _KafkaOffsetFetchV2.version()) =>
          _broker_apis_to_use(api_key) = _KafkaOffsetFetchV2
          break
        | (_KafkaGroupCoordinatorV0.api_key(),
          _KafkaGroupCoordinatorV0.version())
        =>
          _broker_apis_to_use(api_key) = _KafkaGroupCoordinatorV0
          break
        | (_KafkaJoinGroupV0.api_key(), _KafkaJoinGroupV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaJoinGroupV0
          break
        | (_KafkaJoinGroupV1.api_key(), _KafkaJoinGroupV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaJoinGroupV1
          break
        | (_KafkaHeartbeatV0.api_key(), _KafkaHeartbeatV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaHeartbeatV0
          break
        | (_KafkaLeaveGroupV0.api_key(), _KafkaLeaveGroupV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaLeaveGroupV0
          break
        | (_KafkaSyncGroupV0.api_key(), _KafkaSyncGroupV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaSyncGroupV0
          break
        | (_KafkaDescribeGroupsV0.api_key(), _KafkaDescribeGroupsV0.version())
          => _broker_apis_to_use(api_key) = _KafkaDescribeGroupsV0
          break
        | (_KafkaListGroupsV0.api_key(), _KafkaListGroupsV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaListGroupsV0
          break
        | (_KafkaSaslHandshakeV0.api_key(), _KafkaSaslHandshakeV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaSaslHandshakeV0
          break
        | (_KafkaApiVersionsV0.api_key(), _KafkaApiVersionsV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaApiVersionsV0
          break
        | (_KafkaCreateTopicsV0.api_key(), _KafkaCreateTopicsV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaCreateTopicsV0
          break
        | (_KafkaCreateTopicsV1.api_key(), _KafkaCreateTopicsV1.version()) =>
          _broker_apis_to_use(api_key) = _KafkaCreateTopicsV1
          break
        | (_KafkaDeleteTopicsV0.api_key(), _KafkaDeleteTopicsV0.version()) =>
          _broker_apis_to_use(api_key) = _KafkaDeleteTopicsV0
          break
        end
        current_version = current_version - 1
      end
    end


// timer notify for reconnection a disconnected kafka connection
class _ReconnectTimerNotify is TimerNotify
  let _conn: CustomTCPConnection tag

  new iso create(conn: CustomTCPConnection tag) =>
    _conn = conn

  fun ref apply(timer: Timer, count: U64): Bool =>
    _conn.reconnect()
    false

// timer notify for fetching data from the broker
class _KafkaFetchRequestTimerNotify is TimerNotify
  let _conn: KafkaBrokerConnection tag

  new iso create(conn: KafkaBrokerConnection tag) =>
    _conn = conn

  fun ref apply(timer: Timer, count: U64): Bool =>
    _conn._consume_messages()
    false

// timer notify for requesting metadata from the broker
class _KafkaRefreshMetadataTimerNotify is TimerNotify
  let _conn: KafkaBrokerConnection tag

  new iso create(conn: KafkaBrokerConnection tag) =>
    _conn = conn

  fun ref apply(timer: Timer, count: U64): Bool =>
    _conn._refresh_metadata()
    false

// timer notify for sending pending data from the broker
class _KafkaSendPendingMessagesTimerNotify is TimerNotify
  let _conn: KafkaBrokerConnection tag

  new iso create(conn: KafkaBrokerConnection tag) =>
    _conn = conn

  fun ref apply(timer: Timer, count: U64): Bool =>
    _conn._send_pending_messages()
    false
