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
  let consumer_unacked_offsets: Map[KafkaTopicPartition, Set[I64] iso] = consumer_unacked_offsets.create()

  new create()
  =>
    None

  fun string(): String =>
    var topm_str = recover ref String end
    for t in topics_state.values() do topm_str.append(", ").append(t.string()) end
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
  let partitions_state: Map[I32, _KafkaTopicPartitionState] = partitions_state.create()
  var consumers: Array[KafkaConsumer tag] val = recover val consumers.create() end
  var message_handler: KafkaConsumerMessageHandler

  new create(topic': String, message_handler': KafkaConsumerMessageHandler, topic_error_code': I16 = 0, is_internal': (Bool | None) = None)
  =>
    topic_error_code = topic_error_code'
    topic = topic'
    is_internal = is_internal'
    message_handler = message_handler'

  fun string(): String =>
    var parts_str = recover ref String end
    for p in partitions_state.values() do parts_str.append(", ").append(p.string()) end
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
  var request_offset: I64 = 0
  var max_bytes: I32
  var paused: Bool = true
  var old_leader: Bool = false
  var current_leader: Bool = false

  new create(partition_error_code': I16, partition_id': I32, leader': I32, replicas': Array[I32] val, isrs': Array[I32] val, request_timestamp': I64 = -2, max_bytes': I32 = 32768)
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
    for r in replicas.values() do replicas_str.append(", ").append(r.string()) end
    var isrs_str = recover ref String end
    for i in isrs.values() do isrs_str.append(", ").append(i.string()) end
    "KafkaTopicPartitionMetadata: [ "
      + "partition_error_code = " + partition_error_code.string()
      + ", partition_id = " + partition_id.string()
      + ", leader = " + leader.string()
      + ", old_leader = " + old_leader.string()
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
  let _pending_buffer: Array[(I32, U64, Map[String, Map[I32, Array[ProducerKafkaMessage val]]])] = _pending_buffer.create(64)
  let _requests_buffer: Array[(I32, _KafkaApi, (None | (I32, U64, Map[String, Map[I32, Array[ProducerKafkaMessage val]]])))] = _requests_buffer.create(64)
  var _broker_apis_to_use: Map[I16, _KafkaApi] = _broker_apis_to_use.create()
  var _api_versions_supported: Bool = true

  let _state: _KafkaState = _KafkaState

  var _brokers: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val = recover val _brokers.create() end
  let _connection_broker_id: I32

  let _conf: KafkaConfig val
  let _metadata_only: Bool
  let _kafka_client: KafkaClient

  let _timers: Timers = Timers
  let _reconnect_closed_delay: U64
  let _reconnect_failed_delay: U64

  let _statemachine: Fsm[CustomTCPConnection ref]

  var send_batch_timer: (Timer tag | None) = None
  var metadata_refresh_timer: (Timer tag | None) = None
  var metadata_refresh_request_outstanding: Bool = false
  var fetch_data_timer: (Timer tag | None) = None

  var _all_topics_partitions_paused: Bool = true

  let _name: String

  new create(client: KafkaClient, conf: KafkaConfig val, topic_consumer_handlers: Map[String, KafkaConsumerMessageHandler val] val, connection_broker_id: I32 = -1, reconnect_closed_delay: U64 = 100_000_000, reconnect_failed_delay: U64 = 10_000_000_000) =>
    _conf = conf
    _kafka_client = client
    _connection_broker_id = connection_broker_id
    _metadata_only = (connection_broker_id == -1)
    _reconnect_closed_delay = reconnect_closed_delay
    _reconnect_failed_delay = reconnect_failed_delay

    _name = _conf.client_name + "#" + _connection_broker_id.string() + ": "

    // initialize pending buffer
    _pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage val]]]))

    for (topic, consumer_handler) in topic_consumer_handlers.pairs() do
      _state.topics_state(topic) = _KafkaTopicState(topic, consumer_handler.clone())
    end

    _statemachine = Fsm[CustomTCPConnection ref](_conf.logger)

    // simple intialization phase state machine
    try
      _statemachine.add_allowed_state(_KafkaPhaseStart)
      _statemachine.add_allowed_state(_KafkaPhaseSkipUpdateApiVersions)
      _statemachine.add_allowed_state(_KafkaPhaseUpdateApiVersions)
      _statemachine.add_allowed_state(_KafkaPhaseUpdateMetadata)
      _statemachine.add_allowed_state(_KafkaPhaseUpdateOffsets)
      _statemachine.add_allowed_state(_KafkaPhaseDone)
      _statemachine.add_allowed_state(_KafkaPhaseUpdateApiVersionsReconnect)
      _statemachine.add_allowed_state(_KafkaPhaseSkipUpdateApiVersionsReconnect)
      _statemachine.add_allowed_state(_KafkaPhaseUpdateMetadataReconnect)

      _statemachine.valid_transition(FsmStateAny, _KafkaPhaseStart, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) => None} ref)

      _statemachine.valid_transition(FsmStateAny, _KafkaPhaseUpdateApiVersions, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) => conn.get_handler().writev(kh.update_broker_api_versions())} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersions, _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.refresh_metadata(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersions, _KafkaPhaseSkipUpdateApiVersions, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.set_fallback_api_versions(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseSkipUpdateApiVersions, _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.refresh_metadata(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseStart, _KafkaPhaseUpdateMetadata, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.refresh_metadata(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateMetadata, _KafkaPhaseUpdateOffsets, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => conn.get_handler().writev(kh.request_offsets())} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateOffsets, _KafkaPhaseDone, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.done_and_consume(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseDone, _KafkaPhaseUpdateApiVersionsReconnect, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) => conn.get_handler().writev(kh.update_broker_api_versions())} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersionsReconnect, _KafkaPhaseUpdateMetadataReconnect, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.refresh_metadata(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateApiVersionsReconnect, _KafkaPhaseSkipUpdateApiVersionsReconnect, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.set_fallback_api_versions(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseSkipUpdateApiVersionsReconnect, _KafkaPhaseUpdateMetadataReconnect, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.refresh_metadata(conn)} ref)

      _statemachine.valid_transition(_KafkaPhaseUpdateMetadataReconnect, _KafkaPhaseDone, {ref(old_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], conn: CustomTCPConnection ref)(kh = this) ? => kh.done_and_consume_reconnect(conn)} ref)

      _statemachine.initialize(_KafkaPhaseStart, {(old_state: FsmState val, new_state: FsmState val, state_machine: Fsm[CustomTCPConnection ref], data: CustomTCPConnection ref)(kh = this, logger = _conf.logger, name = _name) => logger(Error) and logger.log(Error, name + "Error transitioning states from " + old_state.string() + " to " + new_state.string())} ref)

    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error initializing state machine!")
    end

  fun ref _update_consumers(topic_consumers: Map[String, Array[KafkaConsumer tag] val] val) =>
    for (topic, consumers) in topic_consumers.pairs() do
      try
        _state.topics_state(topic).consumers = consumers
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "Error updating consumers for topic: " + topic + ". This should never happen.")
      end
    end

  fun get_conf(): KafkaConfig val => _conf

  fun ref done_and_consume(conn: CustomTCPConnection ref) ? =>
    _kafka_client._broker_initialized(_connection_broker_id)
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Done with initialization and metadata/offsets update")

    metadata_refresh_request_outstanding = false
    match metadata_refresh_timer
    | None =>
      // start metadata refresh timer
      let timer = Timer(_KafkaRefreshMetadataTimerNotify((conn as KafkaBrokerConnection)), _conf.refresh_metadata_interval)
      metadata_refresh_timer = timer
      _timers(consume timer)
    end

    if not _all_topics_partitions_paused then
      consume_messages(conn)
    end

  fun ref done_and_consume_reconnect(conn: CustomTCPConnection ref) ? =>
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Done with reconnect and metadata update")

    metadata_refresh_request_outstanding = false
    match metadata_refresh_timer
    | None =>
      // start metadata refresh timer
      let timer = Timer(_KafkaRefreshMetadataTimerNotify((conn as KafkaBrokerConnection)), _conf.refresh_metadata_interval)
      metadata_refresh_timer = timer
      _timers(consume timer)
    end

    if not _all_topics_partitions_paused then
      consume_messages(conn)
    end

  fun ref _update_consumer_message_handler(topic: String, consumer_handler: KafkaConsumerMessageHandler val) =>
    try
      _state.topics_state(topic).message_handler = consumer_handler.clone()
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error updating message handler for topic: " + topic + ". This should never happen.")
    end

  fun ref _consumer_pause(conn: CustomTCPConnection ref, topic: String, partition_id: I32) =>
    // set to paused
    // if everything is paused, next attempt to fetch_data will set _all_topics_partitions_paused = true
    try
      _state.topics_state(topic).partitions_state(partition_id).paused = true
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error pausing consumer for topic: " + topic + " and partition: " + partition_id.string() + ".")
    end

  fun ref _consumer_pause_all(conn: CustomTCPConnection ref) =>
    // set all to paused
    for (topic, ts) in _state.topics_state.pairs() do
      for (partition_id, ps) in ts.partitions_state.pairs() do
        ps.paused = true
      end
    end

  fun ref _consumer_resume(conn: CustomTCPConnection ref, topic: String, partition_id: I32) =>
    // set to unpaused
    try
      _state.topics_state(topic).partitions_state(partition_id).paused = false

      let was_totally_paused = _all_topics_partitions_paused = false

      // if we're fully initialized and we used to be completely paused
      if (_statemachine.current_state() is _KafkaPhaseDone) and (was_totally_paused == true) then
        try
          consume_messages(conn)
        else
          _conf.logger(Error) and _conf.logger.log(Error, _name + "error consuming messages in consumer resume.")
        end
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error resuming consumer for topic: " + topic + " and partition: " + partition_id.string() + ".")
    end

  fun ref _consumer_resume_all(conn: CustomTCPConnection ref) =>
    let was_totally_paused = _all_topics_partitions_paused = false

    // set all to unpaused
    for (topic, ts) in _state.topics_state.pairs() do
      for (partition_id, ps) in ts.partitions_state.pairs() do
        ps.paused = false
      end
    end

    // if we're fully initialized and we used to be completely paused
    if (_statemachine.current_state() is _KafkaPhaseDone) and (was_totally_paused == true) then
      try
        consume_messages(conn)
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "error consuming messages in consumer resume all.")
      end
    end

  fun ref message_consumed(msg: KafkaMessage val, success: Bool) =>
    // What to do if `success` is `false`? Can we do anything meaningful aside from ensuring that largest offset not successfully consumed is not committed?
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "received message consumed. Success: " + success.string() + ", msg: " + msg.string())
    if success then
      // TODO: Does it matter if we get an ack for something that we're not tracking any longer or at all?
      try
        _state.consumer_unacked_offsets(msg._get_topic_partition()).unset(msg.get_offset())
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "error removing message from track unacked offset collection. msg: " + msg.string())
      end
    end

  fun ref received(conn: CustomTCPConnection ref, data: Array[U8] iso,
    times: USize): Bool
  =>
    let network_received_timestamp: U64 = Time.nanos()
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Kafka client received " + data.size().string() + " bytes")
    if _header then
      try
        let payload_size: USize = payload_length(consume data)

        conn.get_handler().expect(payload_size)
        _header = false
      end
      true
    else
      try
        decode_response((conn as KafkaBrokerConnection), consume data, network_received_timestamp)
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "Error decoding kafka message")
      end

      conn.get_handler().expect(_header_length)
      _header = true

      ifdef linux then
        true
      else
        false
      end
    end

  fun ref accepted(conn: CustomTCPConnection ref) =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Kafka client accepted a connection")
    conn.get_handler().expect(_header_length)

  fun ref connecting(conn: CustomTCPConnection ref, count: U32) =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Kafka client connecting")

  fun ref connected(conn: CustomTCPConnection ref) =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Kafka client successfully connected")
    try
      initialize_connection(conn)
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error initializing kafka connection")
    end

  fun ref expect(conn: CustomTCPConnection ref, qty: USize): USize =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Kafka client expecting " + qty.string() + " bytes")
    qty

  fun ref connect_failed(conn: CustomTCPConnection ref) =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Kafka client failed connection")
    let t = Timer(_ReconnectTimerNotify(conn), _reconnect_failed_delay, 0)
    _timers(consume t)

  fun ref auth_failed(conn: CustomTCPConnection ref) =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Kafka client failed authentication")

  fun ref closed(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Kafka client closed connection")

    // clean up pending requests since they're invalid now and need to be retried (for produce requests only)
    try
      while _requests_buffer.size() > 0 do
        (let sent_correlation_id, let request_type, let extra_request_data) = _requests_buffer.pop()

        match request_type
        // if it was a produce request, put it back on the pending buffer
        | let produce_api: _KafkaProduceApi
          =>
            match consume extra_request_data
               | (let s: I32, let n: U64, let m: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]) =>
                 _pending_buffer.unshift((s, n, consume m))
               else
                 _conf.logger(Error) and _conf.logger.log(Error, _name + "Error casting extra_request_data in produce response. This should never happen.")
               end
        end
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error cleaning up pending requests. This should never happen.")

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

    let t = Timer(_ReconnectTimerNotify(conn), _reconnect_closed_delay, 0)
    _timers(consume t)

  fun ref throttled(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Kafka client is throttled")
    _kafka_client._throttle_producers(_connection_broker_id)

  fun ref unthrottled(conn: CustomTCPConnection ref) =>
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Kafka client is unthrottled")
    _kafka_client._unthrottle_producers(_connection_broker_id)

  fun dispose() =>
    _conf.logger(Info) and _conf.logger.log(Info, _name + "Disposing any active reconnect timers...")
    _timers.dispose()

  // correlation id is assigned to each request sent to the broker. Brokers always send back correlation id with each response and process all requests sequentially.
  fun ref _next_correlation_id(): I32 =>
    _correlation_id = _correlation_id + 1

  fun payload_length(data: Array[U8] val): USize ? =>
    let rb = recover ref Reader end
    rb.append(consume data)
    try
      BigEndianDecoder.i32(rb).usize()
    else
      error
    end

  fun ref initialize_connection(conn: CustomTCPConnection ref) ? =>
    match (_statemachine.current_state(), _api_versions_supported)
    | (_KafkaPhaseUpdateApiVersions, _) =>
      // if we're reconnecting when we had requested ApiVersions, we need to fall back to a fixed set of functionality because broker doesn't support ApiVersions call
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersions, conn)
    | (_KafkaPhaseUpdateApiVersionsReconnect, _) =>
      // if we're reconnecting when we had requested ApiVersions, we need to fall back to a fixed set of functionality because broker doesn't support ApiVersions call
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersionsReconnect, conn)
    | (_KafkaPhaseStart, true) =>
      _statemachine.transition_to(_KafkaPhaseUpdateApiVersions, conn)
    | (_KafkaPhaseStart, false) =>
      // if ApiVersions is not supported, we need to fall back to a fixed set of functionality
      _statemachine.transition_to(_KafkaPhaseSkipUpdateApiVersions, conn)
    | (_KafkaPhaseDone, _) =>
      // if we're already fully initialized
      // retry the ApiVersions call in case broker was upgraded
      _api_versions_supported = true
      _statemachine.transition_to(_KafkaPhaseUpdateApiVersionsReconnect, conn)
    else
      // we're not fully initialized but we're past ApiVersions
      if not (_statemachine.current_state() is _KafkaPhaseUpdateApiVersions) and _api_versions_supported then
        _statemachine.transition_to(_KafkaPhaseUpdateApiVersions, conn)
      else
        _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)
      end
    end
    conn.get_handler().expect(_header_length)

  fun ref set_fallback_api_versions(conn: CustomTCPConnection ref) ? =>
    _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Got disconnected requesting broker api versions")
    _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Falling back to defaults (version 0.8)")
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
      _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)
    | _KafkaPhaseSkipUpdateApiVersionsReconnect =>
      _statemachine.transition_to(_KafkaPhaseUpdateMetadataReconnect, conn)
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Error we're in state: " + _statemachine.current_state().string() + " in set_fallback_api_versions. This should never happen.")
    end


  // Rules for kafka requests
  // * Only one fetch request at a time (based on a timer; fetch requests timeout wait is always done in client side and requests to broker always request data immediately in order to allow other requests [produce, metadata, etc] to be done in the meantime due to kafka's pipelined processing of requests)
  // * Only one metadata update request at a time (based on a timer to learn of changes to topics we care about (new partitions) and brokers being added)
  // * Only one update offsets request at a time; does this even need to occur outside of reconnect/initial connect?
  // * Multiple produce requests at a time
  // * Produce requests go through a queue which is limited by max outstanding requests
  // * Produce requests are the only ones that need to be broken up into multiple requests due to size
  // * fetch/metadata update/update offsets can skip ahead of produces requests in the queue
  // * TODO: figure out how group consumer related stuff fits in


  // send request to produce messages to kafka broker.. this comes from KafkaProducers by use of the KafkaProducerMapping
  // auth: KafkaProducerAuth is a guard to ensure people don't try and send messages without using the KafkaProducerMapping
  fun ref send_kafka_message(conn: KafkaBrokerConnection ref, topic: String, partition_id: I32, msg: ProducerKafkaMessage val, auth: _KafkaProducerAuth) ? =>
    let produce_api = _broker_apis_to_use(_KafkaProduceV0.api_key()) as _KafkaProduceApi
    produce_api.combine_and_split_by_message_size_single(_conf, _pending_buffer, topic, partition_id, msg, _state.topics_state)

    _maybe_process_pending_buffer(conn)

  fun ref send_kafka_messages(conn: KafkaBrokerConnection ref, topic: String, msgs: Map[I32, Array[ProducerKafkaMessage val] iso] val, auth: _KafkaProducerAuth) ? =>
    let produce_api = _broker_apis_to_use(_KafkaProduceV0.api_key()) as _KafkaProduceApi
    produce_api.combine_and_split_by_message_size(_conf, _pending_buffer, topic, msgs, _state.topics_state)

    _maybe_process_pending_buffer(conn)

  fun ref _maybe_process_pending_buffer(conn: KafkaBrokerConnection ref) ? =>
    (_, let num_msgs, _) = _pending_buffer(0)
    if num_msgs == 0 then
      return
    end

    // check if batch send timer is active
    match send_batch_timer
    | let t: Timer tag =>
      // check if we've accumulated max_message_size of data to send (we'll have more than 1 pending_buffer entry if yes)
      if _pending_buffer.size() > 1 then
        // send everything but last buffer entry since it almost definitely has room for more data to accumulate
        process_pending_buffer(conn, true)

        // cancel and recreate timer for remaining messages to be sent
        _timers.cancel(t)
        let timer = Timer(_KafkaSendPendingMessagesTimerNotify(conn), _conf.max_produce_buffer_time)
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
      // if no batch send timer then check if we're supposed to be batching or not
      if _conf.max_produce_buffer_time > 0 then
        // if yes, create timer to send later
        let timer = Timer(_KafkaSendPendingMessagesTimerNotify(conn), _conf.max_produce_buffer_time)
        send_batch_timer = timer
        _timers(consume timer)
      else
        // if no, send messages
        process_pending_buffer(conn)
      end
    end

  fun ref _send_pending_messages(conn: CustomTCPConnection ref) ? =>
    send_batch_timer = None
    process_pending_buffer(conn)

  fun ref process_pending_buffer(conn: CustomTCPConnection ref, send_all_but_one: Bool = false) ? =>
    (_, let n, _) = _pending_buffer(0)
    if n == 0 then
      return
    end

    let limit: USize = if send_all_but_one then 1 else 0 end
    while (_requests_buffer.size() < _conf.max_inflight_requests) and (_pending_buffer.size() > limit) do
      // TODO: Figure out a way to avoid shift... maybe use a ring buffer?
      (let size, let num_msgs, let msgs) = _pending_buffer.shift()

      conn.get_handler().writev(produce_messages(size, num_msgs, consume msgs))
    end
    if _pending_buffer.size() == 0 then
      // initialize pending buffer
      _pending_buffer.push((0, 0, Map[String, Map[I32, Array[ProducerKafkaMessage val]]]))
    end

  fun ref produce_messages(size: I32, num_msgs: U64, msgs: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]): Array[ByteSeq] iso^ ?
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "producing messages")
    let produce_api = _broker_apis_to_use(_KafkaProduceV0.api_key()) as _KafkaProduceApi
    let correlation_id = _next_correlation_id()

    // Don't store in _requests_buffer for produce requests which don't get a response from the broker (acks == 0)
    if _conf.produce_acks != 0 then
      _requests_buffer.push((correlation_id, produce_api, (size, num_msgs, msgs)))
    end

    let encoded_msg = produce_api.encode_request(correlation_id, _conf, msgs)

    // Send delivery reports for produce requests which don't get a response from the broker (acks == 0)
    // These are fire and forget
    if _conf.produce_acks == 0 then
      for (t, tm) in msgs.pairs() do
        for (p, pm) in tm.pairs() do
          for m in pm.values() do
            m._send_delivery_report(KafkaProducerDeliveryReport(ErrorNone, t, p, -1, -1, m.get_opaque()))
          end
        end
      end
    end

    encoded_msg

  // try and fetch messages from kafka
  fun ref consume_messages(conn: CustomTCPConnection ref) ? =>
    if(_conf.consumer_topics.size() > 0) then
      let fetch_request = fetch_messages()
      match consume fetch_request
      | let fr: Array[ByteSeq] iso =>
        conn.get_handler().writev(consume fr)
      else
        // there are no topics/partitiions to fetch data for that are unpaused
        _all_topics_partitions_paused = true
      end
    end

  fun ref fetch_messages(): (Array[ByteSeq] iso^ | None) ?
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "fetching messages")
    let fetch_api = _broker_apis_to_use(_KafkaFetchV0.api_key()) as _KafkaFetchApi
    let correlation_id = _next_correlation_id()

    _requests_buffer.push((correlation_id, fetch_api, None))
    fetch_api.encode_request(correlation_id, _conf, _state.topics_state)

  // update brokers list based on what main kafka client actor knows
  fun ref update_brokers_list(brokers_list: Map[I32, (_KafkaBroker val, KafkaBrokerConnection tag)] val) =>
    _brokers = brokers_list
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Brokers list updated")

  // try and request which api versions are supported by the broker
  fun ref update_broker_api_versions(): Array[ByteSeq] iso^
  =>
    let correlation_id = _next_correlation_id()
    _requests_buffer.push((correlation_id, _KafkaApiVersionsV0, None))
    _KafkaApiVersionsV0.encode_request(correlation_id, _conf)

  fun ref refresh_metadata(conn: CustomTCPConnection ref) ? =>
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
      conn.get_handler().writev(request_metadata())
    end

  fun ref request_metadata(): Array[ByteSeq] iso^ ?
  =>
    let correlation_id = _next_correlation_id()
    let metadata_api = _broker_apis_to_use(_KafkaMetadataV0.api_key()) as _KafkaMetadataApi

    _requests_buffer.push((correlation_id, metadata_api, None))
    metadata_api.encode_request(correlation_id, _conf)

  fun ref request_offsets(): Array[ByteSeq] iso^ ?
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Requesting offsets")
    let offsets_api = _broker_apis_to_use(_KafkaOffsetsV0.api_key()) as _KafkaOffsetsApi
    let correlation_id = _next_correlation_id()

    _requests_buffer.push((correlation_id, offsets_api, None))
    offsets_api.encode_request(correlation_id, _conf, _state.topics_state)

  // update internal state using metadata received
  fun ref _update_metadata(meta: _KafkaMetadata val, conn: CustomTCPConnection ref)
  =>
    try
      for topic_meta in meta.topics_metadata.values() do
        let topic_state = try _state.topics_state(topic_meta.topic)
          else
            _conf.logger(Error) and _conf.logger.log(Error, _name + "Unable to get topic state for topic: " + topic_meta.topic + "! This should never happen.")
            error
          end

        topic_state.topic_error_code = topic_meta.topic_error_code
        topic_state.is_internal = topic_meta.is_internal

        for part_meta in topic_meta.partitions_metadata.values() do
          let part_state = try
                             topic_state.partitions_state(part_meta.partition_id)
                           else
                             let ps = _KafkaTopicPartitionState(part_meta.partition_error_code, part_meta.partition_id, part_meta.leader, part_meta.replicas, part_meta.isrs, part_meta.request_timestamp, _conf.partition_fetch_max_bytes)
                             topic_state.partitions_state(part_meta.partition_id) = ps
                             ps
                           end
          part_state.partition_error_code = part_meta.partition_error_code
          part_state.leader = part_meta.leader
          part_state.leader = part_meta.leader
          part_state.replicas = part_meta.replicas
          part_state.isrs = part_meta.isrs
          part_state.request_timestamp = part_meta.request_timestamp

          if part_meta.leader == _connection_broker_id then
            part_state.current_leader = true
          else
            part_state.old_leader = part_state.current_leader = false
          end
        end
      end
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Unable to get topic state! This should never happen.")
    end

    // transition to updating offsets if needed
    match _statemachine.current_state()
    | _KafkaPhaseUpdateMetadata =>
      try
        _statemachine.transition_to(_KafkaPhaseUpdateOffsets, conn)
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "Unable to transition to _KafkaPhaseUpdateOffsets. This should never happen.")
      end
    | _KafkaPhaseUpdateMetadataReconnect =>
      try
        _statemachine.transition_to(_KafkaPhaseDone, conn)
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "Unable to transition to _KafkaPhaseDone. This should never happen.")
      end
    end

  // update state based on which offsets kafka says are available
  fun ref update_offsets(offsets: Array[_KafkaTopicOffset]) ?
  =>
    for topic in offsets.values() do
      for part in topic.partitions_offset.values() do
        _state.topics_state(topic.topic).partitions_state(part.partition_id).request_offset = part.offset
        _state.topics_state(topic.topic).partitions_state(part.partition_id).error_code = part.error_code
        _state.topics_state(topic.topic).partitions_state(part.partition_id).timestamp = part.timestamp
      end
    end


// On partition leader change:
// * based on fact that all brokers provide same info in response to a metadata request
// * old leader broker connection sends notification (via kafka client actor?) to all producers that topic X/partition Y are throttled/undergoing a leadership change (so the producermapping can buffer requests for that topic/partition)
// * old leader broker connection polls to get new leader info (and passes it along to other broker connections/kafka client actor)
// * new leader broker connection will request pending produce requests from old leader broker connection providing it's leader id and fetch state (i.e. offset of last request so it can continue from there; unacked messages, etc)
// * old leader broker connection will send requested info to new leader broker connection (after confirming new leader id matches what it has on record) and purge this info from it's pending buffer (to avoid multiple/duplicate sends)
// * new leader broker connection will signal to all producers that topic X/partition Y are done with leadership change and ready to resume (to avoid out of order sends if producers keep sending while leader change occurs since there would be no causal message ordering guarantee there)
// * old leader broker connection will forward any consumer level message acks to new leader broker connection (this is for offset tracking)
// * what if old broker connection remains leader after election?

// On disconnect:
// * disconnected broker connection sends notification (via kafka client actor?) to all producers that topic X/partition Y are throttled for all topics/partitions it is leader for
// * on reconnect, signal to all producers that topic X/partition Y are ready to resume for all topics/partitions it is leader for (if leader didn't change)
// * if leader changed, will follow partition leader change logic

// on network congestion
// * throttled broker connection sends notification (via kafka client actor?) to all producers that topic X/partition Y are throttled for all topics/partitions it is leader for
// * on unthrottle broker connection sends notification (via kafka client actor?) to all producers that topic X/partition Y are unthrottled for all topics/partitions it is leader for

// due to too much buffering?
// * is this needed if we throttle due to network congestion?

  fun ref _leader_change_throttle_ack(topics_to_throttle: Map[String, Set[I32] val] val, conn: CustomTCPConnection ref) =>
    // TODO: implement throttle ack and send state over to new leader logic
    None

  fun ref process_produce_response(produce_response: Map[String, _KafkaTopicProduceResponse], throttle_time: I32, msgs: Map[String, Map[I32, Array[ProducerKafkaMessage val]]], conn: KafkaBrokerConnection ref) ?
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "processing produce response and sending delivery reports.")
    // TODO: Add logic to handle errors that can be handled by client automagically
    // TODO: throttle producers for any leader changes
    // TODO: buffer data for any topics/partitions with leader changes
    for (topic, topic_response) in produce_response.pairs() do
      let topic_msgs = msgs(topic)
      for (part, part_response) in topic_response.partition_responses.pairs() do
        let kafka_error = map_kafka_error(part_response.error_code)
        let partition_msgs = topic_msgs(part)
        try
          match kafka_error
          | ErrorNone =>
            for (i, m) in partition_msgs.pairs() do
              m._send_delivery_report(KafkaProducerDeliveryReport(kafka_error, topic, part, part_response.offset + i.i64(), part_response.timestamp, m.get_opaque()))
            end
          | ErrorLeaderNotAvailable => handle_partition_leader_change(conn, kafka_error, topic, part, partition_msgs)
          | ErrorNotLeaderForPartition => handle_partition_leader_change(conn, kafka_error, topic, part, partition_msgs)
            // The following should only happen if a partition was deleted; treat as transient and retry just in case; if it is permanent, will fail after max retries
          | ErrorUnknownTopicOrPartition => handle_partition_leader_change(conn, kafka_error, topic, part, partition_msgs)
          else
            _conf.logger(Error) and _conf.logger.log(Error, _name + "Received unexpected error for topic: " + topic + " and partition: " + part.string() + " error: " + kafka_error.string())
            for (i, m) in partition_msgs.pairs() do
              m._send_delivery_report(KafkaProducerDeliveryReport(kafka_error, topic, part, part_response.offset + i.i64(), part_response.timestamp, m.get_opaque()))
            end
          end
        else
            _conf.logger(Error) and _conf.logger.log(Error, _name + "Unable to send message delivery report for topic: " + topic + " and partition: " + part.string() + "!")
        end
      end
    end

  fun ref handle_partition_leader_change(conn: KafkaBrokerConnection ref, kafka_error: KafkaError, topic: String, partition_id: I32, msgs: Array[ProducerKafkaMessage val]) ? =>
    // TODO: add logic to save messages to retry later and mark partition as having no leader and decrement retry count
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Encountered error: " + kafka_error.string() + "Refreshing metadata to get new leader info for topic: " + topic + " and partition: " + partition_id.string() + "!")
    refresh_metadata(conn)

  fun map_kafka_error(error_code: I16): KafkaError =>
    match (error_code)
    | ErrorNone() => ErrorNone
    | ErrorUnknown() => ErrorUnknown
    | ErrorOffsetOutOfRange() => ErrorOffsetOutOfRange
    | ErrorCorruptMessage() => ErrorCorruptMessage
    | ErrorUnknownTopicOrPartition() => ErrorUnknownTopicOrPartition
    | ErrorInvalidFetchSize() => ErrorInvalidFetchSize
    | ErrorLeaderNotAvailable() => ErrorLeaderNotAvailable
    | ErrorNotLeaderForPartition() => ErrorNotLeaderForPartition
    | ErrorRequestTimedOut() => ErrorRequestTimedOut
    | ErrorBrokerNotAvailable() => ErrorBrokerNotAvailable
    | ErrorReplicaNotAvailable() => ErrorReplicaNotAvailable
    | ErrorMessageTooLarge() => ErrorMessageTooLarge
    | ErrorStaleControllerEpoch() => ErrorStaleControllerEpoch
    | ErrorOffsetMetadataTooLarge() => ErrorOffsetMetadataTooLarge
    | ErrorNetworkException() => ErrorNetworkException
    | ErrorGroupLoadInProgress() => ErrorGroupLoadInProgress
    | ErrorGroupCoordinatorNotAvailable() => ErrorGroupCoordinatorNotAvailable
    | ErrorNotCoordinatorForGroup() => ErrorNotCoordinatorForGroup
    | ErrorInvalidTopicException() => ErrorInvalidTopicException
    | ErrorRecordListTooLarge() => ErrorRecordListTooLarge
    | ErrorNotEnoughReplicas() => ErrorNotEnoughReplicas
    | ErrorNotEnoughReplicasAfterAppend() => ErrorNotEnoughReplicasAfterAppend
    | ErrorInvalidRequiredAcks() => ErrorInvalidRequiredAcks
    | ErrorIllegalGeneration() => ErrorIllegalGeneration
    | ErrorInconsistentGroupProtocol() => ErrorInconsistentGroupProtocol
    | ErrorInvalidGroupId() => ErrorInvalidGroupId
    | ErrorUnknownMemberId() => ErrorUnknownMemberId
    | ErrorInvalidSessionTimeout() => ErrorInvalidSessionTimeout
    | ErrorRebalanceInProgress() => ErrorRebalanceInProgress
    | ErrorInvalidCommitOffsetSize() => ErrorInvalidCommitOffsetSize
    | ErrorTopicAuthorizationFailed() => ErrorTopicAuthorizationFailed
    | ErrorGroupAuthorizationFailed() => ErrorGroupAuthorizationFailed
    | ErrorClusterAuthorizationFailed() => ErrorClusterAuthorizationFailed
    | ErrorInvalidTimestamp() => ErrorInvalidTimestamp
    | ErrorUnsupportedSASLMechanism() => ErrorUnsupportedSASLMechanism
    | ErrorIllegalSASLState() => ErrorIllegalSASLState
    | ErrorUnsupportedVersion() => ErrorUnsupportedVersion
    | ErrorTopicAlreadyExists() => ErrorTopicAlreadyExists
    | ErrorInvalidPartitions() => ErrorInvalidPartitions
    | ErrorInvalidReplicationFactor() => ErrorInvalidReplicationFactor
    | ErrorInvalidReplicaAssignment() => ErrorInvalidReplicaAssignment
    | ErrorInvalidConfig() => ErrorInvalidConfig
    | ErrorNotController() => ErrorNotController
    | ErrorInvalidRequest() => ErrorInvalidRequest
    | ErrorUnsupportedForMessageFormat() => ErrorUnsupportedForMessageFormat
    | ErrorPolicyViolation() => ErrorPolicyViolation
    | ErrorOutOfOrderSequenceNumber() => ErrorOutOfOrderSequenceNumber
    | ErrorDuplicateSequenceNumber() => ErrorDuplicateSequenceNumber
    | ErrorInvalidProducerEpoch() => ErrorInvalidProducerEpoch
    | ErrorInvalidTxnState() => ErrorInvalidTxnState
    | ErrorInvalidProducerIdMapping() => ErrorInvalidProducerIdMapping
    | ErrorInvalidTransactionTimeout() => ErrorInvalidTransactionTimeout
    | ErrorConcurrentTransactions() => ErrorConcurrentTransactions
    | ErrorTransactionCoordinatorFenced() => ErrorTransactionCoordinatorFenced
    | ErrorTransactionalIdAuthorizationFailed() => ErrorTransactionalIdAuthorizationFailed
    | ErrorProducerIdAuthorizationFailed() => ErrorProducerIdAuthorizationFailed
    | ErrorSecurityDisabled() => ErrorSecurityDisabled
    | ErrorBrokerAuthorizationFailed() => ErrorBrokerAuthorizationFailed
    else
      _conf.logger(Warn) and _conf.logger.log(Warn, _name + "Unable to look up error code: " + error_code.string() + "! Using ErrorUnknown.")
      ErrorUnknown
    end


  // send fetched messages to KafkaConsumers
  fun ref process_fetched_data(fetch_results: Map[String, _KafkaTopicFetchResult], network_received_timestamp: U64)
  =>
    _conf.logger(Fine) and _conf.logger.log(Fine, _name + "processing and distributing fetched data")
    for (topic, topic_result) in fetch_results.pairs() do
      try
        let ts = _state.topics_state(topic)
        for (part, part_result) in topic_result.partition_responses.pairs() do
          for m in part_result.messages.values() do
            try
              if m.get_offset() >= ts.partitions_state(part).request_offset then
                let consumer = ts.message_handler(ts.consumers, m)
                match consumer
                | let c: KafkaConsumer tag =>
                    track_consumer_unacked_message(m)
                    c.receive_kafka_message(m, network_received_timestamp)
                else
                  _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Ignoring message because consumer_message_handler returned None, message: " + m.string())
                end
              else
                _conf.logger(Fine) and _conf.logger.log(Fine, _name + "Ignoring message because offset is lower than requested. Requested offset: " + _state.topics_state(topic).partitions_state(part).request_offset.string() + ", message: " + m.string())
              end
            else
              _conf.logger(Error) and _conf.logger.log(Error, _name + "error distributing fetched msg: " + m.string() + ". This should never happen.")
            end
          end
        end
      else
        _conf.logger(Error) and _conf.logger.log(Error, _name + "error looking up state for topic: " + topic + ". This should never happen.")
      end
    end

  fun ref track_consumer_unacked_message(msg: KafkaMessage val) =>
    try
       _state.consumer_unacked_offsets(msg._get_topic_partition()).set(msg.get_offset())
    else
      let po = recover Set[I64] end
      po.set(msg.get_offset())
      _state.consumer_unacked_offsets(msg._get_topic_partition()) = consume po
    end

  // update state for next offsets to request based on fetched data
  fun ref update_offsets_fetch_response(fetch_results: Map[String, _KafkaTopicFetchResult]) ?
  =>
    for topic in fetch_results.values() do
      for part in topic.partition_responses.values() do
        if part.largest_offset_seen != -1 then
          _state.topics_state(topic.topic).partitions_state(part.partition_id).request_offset = part.largest_offset_seen + 1
        end
      end
    end


  // dispatch method for decoding response from kafka after matching it up to appropriate requested correlation id
  fun ref decode_response(conn: KafkaBrokerConnection ref, data: Array[U8] val, network_received_timestamp: U64) ? =>
    (let sent_correlation_id, let request_type, let extra_request_data) = _requests_buffer.shift()

    let rb = recover ref Reader end
    rb.append(data)

    let resp_correlation_id = _KafkaResponseHeader.decode(_conf.logger, rb)

    if resp_correlation_id != sent_correlation_id then
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Correlation ID from kafka server doesn't match: sent: " + sent_correlation_id.string() + ", received: " + resp_correlation_id.string())
      error
    end

    match request_type
    | let api_versions_api: _KafkaApiVersionsApi
      =>
        update_api_versions_to_use(api_versions_api.decode_response(_conf.logger, rb))
        // Update metadata
        match _statemachine.current_state()
        | _KafkaPhaseUpdateApiVersions =>
          _statemachine.transition_to(_KafkaPhaseUpdateMetadata, conn)
        | _KafkaPhaseUpdateApiVersionsReconnect =>
          _statemachine.transition_to(_KafkaPhaseUpdateMetadataReconnect, conn)
        else
          _conf.logger(Error) and _conf.logger.log(Error, _name + "Error we're in state: " + _statemachine.current_state().string() + " in decode_response. This should never happen.")
        end
    | let metadata_api: _KafkaMetadataApi
      =>
        metadata_refresh_request_outstanding = false

        let metadata = metadata_api.decode_response(_conf.logger, rb)
        _conf.logger(Fine) and _conf.logger.log(Fine, _name + metadata.string())

        // send kafka client updated metadata
        _kafka_client._update_metadata(metadata)

        if _statemachine.current_state() is _KafkaPhaseDone then
          // cancel any pre-existing timers (in case we have multiple refresh requests issued)
          match metadata_refresh_timer
          | let t: Timer tag =>
            _timers.cancel(t)
            metadata_refresh_timer = None
          end

          // create a new timer to refresh metadata
          let timer = Timer(_KafkaRefreshMetadataTimerNotify(conn), _conf.refresh_metadata_interval)
          metadata_refresh_timer = timer
          _timers(consume timer)
        else
          if _metadata_only then
            _conf.logger(Info) and _conf.logger.log(Info, _name + "Done updating metadata. Disposing connection.")
            conn.get_handler().dispose()
          end
        end
    | let offsets_api: _KafkaOffsetsApi
      =>
        let offsets = offsets_api.decode_response(_conf.logger, rb)
        var offsets_str = recover ref String end
        for o in offsets.values() do offsets_str.append(", ").append(o.string()) end

        _conf.logger(Fine) and _conf.logger.log(Fine, _name + offsets_str.string())
        update_offsets(offsets)
        _statemachine.transition_to(_KafkaPhaseDone, conn)
    | let fetch_api: _KafkaFetchApi
      =>
        _conf.logger(Fine) and _conf.logger.log(Fine, _name + "decoding fetched data")
        (let throttle_time_ms, let fetched_data) = fetch_api.decode_response(conn, _conf.logger, rb, _state.topics_state)
        var fetched_str = recover ref String end
        for f in fetched_data.values() do fetched_str.append(", ").append(f.string()) end

        _conf.logger(Fine) and _conf.logger.log(Fine, _name + fetched_str.string())
        process_fetched_data(fetched_data, network_received_timestamp)
        update_offsets_fetch_response(fetched_data)
        let timer = Timer(_KafkaFetchRequestTimerNotify(conn), _conf.fetch_interval)
        fetch_data_timer = timer
        _timers(consume timer)
    | let produce_api: _KafkaProduceApi
      =>
        _conf.logger(Fine) and _conf.logger.log(Fine, _name + "decoding produce response")
        (let produce_response, let throttle_time) = produce_api.decode_response(_conf.logger, rb)
        (let size, let num_msgs, let msgs) = match extra_request_data
           | (let s: I32, let n: U64, let m: Map[String, Map[I32, Array[ProducerKafkaMessage val]]]) => (s, n, m)
           else
             _conf.logger(Error) and _conf.logger.log(Error, _name + "Error casting extra_request_data in produce response. This should never happen.")
             error
           end
        process_produce_response(produce_response, throttle_time, consume msgs, conn)
        var produced_str = recover ref String end
        for p in produce_response.values() do produced_str.append(", ").append(p.string()) end

        _conf.logger(Fine) and _conf.logger.log(Fine, _name + produced_str.string())
    else
      _conf.logger(Error) and _conf.logger.log(Error, _name + "Unknown kafka response type")
      error
    end

    // check if batch send timer is active; if not, processing pending buffer so we make progress if we're below max in-flight messages again
    if send_batch_timer is None then
      process_pending_buffer(conn)
    end


  // assign proper api versions to use after getting supported version info from broker
  fun ref update_api_versions_to_use(api_versions_supported: Array[(I16, I16, I16)]) =>
    _broker_apis_to_use.clear()
    for (api_key, min_version, max_version) in api_versions_supported.values() do
      var current_version = max_version
      while current_version >= min_version do
        match (api_key, current_version)
        | (_KafkaProduceV0.api_key(), _KafkaProduceV0.version()) => _broker_apis_to_use(api_key) = _KafkaProduceV0; break
        | (_KafkaProduceV1.api_key(), _KafkaProduceV1.version()) => _broker_apis_to_use(api_key) = _KafkaProduceV1; break
        | (_KafkaProduceV2.api_key(), _KafkaProduceV2.version()) => _broker_apis_to_use(api_key) = _KafkaProduceV2; break
        | (_KafkaFetchV0.api_key(), _KafkaFetchV0.version()) => _broker_apis_to_use(api_key) = _KafkaFetchV0; break
        | (_KafkaFetchV1.api_key(), _KafkaFetchV1.version()) => _broker_apis_to_use(api_key) = _KafkaFetchV1; break
        | (_KafkaFetchV2.api_key(), _KafkaFetchV2.version()) => _broker_apis_to_use(api_key) = _KafkaFetchV2; break
//        | (_KafkaFetchV3.api_key(), _KafkaFetchV3.version()) => _broker_apis_to_use(api_key) = _KafkaFetchV3; break
        | (_KafkaOffsetsV0.api_key(), _KafkaOffsetsV0.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetsV0; break
        | (_KafkaOffsetsV1.api_key(), _KafkaOffsetsV1.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetsV1; break
        | (_KafkaMetadataV0.api_key(), _KafkaMetadataV0.version()) => _broker_apis_to_use(api_key) = _KafkaMetadataV0; break
        | (_KafkaMetadataV1.api_key(), _KafkaMetadataV1.version()) => _broker_apis_to_use(api_key) = _KafkaMetadataV1; break
        | (_KafkaMetadataV2.api_key(), _KafkaMetadataV2.version()) => _broker_apis_to_use(api_key) = _KafkaMetadataV2; break
        | (_KafkaLeaderAndIsrV0.api_key(), _KafkaLeaderAndIsrV0.version()) => _broker_apis_to_use(api_key) = _KafkaLeaderAndIsrV0; break
        | (_KafkaStopReplicaV0.api_key(), _KafkaStopReplicaV0.version()) => _broker_apis_to_use(api_key) = _KafkaStopReplicaV0; break
        | (_KafkaUpdateMetadataV0.api_key(), _KafkaUpdateMetadataV0.version()) => _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV0; break
        | (_KafkaUpdateMetadataV1.api_key(), _KafkaUpdateMetadataV1.version()) => _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV1; break
        | (_KafkaUpdateMetadataV2.api_key(), _KafkaUpdateMetadataV2.version()) => _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV2; break
        | (_KafkaUpdateMetadataV3.api_key(), _KafkaUpdateMetadataV3.version()) => _broker_apis_to_use(api_key) = _KafkaUpdateMetadataV3; break
        | (_KafkaControlledShutdownV1.api_key(), _KafkaControlledShutdownV1.version()) => _broker_apis_to_use(api_key) = _KafkaControlledShutdownV1; break
        | (_KafkaOffsetCommitV0.api_key(), _KafkaOffsetCommitV0.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0; break
        | (_KafkaOffsetCommitV1.api_key(), _KafkaOffsetCommitV0.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0; break
        | (_KafkaOffsetCommitV2.api_key(), _KafkaOffsetCommitV0.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetCommitV0; break
        | (_KafkaOffsetFetchV0.api_key(), _KafkaOffsetFetchV0.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetFetchV0; break
        | (_KafkaOffsetFetchV1.api_key(), _KafkaOffsetFetchV1.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetFetchV1; break
        | (_KafkaOffsetFetchV2.api_key(), _KafkaOffsetFetchV2.version()) => _broker_apis_to_use(api_key) = _KafkaOffsetFetchV2; break
        | (_KafkaGroupCoordinatorV0.api_key(), _KafkaGroupCoordinatorV0.version()) => _broker_apis_to_use(api_key) = _KafkaGroupCoordinatorV0; break
        | (_KafkaJoinGroupV0.api_key(), _KafkaJoinGroupV0.version()) => _broker_apis_to_use(api_key) = _KafkaJoinGroupV0; break
        | (_KafkaJoinGroupV1.api_key(), _KafkaJoinGroupV1.version()) => _broker_apis_to_use(api_key) = _KafkaJoinGroupV1; break
        | (_KafkaHeartbeatV0.api_key(), _KafkaHeartbeatV0.version()) => _broker_apis_to_use(api_key) = _KafkaHeartbeatV0; break
        | (_KafkaLeaveGroupV0.api_key(), _KafkaLeaveGroupV0.version()) => _broker_apis_to_use(api_key) = _KafkaLeaveGroupV0; break
        | (_KafkaSyncGroupV0.api_key(), _KafkaSyncGroupV0.version()) => _broker_apis_to_use(api_key) = _KafkaSyncGroupV0; break
        | (_KafkaDescribeGroupsV0.api_key(), _KafkaDescribeGroupsV0.version()) => _broker_apis_to_use(api_key) = _KafkaDescribeGroupsV0; break
        | (_KafkaListGroupsV0.api_key(), _KafkaListGroupsV0.version()) => _broker_apis_to_use(api_key) = _KafkaListGroupsV0; break
        | (_KafkaSaslHandshakeV0.api_key(), _KafkaSaslHandshakeV0.version()) => _broker_apis_to_use(api_key) = _KafkaSaslHandshakeV0; break
        | (_KafkaApiVersionsV0.api_key(), _KafkaApiVersionsV0.version()) => _broker_apis_to_use(api_key) = _KafkaApiVersionsV0; break
        | (_KafkaCreateTopicsV0.api_key(), _KafkaCreateTopicsV0.version()) => _broker_apis_to_use(api_key) = _KafkaCreateTopicsV0; break
        | (_KafkaCreateTopicsV1.api_key(), _KafkaCreateTopicsV1.version()) => _broker_apis_to_use(api_key) = _KafkaCreateTopicsV1; break
        | (_KafkaDeleteTopicsV0.api_key(), _KafkaDeleteTopicsV0.version()) => _broker_apis_to_use(api_key) = _KafkaDeleteTopicsV0; break
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
    @printf[I32]("Attempting to reconnect... \n".cstring())
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
