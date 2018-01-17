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

// not used yet but in theory the definition of the kafka error types so they
// can be hendled correctly

use "customlogger"

trait val KafkaError
  fun apply(): KafkaApiError
  fun string(): String
  fun kafka_official(): Bool
  fun _retriable(): Bool


primitive MapKafkaError
  fun apply(logger: Logger[String], error_code: KafkaApiError): KafkaError =>
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
    | ErrorTransactionalIdAuthorizationFailed() =>
      ErrorTransactionalIdAuthorizationFailed
    | ErrorProducerIdAuthorizationFailed() => ErrorProducerIdAuthorizationFailed
    | ErrorSecurityDisabled() => ErrorSecurityDisabled
    | ErrorBrokerAuthorizationFailed() => ErrorBrokerAuthorizationFailed
    else
      logger(Warn) and logger.log(Warn,
        "Unable to look up error code: " + error_code.string() +
        "! Using ErrorUnknown.")
      ErrorUnknown
    end


// TODO: Does this class mean that the match in MapKafkaError creates a new object each time?
//       if yes, need to fix so that doesn't happen all the time
class val ClientErrorShouldNeverHappen is KafkaError
  let details: String

  new val create(message: String = "Error encountered", loc: SourceLoc = __loc) =>
    let file_name: String = loc.file()
    let file_linenum: String  = loc.line().string()
    let file_linepos: String  = loc.pos().string()
    details = message + " - " + file_name + ":" + file_linenum + ":" + file_linepos

  fun apply(): KafkaApiError => -9000
  fun string(): String => "Client(ClientErrorShouldNeverHappen/" + apply().string() +
    "): This should never happen. " + details
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

class val ClientErrorDecode is KafkaError
  let details: String

  new val create(message: String = "Decode error encountered", loc: SourceLoc = __loc) =>
    let file_name: String = loc.file()
    let file_linenum: String  = loc.line().string()
    let file_linepos: String  = loc.pos().string()
    details = message + " - " + file_name + ":" + file_linenum + ":" + file_linepos

  fun apply(): KafkaApiError => -9001
  fun string(): String => "Client(ClientErrorDecode/" + apply().string() +
    "): Error decoding data. " + details
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorPartitionFail is KafkaError
  fun apply(): KafkaApiError => -9002
  fun string(): String => "Client(ClientErrorPartitionFail/" + apply().string() +
    "): Partition requested doesn't exist in kafka."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorInvalidPartition is KafkaError
  fun apply(): KafkaApiError => -9003
  fun string(): String => "Client(ClientErrorInvalidPartition/" + apply().string() +
    "): Invalid Partition assigned by producer message handler."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorProducerTopicNotRegistered is KafkaError
  fun apply(): KafkaApiError => -9004
  fun string(): String => "Client(ClientErrorProducerTopicNotRegistered/" + apply().string() +
    "): Topic for producing not registered in config."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorMessageTooLarge is KafkaError
  fun apply(): KafkaApiError => -9005
  fun string(): String => "Client(ClientErrorMessageTooLarge/" + apply().string() + "): The request " +
    "included a message larger than the max message size the client is " +
    "configured to handle."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorNoBuffering is KafkaError
  fun apply(): KafkaApiError => -9006
  fun string(): String => "Client(ClientErrorNoBuffering/" + apply().string() +
    "): Topic/partition is throttled and KafkaClient hasn't implemented " +
    "buffering yet."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => true

primitive ClientErrorCorrelationIdMismatch is KafkaError
  fun apply(): KafkaApiError => -9007
  fun string(): String => "Client(ClientErrorCorrelationIdMismatch/" + apply().string() +
    "): Correlation ID from Broker doesn't match expected Correlation ID."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorUnknownRequest is KafkaError
  fun apply(): KafkaApiError => -9008
  fun string(): String => "Client(ClientErrorUnknownRequest/" + apply().string() +
    "): Unknown request type for response."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorUnableToLookupBroker is KafkaError
  fun apply(): KafkaApiError => -9009
  fun string(): String => "Client(ClientErrorUnableToLookupBroker/" + apply().string() +
    "): Unable to lookup broker id to send message to."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorDecodingMetadataResponse is KafkaError
  fun apply(): KafkaApiError => -9010
  fun string(): String => "Client(ClientErrorDecodingMetadataResponse/" + apply().string() +
    "): Error decoding metadata response."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorInitilizingConnection is KafkaError
  fun apply(): KafkaApiError => -9011
  fun string(): String => "Client(ClientErrorInitilizingConnection/" + apply().string() +
    "): Error initializing connection."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorInitializingFSM is KafkaError
  fun apply(): KafkaApiError => -9012
  fun string(): String => "Client(ClientErrorInitializingFSM/" + apply().string() +
    "): Error initializing state machine."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

class val ClientErrorTransitioningFSMStates is KafkaError
  let details: String

  new val create(message: String = "State transition encountered", loc: SourceLoc = __loc) =>
    let file_name: String = loc.file()
    let file_linenum: String  = loc.line().string()
    let file_linepos: String  = loc.pos().string()
    details = message + " - " + file_name + ":" + file_linenum + ":" + file_linepos

  fun apply(): KafkaApiError => -9013
  fun string(): String => "Client(ClientErrorTransitioningFSMStates/" + apply().string() +
    "): Error transitioning states in state machine." + details
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorCorruptMessage is KafkaError
  fun apply(): KafkaApiError => -9014
  fun string(): String => "Client(ClientErrorCorruptMessage/" + apply().string() + "): This message has" +
    " failed its CRC checksum."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorNoZlib is KafkaError
  fun apply(): KafkaApiError => -9015
  fun string(): String => "Client(ClientErrorNoZlib/" + apply().string() + "): GZip/Zlib compression support " +
    "not compiled."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorGZipDecompress is KafkaError
  fun apply(): KafkaApiError => -9016
  fun string(): String => "Client(ClientErrorGZipDecompress/" + apply().string() + "): Gzip decompression error!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorNoSnappy is KafkaError
  fun apply(): KafkaApiError => -9017
  fun string(): String => "Client(ClientErrorNoSnappy/" + apply().string() + "): Snappy compression support not " +
          "compiled."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorSnappyDecompress is KafkaError
  fun apply(): KafkaApiError => -9018
  fun string(): String => "Client(ClientErrorSnappyDecompress/" + apply().string() + "): Snappy decompression error!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorNoLZ4 is KafkaError
  fun apply(): KafkaApiError => -9019
  fun string(): String => "Client(ClientErrorNoLZ4/" + apply().string() + "): LZ4 compression support not " +
          "compiled."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorLZ4Decompress is KafkaError
  fun apply(): KafkaApiError => -9020
  fun string(): String => "Client(ClientErrorLZ4Decompress/" + apply().string() + "): LZ4 decompression error!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorUnknownCompression is KafkaError
  fun apply(): KafkaApiError => -9021
  fun string(): String => "Client(ClientErrorUnknownCompression/" + apply().string() + "): Unknown compression format encountered!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ClientErrorNoConnection is KafkaError
  fun apply(): KafkaApiError => -9022
  fun string(): String => "Client(ClientErrorNoConnection/" + apply().string() + "): No connection! Ran out of reconnect attempts!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

class val ClientErrorNoBrokerConnection is KafkaError
  let broker_tag: KafkaBrokerConnection tag

  new val create(broker_tag': KafkaBrokerConnection tag) =>
    broker_tag = broker_tag'

  fun apply(): KafkaApiError => -9023
  fun string(): String => "Client(ClientErrorNoBrokerConnection/" + apply().string() + "): No broker connection! Ran out of reconnect attempts!"
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ErrorNone is KafkaError
  fun apply(): KafkaApiError => 0
  fun string(): String => apply().string() + ": Success (no error)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnknown is KafkaError
  fun apply(): KafkaApiError => -1
  fun string(): String => "Broker(ErrorUnknown/" + apply().string() +
    "): The server experienced an unexpected error when processing the request."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOffsetOutOfRange is KafkaError
  fun apply(): KafkaApiError => 1
  fun string(): String => "Broker(ErrorOffsetOutOfRange/" + apply().string() + "): The requested " +
    "offset is not within the range of offsets maintained by the server."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorCorruptMessage is KafkaError
  fun apply(): KafkaApiError => 2
  fun string(): String => "Broker(ErrorCorruptMessage/" + apply().string() + "): This message has" +
    " failed its CRC checksum, exceeds the valid size, or is otherwise corrupt."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorUnknownTopicOrPartition is KafkaError
  fun apply(): KafkaApiError => 3
  fun string(): String => "Broker(ErrorUnknownTopicOrPartition/" + apply().string() +
    "): This server does not host this topic-partition."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidFetchSize is KafkaError
  fun apply(): KafkaApiError => 4
  fun string(): String => "Broker(ErrorInvalidFetchSize/" + apply().string() +
    "): The requested fetch size is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorLeaderNotAvailable is KafkaError
  fun apply(): KafkaApiError => 5
  fun string(): String => "Broker(ErrorLeaderNotAvailable/" + apply().string() + "): There is no " +
    "leader for this topic-partition as we are in the middle of a leadership " +
    "election."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotLeaderForPartition is KafkaError
  fun apply(): KafkaApiError => 6
  fun string(): String => "Broker(ErrorNotLeaderForPartition/" + apply().string() + "): This server is " +
    "not the leader for that topic-partition."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorRequestTimedOut is KafkaError
  fun apply(): KafkaApiError => 7
  fun string(): String => "Broker(ErrorRequestTimedOut/" + apply().string() +
    "): The request timed out."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorBrokerNotAvailable is KafkaError
  fun apply(): KafkaApiError => 8
  fun string(): String => "Broker(ErrorBrokerNotAvailable/" + apply().string() +
    "): The broker is not available."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorReplicaNotAvailable is KafkaError
  fun apply(): KafkaApiError => 9
  fun string(): String => "Broker(ErrorReplicaNotAvailable/" + apply().string() +
    "): The replica is not available for the requested topic-partition"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorMessageTooLarge is KafkaError
  fun apply(): KafkaApiError => 10
  fun string(): String => "Broker(ErrorMessageTooLarge/" + apply().string() + "): The request " +
    "included a message larger than the max message size the server will " +
    "accept."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorStaleControllerEpoch is KafkaError
  fun apply(): KafkaApiError => 11
  fun string(): String => "Broker(ErrorStaleControllerEpoch/" + apply().string() +
    "): The controller moved to another broker."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOffsetMetadataTooLarge is KafkaError
  fun apply(): KafkaApiError => 12
  fun string(): String => "Broker(ErrorOffsetMetadataTooLarge/" + apply().string() +
    "): The metadata field of the offset request was too large."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNetworkException is KafkaError
  fun apply(): KafkaApiError => 13
  fun string(): String => "Broker(ErrorNetworkException/" + apply().string() +
    "): The server disconnected before a response was received."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorGroupLoadInProgress is KafkaError
  fun apply(): KafkaApiError => 14
  fun string(): String => "Broker(ErrorGroupLoadInProgress/" + apply().string() + "): The group " +
    "coordinator is loading and hence can't process requests for this group."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorGroupCoordinatorNotAvailable is KafkaError
  fun apply(): KafkaApiError => 15
  fun string(): String => "Broker(ErrorGroupCoordinatorNotAvailable/" + apply().string() +
    "): The group coordinator is not available."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotCoordinatorForGroup is KafkaError
  fun apply(): KafkaApiError => 16
  fun string(): String => "Broker(ErrorNotCoordinatorForGroup/" + apply().string() +
    "): This is not the correct coordinator for this group."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidTopicException is KafkaError
  fun apply(): KafkaApiError => 17
  fun string(): String => "Broker(ErrorInvalidTopicException/" + apply().string() +
    "): The request attempted to perform an operation on an invalid topic."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorRecordListTooLarge is KafkaError
  fun apply(): KafkaApiError => 18
  fun string(): String => "Broker(ErrorRecordListTooLarge/" + apply().string() + "): The request " +
    "included message batch larger than the configured segment size on the " +
    "server."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNotEnoughReplicas is KafkaError
  fun apply(): KafkaApiError => 19
  fun string(): String => "Broker(ErrorNotEnoughReplicas/" + apply().string() + "): Messages are " +
    "rejected since there are fewer in-sync replicas than required."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotEnoughReplicasAfterAppend is KafkaError
  fun apply(): KafkaApiError => 20
  fun string(): String => "Broker(ErrorNotEnoughReplicasAfterAppend/" + apply().string() + "): Messages are " +
    "written to the log, but to fewer in-sync replicas than required."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidRequiredAcks is KafkaError
  fun apply(): KafkaApiError => 21
  fun string(): String => "Broker(ErrorInvalidRequiredAcks/" + apply().string() +
    "): Produce request specified an invalid value for required acks."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorIllegalGeneration is KafkaError
  fun apply(): KafkaApiError => 22
  fun string(): String => "Broker(ErrorIllegalGeneration/" + apply().string() +
    "): Specified group generation id is not valid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInconsistentGroupProtocol is KafkaError
  fun apply(): KafkaApiError => 23
  fun string(): String => "Broker(ErrorInconsistentGroupProtocol/" + apply().string() + "): The group " +
    "member's supported protocols are incompatible with those of existing " +
    "members."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidGroupId is KafkaError
  fun apply(): KafkaApiError => 24
  fun string(): String => "Broker(ErrorInvalidGroupId/" + apply().string() +
    "): The configured groupId is invalid"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnknownMemberId is KafkaError
  fun apply(): KafkaApiError => 25
  fun string(): String => "Broker(ErrorUnknownMemberId/" + apply().string() +
    "): The coordinator is not aware of this member."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidSessionTimeout is KafkaError
  fun apply(): KafkaApiError => 26
  fun string(): String => "Broker(ErrorInvalidSessionTimeout/" + apply().string() + "): The session " +
    "timeout is not within the range allowed by the broker (as configured by " +
    "group.min.session.timeout.ms and group.max.session.timeout.ms)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorRebalanceInProgress is KafkaError
  fun apply(): KafkaApiError => 27
  fun string(): String => "Broker(ErrorRebalanceInProgress/" + apply().string() +
    "): The group is rebalancing, so a rejoin is needed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidCommitOffsetSize is KafkaError
  fun apply(): KafkaApiError => 28
  fun string(): String => "Broker(ErrorInvalidCommitOffsetSize/" + apply().string() +
    "): The committing offset data size is not valid"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTopicAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 29
  fun string(): String => "Broker(ErrorTopicAuthorizationFailed/" + apply().string() +
    "): Not authorized to access topics: [Topic authorization failed.]"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorGroupAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 30
  fun string(): String => "Broker(ErrorGroupAuthorizationFailed/" + apply().string() +
    "): Not authorized to access group: Group authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorClusterAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 31
  fun string(): String => "Broker(ErrorClusterAuthorizationFailed/" + apply().string() +
    "): Cluster authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTimestamp is KafkaError
  fun apply(): KafkaApiError => 32
  fun string(): String => "Broker(ErrorInvalidTimestamp/" + apply().string() +
    "): The timestamp of the message is out of acceptable range."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedSASLMechanism is KafkaError
  fun apply(): KafkaApiError => 33
  fun string(): String => "Broker(ErrorUnsupportedSASLMechanism/" + apply().string() +
    "): The broker does not support the requested SASL mechanism."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorIllegalSASLState is KafkaError
  fun apply(): KafkaApiError => 34
  fun string(): String => "Broker(ErrorIllegalSASLState/" + apply().string() +
    "): Request is not valid given the current SASL state."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedVersion is KafkaError
  fun apply(): KafkaApiError => 35
  fun string(): String => "Broker(ErrorUnsupportedVersion/" + apply().string() +
    "): The version of API is not supported."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTopicAlreadyExists is KafkaError
  fun apply(): KafkaApiError => 36
  fun string(): String => "Broker(ErrorTopicAlreadyExists/" + apply().string() +
    "): Topic with this name already exists."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidPartitions is KafkaError
  fun apply(): KafkaApiError => 37
  fun string(): String => "Broker(ErrorInvalidPartitions/" + apply().string() +
    "): Number of partitions is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidReplicationFactor is KafkaError
  fun apply(): KafkaApiError => 38
  fun string(): String => "Broker(ErrorInvalidReplicationFactor/" + apply().string() +
    "): Replication-factor is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidReplicaAssignment is KafkaError
  fun apply(): KafkaApiError => 39
  fun string(): String => "Broker(ErrorInvalidReplicaAssignment/" + apply().string() +
    "): Replica assignment is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidConfig is KafkaError
  fun apply(): KafkaApiError => 40
  fun string(): String => "Broker(ErrorInvalidConfig/" + apply().string() +
    "): Configuration is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNotController is KafkaError
  fun apply(): KafkaApiError => 41
  fun string(): String => "Broker(ErrorNotController/" + apply().string() +
    "): This is not the correct controller for this cluster."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidRequest is KafkaError
  fun apply(): KafkaApiError => 42
  fun string(): String => "Broker(ErrorInvalidRequest/" + apply().string() + "): This most " +
    "likely occurs because of a request being malformed by the client " +
    "library or the message was sent to an incompatible broker. See the " +
    "broker logs for more details."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedForMessageFormat is KafkaError
  fun apply(): KafkaApiError => 43
  fun string(): String => "Broker(ErrorUnsupportedForMessageFormat/" + apply().string() +
    "): The message format version on the broker does not support the request."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorPolicyViolation is KafkaError
  fun apply(): KafkaApiError => 44
  fun string(): String => "Broker(ErrorPolicyViolation/" + apply().string() +
    "): Request parameters do not satisfy the configured policy."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOutOfOrderSequenceNumber is KafkaError
  fun apply(): KafkaApiError => 45
  fun string(): String => "Broker(ErrorOutOfOrderSequenceNumber/" + apply().string() +
    "): The broker received an out of order sequence number."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorDuplicateSequenceNumber is KafkaError
  fun apply(): KafkaApiError => 46
  fun string(): String => "Broker(ErrorDuplicateSequenceNumber/" + apply().string() +
    "): The broker received a duplicate sequence number."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidProducerEpoch is KafkaError
  fun apply(): KafkaApiError => 47
  fun string(): String => "Broker(ErrorInvalidProducerEpoch/" + apply().string() + "): Producer " +
    "attempted an operation with an old epoch. Either there is a newer " +
    "producer with the same transactionalId, or the producer's transaction " +
    "has been expired by the broker."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTxnState is KafkaError
  fun apply(): KafkaApiError => 48
  fun string(): String => "Broker(ErrorInvalidTxnState/" + apply().string() +
    "): The producer attempted a transactional operation in an invalid state."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidProducerIdMapping is KafkaError
  fun apply(): KafkaApiError => 49
  fun string(): String => "Broker(ErrorInvalidProducerIdMapping/" + apply().string() + "): The producer " +
    "attempted to use a producer id which is not currently assigned to its " +
    "transactional id."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTransactionTimeout is KafkaError
  fun apply(): KafkaApiError => 50
  fun string(): String => "Broker(ErrorInvalidTransactionTimeout/" + apply().string() + "): The transaction " +
    "timeout is larger than the maximum value allowed by the broker (as " +
    "configured by max.transaction.timeout.ms)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorConcurrentTransactions is KafkaError
  fun apply(): KafkaApiError => 51
  fun string(): String => "Broker(ErrorConcurrentTransactions/" + apply().string() + "): The producer " +
    "attempted to update a transaction while another concurrent operation on " +
    "the same transaction was ongoing."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTransactionCoordinatorFenced is KafkaError
  fun apply(): KafkaApiError => 52
  fun string(): String => "Broker(ErrorTransactionCoordinatorFenced/" + apply().string() + "): Indicates that " +
    "the transaction coordinator sending a WriteTxnMarker is no longer the " +
    "current coordinator for a given producer."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTransactionalIdAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 53
  fun string(): String => "Broker(ErrorTransactionalIdAuthorizationFailed/" + apply().string() +
    "): Transactional Id authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorProducerIdAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 54
  fun string(): String => "Broker(ErrorProducerIdAuthorizationFailed/" + apply().string() + "): Producer is not " +
    "authorized to use producer Ids, which is required to write idempotent " +
    "data."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorSecurityDisabled is KafkaError
  fun apply(): KafkaApiError => 55
  fun string(): String => "Broker(ErrorSecurityDisabled/" + apply().string() +
    "): Security features are disabled."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorBrokerAuthorizationFailed is KafkaError
  fun apply(): KafkaApiError => 56
  fun string(): String => "Broker(ErrorBrokerAuthorizationFailed/" + apply().string() +
    "): Broker authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false
