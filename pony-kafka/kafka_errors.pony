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

trait val KafkaError
  fun apply(): I16
  fun string(): String
  fun kafka_official(): Bool
  fun _retriable(): Bool

primitive KafkaClientShouldNeverHappen is KafkaError
  fun apply(): I16 => -9000
  fun string(): String => "Client(" + apply().string() +
    "): This should never happen."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive KafkaClientInvalidPartition is KafkaError
  fun apply(): I16 => -9001
  fun string(): String => "Client(" + apply().string() +
    "): Invalid Partition assigned by producer message handler."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive KafkaClientProducerTopicNotRegistered is KafkaError
  fun apply(): I16 => -9002
  fun string(): String => "Client(" + apply().string() +
    "): Topic for producing not registered in config."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ErrorClientMessageTooLarge is KafkaError
  fun apply(): I16 => -9003
  fun string(): String => "Client(" + apply().string() + "): The request " +
    "included a message larger than the max message size the client is " +
    "configured to handle."
  fun kafka_official(): Bool => false
  fun _retriable(): Bool => false

primitive ErrorNone is KafkaError
  fun apply(): I16 => 0
  fun string(): String => apply().string() + ": Success (no error)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnknown is KafkaError
  fun apply(): I16 => -1
  fun string(): String => "Broker(" + apply().string() +
    "): The server experienced an unexpected error when processing the request."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOffsetOutOfRange is KafkaError
  fun apply(): I16 => 1
  fun string(): String => "Broker(" + apply().string() + "): The requested " +
    "offset is not within the range of offsets maintained by the server."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorCorruptMessage is KafkaError
  fun apply(): I16 => 2
  fun string(): String => "Broker(" + apply().string() + "): This message has" +
    " failed its CRC checksum, exceeds the valid size, or is otherwise corrupt."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorUnknownTopicOrPartition is KafkaError
  fun apply(): I16 => 3
  fun string(): String => "Broker(" + apply().string() +
    "): This server does not host this topic-partition."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidFetchSize is KafkaError
  fun apply(): I16 => 4
  fun string(): String => "Broker(" + apply().string() +
    "): The requested fetch size is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorLeaderNotAvailable is KafkaError
  fun apply(): I16 => 5
  fun string(): String => "Broker(" + apply().string() + "): There is no " +
    "leader for this topic-partition as we are in the middle of a leadership " +
    "election."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotLeaderForPartition is KafkaError
  fun apply(): I16 => 6
  fun string(): String => "Broker(" + apply().string() + "): This server is " +
    "not the leader for that topic-partition."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorRequestTimedOut is KafkaError
  fun apply(): I16 => 7
  fun string(): String => "Broker(" + apply().string() +
    "): The request timed out."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorBrokerNotAvailable is KafkaError
  fun apply(): I16 => 8
  fun string(): String => "Broker(" + apply().string() +
    "): The broker is not available."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorReplicaNotAvailable is KafkaError
  fun apply(): I16 => 9
  fun string(): String => "Broker(" + apply().string() +
    "): The replica is not available for the requested topic-partition"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorMessageTooLarge is KafkaError
  fun apply(): I16 => 10
  fun string(): String => "Broker(" + apply().string() + "): The request " +
    "included a message larger than the max message size the server will " +
    "accept."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorStaleControllerEpoch is KafkaError
  fun apply(): I16 => 11
  fun string(): String => "Broker(" + apply().string() +
    "): The controller moved to another broker."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOffsetMetadataTooLarge is KafkaError
  fun apply(): I16 => 12
  fun string(): String => "Broker(" + apply().string() +
    "): The metadata field of the offset request was too large."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNetworkException is KafkaError
  fun apply(): I16 => 13
  fun string(): String => "Broker(" + apply().string() +
    "): The server disconnected before a response was received."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorGroupLoadInProgress is KafkaError
  fun apply(): I16 => 14
  fun string(): String => "Broker(" + apply().string() + "): The group " +
    "coordinator is loading and hence can't process requests for this group."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorGroupCoordinatorNotAvailable is KafkaError
  fun apply(): I16 => 15
  fun string(): String => "Broker(" + apply().string() +
    "): The group coordinator is not available."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotCoordinatorForGroup is KafkaError
  fun apply(): I16 => 16
  fun string(): String => "Broker(" + apply().string() +
    "): This is not the correct coordinator for this group."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidTopicException is KafkaError
  fun apply(): I16 => 17
  fun string(): String => "Broker(" + apply().string() +
    "): The request attempted to perform an operation on an invalid topic."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorRecordListTooLarge is KafkaError
  fun apply(): I16 => 18
  fun string(): String => "Broker(" + apply().string() + "): The request " +
    "included message batch larger than the configured segment size on the " +
    "server."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNotEnoughReplicas is KafkaError
  fun apply(): I16 => 19
  fun string(): String => "Broker(" + apply().string() + "): Messages are " +
    "rejected since there are fewer in-sync replicas than required."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorNotEnoughReplicasAfterAppend is KafkaError
  fun apply(): I16 => 20
  fun string(): String => "Broker(" + apply().string() + "): Messages are " +
    "written to the log, but to fewer in-sync replicas than required."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidRequiredAcks is KafkaError
  fun apply(): I16 => 21
  fun string(): String => "Broker(" + apply().string() +
    "): Produce request specified an invalid value for required acks."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorIllegalGeneration is KafkaError
  fun apply(): I16 => 22
  fun string(): String => "Broker(" + apply().string() +
    "): Specified group generation id is not valid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInconsistentGroupProtocol is KafkaError
  fun apply(): I16 => 23
  fun string(): String => "Broker(" + apply().string() + "): The group " +
    "member's supported protocols are incompatible with those of existing " +
    "members."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidGroupId is KafkaError
  fun apply(): I16 => 24
  fun string(): String => "Broker(" + apply().string() +
    "): The configured groupId is invalid"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnknownMemberId is KafkaError
  fun apply(): I16 => 25
  fun string(): String => "Broker(" + apply().string() +
    "): The coordinator is not aware of this member."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidSessionTimeout is KafkaError
  fun apply(): I16 => 26
  fun string(): String => "Broker(" + apply().string() + "): The session " +
    "timeout is not within the range allowed by the broker (as configured by " +
    "group.min.session.timeout.ms and group.max.session.timeout.ms)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorRebalanceInProgress is KafkaError
  fun apply(): I16 => 27
  fun string(): String => "Broker(" + apply().string() +
    "): The group is rebalancing, so a rejoin is needed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidCommitOffsetSize is KafkaError
  fun apply(): I16 => 28
  fun string(): String => "Broker(" + apply().string() +
    "): The committing offset data size is not valid"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTopicAuthorizationFailed is KafkaError
  fun apply(): I16 => 29
  fun string(): String => "Broker(" + apply().string() +
    "): Not authorized to access topics: [Topic authorization failed.]"
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorGroupAuthorizationFailed is KafkaError
  fun apply(): I16 => 30
  fun string(): String => "Broker(" + apply().string() +
    "): Not authorized to access group: Group authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorClusterAuthorizationFailed is KafkaError
  fun apply(): I16 => 31
  fun string(): String => "Broker(" + apply().string() +
    "): Cluster authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTimestamp is KafkaError
  fun apply(): I16 => 32
  fun string(): String => "Broker(" + apply().string() +
    "): The timestamp of the message is out of acceptable range."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedSASLMechanism is KafkaError
  fun apply(): I16 => 33
  fun string(): String => "Broker(" + apply().string() +
    "): The broker does not support the requested SASL mechanism."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorIllegalSASLState is KafkaError
  fun apply(): I16 => 34
  fun string(): String => "Broker(" + apply().string() +
    "): Request is not valid given the current SASL state."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedVersion is KafkaError
  fun apply(): I16 => 35
  fun string(): String => "Broker(" + apply().string() +
    "): The version of API is not supported."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTopicAlreadyExists is KafkaError
  fun apply(): I16 => 36
  fun string(): String => "Broker(" + apply().string() +
    "): Topic with this name already exists."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidPartitions is KafkaError
  fun apply(): I16 => 37
  fun string(): String => "Broker(" + apply().string() +
    "): Number of partitions is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidReplicationFactor is KafkaError
  fun apply(): I16 => 38
  fun string(): String => "Broker(" + apply().string() +
    "): Replication-factor is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidReplicaAssignment is KafkaError
  fun apply(): I16 => 39
  fun string(): String => "Broker(" + apply().string() +
    "): Replica assignment is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidConfig is KafkaError
  fun apply(): I16 => 40
  fun string(): String => "Broker(" + apply().string() +
    "): Configuration is invalid."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorNotController is KafkaError
  fun apply(): I16 => 41
  fun string(): String => "Broker(" + apply().string() +
    "): This is not the correct controller for this cluster."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidRequest is KafkaError
  fun apply(): I16 => 42
  fun string(): String => "Broker(" + apply().string() + "): This most " +
    "likely occurs because of a request being malformed by the client " +
    "library or the message was sent to an incompatible broker. See the " +
    "broker logs for more details."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorUnsupportedForMessageFormat is KafkaError
  fun apply(): I16 => 43
  fun string(): String => "Broker(" + apply().string() +
    "): The message format version on the broker does not support the request."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorPolicyViolation is KafkaError
  fun apply(): I16 => 44
  fun string(): String => "Broker(" + apply().string() +
    "): Request parameters do not satisfy the configured policy."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorOutOfOrderSequenceNumber is KafkaError
  fun apply(): I16 => 45
  fun string(): String => "Broker(" + apply().string() +
    "): The broker received an out of order sequence number."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorDuplicateSequenceNumber is KafkaError
  fun apply(): I16 => 46
  fun string(): String => "Broker(" + apply().string() +
    "): The broker received a duplicate sequence number."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => true

primitive ErrorInvalidProducerEpoch is KafkaError
  fun apply(): I16 => 47
  fun string(): String => "Broker(" + apply().string() + "): Producer " +
    "attempted an operation with an old epoch. Either there is a newer " +
    "producer with the same transactionalId, or the producer's transaction " +
    "has been expired by the broker."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTxnState is KafkaError
  fun apply(): I16 => 48
  fun string(): String => "Broker(" + apply().string() +
    "): The producer attempted a transactional operation in an invalid state."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidProducerIdMapping is KafkaError
  fun apply(): I16 => 49
  fun string(): String => "Broker(" + apply().string() + "): The producer " +
    "attempted to use a producer id which is not currently assigned to its " +
    "transactional id."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorInvalidTransactionTimeout is KafkaError
  fun apply(): I16 => 50
  fun string(): String => "Broker(" + apply().string() + "): The transaction " +
    "timeout is larger than the maximum value allowed by the broker (as " +
    "configured by max.transaction.timeout.ms)."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorConcurrentTransactions is KafkaError
  fun apply(): I16 => 51
  fun string(): String => "Broker(" + apply().string() + "): The producer " +
    "attempted to update a transaction while another concurrent operation on " +
    "the same transaction was ongoing."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTransactionCoordinatorFenced is KafkaError
  fun apply(): I16 => 52
  fun string(): String => "Broker(" + apply().string() + "): Indicates that " +
    "the transaction coordinator sending a WriteTxnMarker is no longer the " +
    "current coordinator for a given producer."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorTransactionalIdAuthorizationFailed is KafkaError
  fun apply(): I16 => 53
  fun string(): String => "Broker(" + apply().string() +
    "): Transactional Id authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorProducerIdAuthorizationFailed is KafkaError
  fun apply(): I16 => 54
  fun string(): String => "Broker(" + apply().string() + "): Producer is not " +
    "authorized to use producer Ids, which is required to write idempotent " +
    "data."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorSecurityDisabled is KafkaError
  fun apply(): I16 => 55
  fun string(): String => "Broker(" + apply().string() +
    "): Security features are disabled."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false

primitive ErrorBrokerAuthorizationFailed is KafkaError
  fun apply(): I16 => 56
  fun string(): String => "Broker(" + apply().string() +
    "): Broker authorization failed."
  fun kafka_official(): Bool => true
  fun _retriable(): Bool => false
