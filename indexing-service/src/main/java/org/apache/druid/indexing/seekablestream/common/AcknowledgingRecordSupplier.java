/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream.common;

import org.apache.druid.data.input.impl.ByteEntity;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Record supplier for queue-semantics streams where the broker manages delivery
 * state and consumers explicitly acknowledge records after processing.
 *
 * Unlike {@link RecordSupplier}, this interface does not support assign/seek
 * operations because the broker controls partition assignment and offset tracking.
 * Instead, consumers subscribe to topics and acknowledge individual records.
 *
 * Designed for Kafka Share Groups (KIP-932) but generic enough for other
 * queue-semantics systems.
 *
 * @param <PartitionIdType>    partition identifier type
 * @param <SequenceOffsetType> sequence/offset number type
 * @param <RecordType>         record entity type
 */
public interface AcknowledgingRecordSupplier<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity>
    extends Closeable
{
  /**
   * Subscribe to the given topics. The broker manages partition assignment.
   *
   * @param topics topics to subscribe to
   */
  void subscribe(Set<String> topics);

  /**
   * Unsubscribe from all topics.
   */
  void unsubscribe();

  /**
   * Returns the current subscription.
   *
   * @return set of subscribed topic names
   */
  Set<String> subscription();

  /**
   * Poll for records from the subscribed topics. The broker delivers records
   * with acquisition locks; records must be acknowledged before the lock expires.
   *
   * @param timeoutMs poll timeout in milliseconds
   * @return list of records, never null
   */
  @NotNull
  List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> poll(long timeoutMs);

  /**
   * Acknowledge a single record with the default type (ACCEPT).
   *
   * @param partitionId partition of the record
   * @param offset      offset of the record
   */
  void acknowledge(PartitionIdType partitionId, SequenceOffsetType offset);

  /**
   * Acknowledge a single record with a specific type.
   *
   * @param partitionId partition of the record
   * @param offset      offset of the record
   * @param type        acknowledgement type (ACCEPT, RELEASE, REJECT)
   */
  void acknowledge(PartitionIdType partitionId, SequenceOffsetType offset, AcknowledgeType type);

  /**
   * Acknowledge a batch of records with a specific type.
   *
   * @param offsets map from partition to collection of offsets
   * @param type    acknowledgement type
   */
  void acknowledge(Map<PartitionIdType, Collection<SequenceOffsetType>> offsets, AcknowledgeType type);

  /**
   * Commit all pending acknowledgements synchronously.
   * Returns a map from partition to an optional exception if the commit
   * failed for that partition.
   *
   * @return commit results per partition
   */
  Map<PartitionIdType, Optional<Exception>> commitSync();

  /**
   * Returns the set of partition IDs for the given stream/topic.
   *
   * @param stream topic name
   * @return set of partition identifiers
   */
  Set<PartitionIdType> getPartitionIds(String stream);

  /**
   * Wakes up a blocked {@link #poll(long)} call so the ingestion loop can
   * exit promptly on graceful stop. Implementations that wrap a
   * {@link org.apache.kafka.clients.consumer.ShareConsumer} should call its
   * {@code wakeup()} method here. The contract for the caller is that the
   * next {@link #poll(long)} (or the in-flight one) will throw an
   * implementation-specific wake-up exception
   * (e.g. {@link org.apache.kafka.common.errors.WakeupException}).
   *
   * <p>Default implementation is a no-op for suppliers that do not support
   * wake-ups; the ingestion loop will fall back to polling the
   * {@code stopRequested} flag at the next poll boundary.</p>
   */
  default void wakeup()
  {
  }

  /**
   * Returns the broker-effective acquisition lock timeout, if known. For
   * Kafka share groups this is the {@code group.share.record.lock.duration.ms}
   * the broker is using. The value is only meaningful after the first
   * successful {@link #poll(long)} call (Kafka does not push it until the
   * client has joined the share group).
   *
   * <p>Default implementation returns {@link Optional#empty()}.</p>
   *
   * @return acquisition lock timeout in milliseconds, or empty if unknown
   */
  default Optional<Integer> acquisitionLockTimeoutMs()
  {
    return Optional.empty();
  }

  /**
   * Close this supplier and release all resources.
   */
  @Override
  void close();
}
