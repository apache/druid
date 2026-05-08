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
 * Record supplier for queue-semantics streams (e.g. Kafka share groups,
 * KIP-932) where the broker owns delivery state and consumers acknowledge
 * records explicitly. Unlike {@link RecordSupplier}, callers do not assign
 * or seek partitions; they subscribe to topics and ack individual records.
 *
 * @param <PartitionIdType>    partition identifier type
 * @param <SequenceOffsetType> sequence/offset number type
 * @param <RecordType>         record entity type
 */
public interface AcknowledgingRecordSupplier<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity>
    extends Closeable
{
  void subscribe(Set<String> topics);

  void unsubscribe();

  Set<String> subscription();

  /**
   * Poll for records. Records carry acquisition locks and must be
   * acknowledged before the lock expires.
   */
  @NotNull
  List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> poll(long timeoutMs);

  /** Acknowledge a single record with the default type ({@code ACCEPT}). */
  void acknowledge(PartitionIdType partitionId, SequenceOffsetType offset);

  /** Acknowledge a single record with the given type. */
  void acknowledge(PartitionIdType partitionId, SequenceOffsetType offset, AcknowledgeType type);

  /** Acknowledge a batch of records with the given type. */
  void acknowledge(Map<PartitionIdType, Collection<SequenceOffsetType>> offsets, AcknowledgeType type);

  /**
   * Commit pending acknowledgements; returns a per-partition exception when
   * the commit failed for that partition.
   */
  Map<PartitionIdType, Optional<Exception>> commitSync();

  Set<PartitionIdType> getPartitionIds(String stream);

  /**
   * Interrupt a blocked {@link #poll(long)} so the loop can exit promptly.
   * Implementations wrapping {@link org.apache.kafka.clients.consumer.ShareConsumer}
   * should delegate to its {@code wakeup()}, after which the in-flight or
   * next {@link #poll(long)} throws an implementation-specific wake-up
   * exception (e.g. {@link org.apache.kafka.common.errors.WakeupException}).
   * Default is a no-op.
   */
  default void wakeup()
  {
  }

  /**
   * Broker-effective acquisition lock timeout, if known. For Kafka share
   * groups this surfaces {@code group.share.record.lock.duration.ms} after
   * the first {@link #poll(long)}; default is {@link Optional#empty()}.
   */
  default Optional<Integer> acquisitionLockTimeoutMs()
  {
    return Optional.empty();
  }

  @Override
  void close();
}
