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

import com.google.common.annotations.Beta;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * The RecordSupplier interface is a wrapper for the incoming seekable data stream
 * (i.e. Kafka consumer, Kinesis streams)
 *
 * @param <partitionType> Partition Number Type
 * @param <sequenceType> Sequence Number Type
 */
@Beta
public interface RecordSupplier<partitionType, sequenceType> extends Closeable
{
  void assign(Set<StreamPartition<partitionType>> partitions);

  /**
   * seek to specified sequence number within a specific partition
   *
   * @param partition      partition to seek
   * @param sequenceNumber sequence number to seek to
   */
  void seek(StreamPartition<partitionType> partition, sequenceType sequenceNumber);

  /**
   * seek to the sequence number immediately following the given sequenceNumber
   *
   * @param partition      partition to seek
   * @param sequenceNumber sequence number to seek
   */
  void seekAfter(StreamPartition<partitionType> partition, sequenceType sequenceNumber);

  /**
   * seek a set of partitions to the earliest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToEarliest(Set<StreamPartition<partitionType>> partitions);

  /**
   * seek a set of partitions to the latest/newest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToLatest(Set<StreamPartition<partitionType>> partitions);

  /**
   * get the current assignment
   *
   * @return set of assignments
   */
  Collection<StreamPartition<partitionType>> getAssignment();

  /**
   * poll the record at the current seeked to sequence in stream
   *
   * @param timeout timeout in milliseconds
   *
   * @return record
   */
  @Nullable
  OrderedPartitionableRecord<partitionType, sequenceType> poll(long timeout);

  /**
   * get the latest sequence number in stream
   *
   * @param partition target partition
   *
   * @return latest sequence number
   *
   * @throws TimeoutException TimeoutException
   */
  sequenceType getLatestSequenceNumber(StreamPartition<partitionType> partition) throws TimeoutException;

  /**
   * get the earliest sequence number in stream
   *
   * @param partition target partition
   *
   * @return earliest sequence number
   *
   * @throws TimeoutException TimeoutException
   */
  sequenceType getEarliestSequenceNumber(StreamPartition<partitionType> partition) throws TimeoutException;

  /**
   * returns the sequence number that the given partition is currently at
   *
   * @param partition target partition
   *
   * @return sequence number
   */
  sequenceType position(StreamPartition<partitionType> partition);

  /**
   * returns the set of partitions under the given stream
   *
   * @param stream name of stream
   *
   * @return set of partitions
   */
  Set<partitionType> getPartitionIds(String stream);

  /**
   * close the RecordSupplier
   */
  @Override
  void close();
}
