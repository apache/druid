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
 * @param <T1> Partition Number Type
 * @param <T2> Sequence Number Type
 */
@Beta
public interface RecordSupplier<T1, T2> extends Closeable
{
  /**
   * assigns a set of partitions to this RecordSupplier
   *
   * @param partitions partitions to assign
   */
  void assign(Set<StreamPartition<T1>> partitions);

  /**
   * seek to specified sequence number
   *
   * @param partition      partition to seek
   * @param sequenceNumber sequence number to seek to
   */
  void seek(StreamPartition<T1> partition, T2 sequenceNumber);

  /**
   * seek to the sequence number immediately following the given sequenceNumber
   *
   * @param partition      partition to seek
   * @param sequenceNumber sequence number to seek
   */
  void seekAfter(StreamPartition<T1> partition, T2 sequenceNumber);

  /**
   * seek a set of partitions to the earliest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToEarliest(Set<StreamPartition<T1>> partitions);

  /**
   * seek a set of partitions to the latest/newest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToLatest(Set<StreamPartition<T1>> partitions);

  /**
   * get the current assignment
   *
   * @return set of assignments
   */
  Collection<StreamPartition<T1>> getAssignment();

  /**
   * poll the record at the current seeked to sequence in stream
   *
   * @param timeout timeout in milliseconds
   *
   * @return record
   */
  @Nullable
  Record<T1, T2> poll(long timeout);

  /**
   * get the latest sequence number in stream
   *
   * @param partition target partition
   *
   * @return latest sequence number
   *
   * @throws TimeoutException TimeoutException
   */
  T2 getLatestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  /**
   * get the earliest sequence number in stream
   *
   * @param partition target partition
   *
   * @return earliest sequence number
   *
   * @throws TimeoutException TimeoutException
   */
  T2 getEarliestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  /**
   * returns the sequence number that the given partition is currently at
   *
   * @param partition target partition
   *
   * @return sequence number
   */
  T2 position(StreamPartition<T1> partition);

  /**
   * returns the set of partitions under the given stream
   *
   * @param streamName name of stream
   *
   * @return set of partitions
   */
  Set<T1> getPartitionIds(String streamName);

  /**
   * close the RecordSupplier
   */
  @Override
  void close();
}
