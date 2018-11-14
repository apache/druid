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
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * The RecordSupplier interface is a wrapper for the incoming seekable data stream
 * (i.e. Kafka consumer, Kinesis streams)
 *
 * @param <PartitionType> Partition Number Type
 * @param <SequenceType>  Sequence Number Type
 */
@Beta
public interface RecordSupplier<PartitionType, SequenceType> extends Closeable
{
  /**
   * assigns the given parittions to this RecordSupplier
   * and seek to the earliest sequence number. Previously
   * assigned partitions will be replaced.
   *
   * @param partitions parititions to assign
   */
  void assign(Set<StreamPartition<PartitionType>> partitions);

  /**
   * seek to specified sequence number within a specific partition
   *
   * @param partition      partition to seek
   * @param sequenceNumber sequence number to seek to
   */
  void seek(StreamPartition<PartitionType> partition, SequenceType sequenceNumber);

  /**
   * seek a set of partitions to the earliest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToEarliest(Set<StreamPartition<PartitionType>> partitions);

  /**
   * seek a set of partitions to the latest/newest record position available in the stream
   *
   * @param partitions partitions to seek
   */
  void seekToLatest(Set<StreamPartition<PartitionType>> partitions);

  /**
   * get the current assignment
   *
   * @return set of assignments
   */
  Collection<StreamPartition<PartitionType>> getAssignment();

  /**
   * poll the record at the current seeked to sequence in stream
   *
   * @param timeout timeout in milliseconds
   *
   * @return record
   */
  @NotNull
  List<OrderedPartitionableRecord<PartitionType, SequenceType>> poll(long timeout);

  /**
   * get the latest sequence number in stream
   *
   * @param partition target partition
   *
   * @return latest sequence number
   */
  @Nullable
  SequenceType getLatestSequenceNumber(StreamPartition<PartitionType> partition);

  /**
   * get the earliest sequence number in stream
   *
   * @param partition target partition
   *
   * @return earliest sequence number
   */
  @Nullable
  SequenceType getEarliestSequenceNumber(StreamPartition<PartitionType> partition);


  /**
   * returns the sequence number of the next record
   *
   * @param partition target partition
   *
   * @return sequence number
   */
  SequenceType getPosition(StreamPartition<PartitionType> partition);

  /**
   * returns the set of partitions under the given stream
   *
   * @param stream name of stream
   *
   * @return set of partitions
   */
  Set<PartitionType> getPartitionIds(String stream);

  /**
   * close the RecordSupplier
   */
  @Override
  void close();
}
