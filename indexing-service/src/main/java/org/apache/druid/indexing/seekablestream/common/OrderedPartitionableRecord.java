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

import java.util.List;
import java.util.Objects;

/**
 * Represents a generic record with a PartitionIdType (partition id) and SequenceOffsetType (sequence number) and data
 * from a Kafka/Kinesis stream
 *
 * @param <PartitionIdType>    partition id
 * @param <SequenceOffsetType> sequence number
 */
public class OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType>
{
  /**
   * In Kinesis, when a shard is closed due to shard splitting, a null ShardIterator is returned.
   * The EOS marker is placed at the end of the Kinesis Record Supplier buffer, such that when
   * an indexing task pulls the record 'EOS', it knows the shard has been closed and should stop
   * reading and start publishing
   */
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String stream;
  private final PartitionIdType partitionId;
  private final SequenceOffsetType sequenceNumber;
  private final List<byte[]> data;

  public OrderedPartitionableRecord(
      String stream,
      PartitionIdType partitionId,
      SequenceOffsetType sequenceNumber,
      List<byte[]> data
  )
  {
    this.stream = stream;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.data = data;
  }

  public String getStream()
  {
    return stream;
  }

  public PartitionIdType getPartitionId()
  {
    return partitionId;
  }

  public SequenceOffsetType getSequenceNumber()
  {
    return sequenceNumber;
  }

  public List<byte[]> getData()
  {
    return data;
  }

  public StreamPartition<PartitionIdType> getStreamPartition()
  {
    return StreamPartition.of(stream, partitionId);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrderedPartitionableRecord<?, ?> that = (OrderedPartitionableRecord<?, ?>) o;
    return Objects.equals(stream, that.stream) &&
           Objects.equals(partitionId, that.partitionId) &&
           Objects.equals(sequenceNumber, that.sequenceNumber);

  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stream, partitionId, sequenceNumber);
  }
}
