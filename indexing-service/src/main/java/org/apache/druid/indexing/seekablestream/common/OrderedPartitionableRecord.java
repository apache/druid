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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.ByteEntity;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a generic record with a PartitionIdType (partition id) and SequenceOffsetType (sequence number) and data
 * from a Kafka/Kinesis stream
 *
 * @param <PartitionIdType>    partition id
 * @param <SequenceOffsetType> sequence number
 */
public class OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity>
{
  private final String stream;
  private final PartitionIdType partitionId;
  private final SequenceOffsetType sequenceNumber;
  private final List<RecordType> data;

  public OrderedPartitionableRecord(
      String stream,
      PartitionIdType partitionId,
      SequenceOffsetType sequenceNumber,
      List<RecordType> data
  )
  {
    Preconditions.checkNotNull(stream, "stream");
    Preconditions.checkNotNull(partitionId, "partitionId");
    Preconditions.checkNotNull(sequenceNumber, "sequenceNumber");
    this.stream = stream;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.data = data == null ? ImmutableList.of() : data;
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

  /**
   * Returns a list of ByteEntities representing this record
   *
   * This method will always return the same list of ByteEntity instances.
   * Since each ByteEntity can only be read once (unless care is taking to reset its buffer positions), it
   * is not advised to call this method multiple times, or iterating over the list more than once.
   *
   * @return the list of ByteEntity object for this record
   */
  @NotNull
  public List<? extends ByteEntity> getData()
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
    OrderedPartitionableRecord<?, ?, ? extends ByteEntity> that = (OrderedPartitionableRecord<?, ?, ? extends ByteEntity>) o;

    if (data.size() != that.data.size()) {
      return false;
    }

    for (int i = 0; i < data.size(); i++) {
      if (!data.get(i).getBuffer().equals(that.data.get(i).getBuffer())) {
        return false;
      }
    }

    return Objects.equals(stream, that.stream) &&
           Objects.equals(partitionId, that.partitionId) &&
           Objects.equals(sequenceNumber, that.sequenceNumber);
  }

  @Override
  public int hashCode()
  {
    final int hashOfData = data.stream().map(e -> e.getBuffer().hashCode()).collect(Collectors.toList()).hashCode();
    return Objects.hash(stream, partitionId, sequenceNumber, hashOfData);
  }
}
