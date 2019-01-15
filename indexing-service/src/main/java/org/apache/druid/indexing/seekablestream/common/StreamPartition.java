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

/**
 * Reprents a Kinesis/Kafka partition with stream name and partitionId,
 * mostly used by {@link RecordSupplier} and
 * {@link org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor}
 *
 * @param <PartitionIdType> partition id type
 */
public class StreamPartition<PartitionIdType>
{
  private final String stream;
  private final PartitionIdType partitionId;

  public StreamPartition(String stream, PartitionIdType partitionId)
  {
    this.stream = stream;
    this.partitionId = partitionId;
  }

  public static <PartitionType> StreamPartition<PartitionType> of(String stream, PartitionType partitionId)
  {
    return new StreamPartition<>(stream, partitionId);
  }

  public String getStream()
  {
    return stream;
  }

  public PartitionIdType getPartitionId()
  {
    return partitionId;
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

    StreamPartition that = (StreamPartition) o;

    if (stream != null ? !stream.equals(that.stream) : that.stream != null) {
      return false;
    }
    return !(partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null);
  }

  @Override
  public int hashCode()
  {
    int result = stream != null ? stream.hashCode() : 0;
    result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "StreamPartition{" +
           "stream='" + stream + '\'' +
           ", partitionId='" + partitionId + '\'' +
           '}';
  }
}

