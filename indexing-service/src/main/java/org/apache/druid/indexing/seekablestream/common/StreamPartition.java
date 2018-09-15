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

public class StreamPartition<T1>
{
  private final String streamName;
  private final T1 partitionId;

  public StreamPartition(String streamName, T1 partitionId)
  {
    this.streamName = streamName;
    this.partitionId = partitionId;
  }

  public static <T1> StreamPartition<T1> of(String streamName, T1 partitionId)
  {
    return new StreamPartition<>(streamName, partitionId);
  }

  public String getStreamName()
  {
    return streamName;
  }

  public T1 getPartitionId()
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

    if (streamName != null ? !streamName.equals(that.streamName) : that.streamName != null) {
      return false;
    }
    return !(partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null);
  }

  @Override
  public int hashCode()
  {
    int result = streamName != null ? streamName.hashCode() : 0;
    result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "StreamPartition{" +
           "streamName='" + streamName + '\'' +
           ", partitionId='" + partitionId + '\'' +
           '}';
  }
}

