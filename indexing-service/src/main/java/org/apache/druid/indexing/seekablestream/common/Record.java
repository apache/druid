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

/**
 * Represents a generic record with a T1 (partition id) and T2 (sequence number) and data
 * from a Kafka/Kinesis stream
 *
 * @param <T1> partition id
 * @param <T2> sequence number
 */
public class Record<T1, T2>
{
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String streamName;
  private final T1 partitionId;
  private final T2 sequenceNumber;
  private final List<byte[]> data;

  public Record(String streamName, T1 partitionId, T2 sequenceNumber, List<byte[]> data)
  {
    this.streamName = streamName;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.data = data;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public T1 getPartitionId()
  {
    return partitionId;
  }

  public T2 getSequenceNumber()
  {
    return sequenceNumber;
  }

  public List<byte[]> getData()
  {
    return data;
  }

  public StreamPartition getStreamPartition()
  {
    return StreamPartition.of(streamName, partitionId);
  }
}
