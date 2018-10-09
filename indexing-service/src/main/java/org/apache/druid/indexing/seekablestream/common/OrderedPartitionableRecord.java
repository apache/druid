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
 * Represents a generic record with a partitionType (partition id) and sequenceType (sequence number) and data
 * from a Kafka/Kinesis stream
 *
 * @param <partitionType> partition id
 * @param <sequenceType> sequence number
 */
public class OrderedPartitionableRecord<partitionType, sequenceType>
{
  public static final String END_OF_SHARD_MARKER = "EOS";

  private final String stream;
  private final partitionType partitionId;
  private final sequenceType sequenceNumber;
  private final List<byte[]> data;

  public OrderedPartitionableRecord(String stream, partitionType partitionId, sequenceType sequenceNumber, List<byte[]> data)
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

  public partitionType getPartitionId()
  {
    return partitionId;
  }

  public sequenceType getSequenceNumber()
  {
    return sequenceNumber;
  }

  public List<byte[]> getData()
  {
    return data;
  }

  public StreamPartition getStreamPartition()
  {
    return StreamPartition.of(stream, partitionId);
  }
}
