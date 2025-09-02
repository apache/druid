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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents a single partition assignment with its start and end offsets.
 */
public class PartitionAssignment<PartitionIdType, SequenceOffsetType>
{
  private final PartitionIdType partitionId;
  private final SequenceOffsetType startOffset;
  private final SequenceOffsetType endOffset;

  @JsonCreator
  public PartitionAssignment(
      @JsonProperty("partitionId") PartitionIdType partitionId,
      @JsonProperty("startOffset") SequenceOffsetType startOffset,
      @JsonProperty("endOffset") SequenceOffsetType endOffset
  )
  {
    this.partitionId = partitionId;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @JsonProperty
  public PartitionIdType getPartitionId()
  {
    return partitionId;
  }

  @JsonProperty
  public SequenceOffsetType getStartOffset()
  {
    return startOffset;
  }

  @JsonProperty
  public SequenceOffsetType getEndOffset()
  {
    return endOffset;
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
    PartitionAssignment<?, ?> that = (PartitionAssignment<?, ?>) o;
    return Objects.equals(partitionId, that.partitionId) &&
           Objects.equals(startOffset, that.startOffset) &&
           Objects.equals(endOffset, that.endOffset);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionId, startOffset, endOffset);
  }

  @Override
  public String toString()
  {
    return "PartitionAssignment{" +
           "partitionId=" + partitionId +
           ", startOffset=" + startOffset +
           ", endOffset=" + endOffset +
           '}';
  }
}