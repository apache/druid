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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.IAE;

import java.util.Comparator;
import java.util.Map;

@JsonTypeInfo(use = Id.NAME, property = "type", defaultImpl = SeekableStreamEndSequenceNumbers.class)
@JsonSubTypes({
    @Type(name = SeekableStreamStartSequenceNumbers.TYPE, value = SeekableStreamStartSequenceNumbers.class),
    @Type(name = SeekableStreamEndSequenceNumbers.TYPE, value = SeekableStreamEndSequenceNumbers.class)
})
public interface SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType>
{
  /**
   * Returns the stream/topic name.
   */
  String getStream();

  /**
   * Returns whether the sequence number data is for possibly multiple streams / topics.
   */
  default boolean isMultiTopicPartition()
  {
    return false;
  }

  /**
   * throws exception if this class and other class are not equal
   * @param other the other instance to compare.
   */
  default void validateSequenceNumbersBaseType(SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other)
  {
    if (this.getClass() != other.getClass()) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getName(),
          other.getClass().getName()
      );
    }
  }

  /**
   * Returns a map of partitionId -> sequenceNumber.
   */
  Map<PartitionIdType, SequenceOffsetType> getPartitionSequenceNumberMap();

  /**
   * Merges this and the given other and returns the merged result.
   *
   * @see DataSourceMetadata#plus
   */
  SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> plus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  );

  /**
   * Subtracts the given other from this and returns the result.
   *
   * @see DataSourceMetadata#minus
   */
  SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> minus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  );

  /**
   * Compare this and the other sequence offsets using comparator.
   * Returns 1, if this sequence is ahead of the other.
   * otherwise, Return 0
   */
  int compareTo(SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> seekableStreamSequenceNumbers, Comparator<SequenceOffsetType> comparator);
}
