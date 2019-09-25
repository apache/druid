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

import java.util.Objects;

/**
 * Represents a Kafka/Kinesis stream sequence number. Mainly used to do
 * comparison and indicate whether the sequence number is exclusive.
 * <p>
 * isExclusive is used to indicate if this sequence number is the starting
 * sequence of some Kinesis partition and should be discarded because some
 * previous task has already read this sequence number
 *
 * @param <SequenceOffsetType> type of sequence number
 */
public abstract class OrderedSequenceNumber<SequenceOffsetType>
    implements Comparable<OrderedSequenceNumber<SequenceOffsetType>>
{
  private final SequenceOffsetType sequenceNumber;
  private final boolean isExclusive;

  protected OrderedSequenceNumber(SequenceOffsetType sequenceNumber, boolean isExclusive)
  {
    this.sequenceNumber = sequenceNumber;
    this.isExclusive = isExclusive;
  }

  public SequenceOffsetType get()
  {
    return sequenceNumber;
  }

  public boolean isExclusive()
  {
    return isExclusive;
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
    OrderedSequenceNumber<?> that = (OrderedSequenceNumber<?>) o;
    return isExclusive == that.isExclusive &&
           Objects.equals(sequenceNumber, that.sequenceNumber);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sequenceNumber, isExclusive);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "sequenceNumber=" + sequenceNumber +
           ", isExclusive=" + isExclusive +
           '}';
  }
}
