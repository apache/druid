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
 * Wrapper class for Kafka and Kinesis stream sequence numbers. Mainly used to do
 * comparison and indicate whether the sequence number should be excluded
 *
 * @param <T> type of sequence number
 */
public abstract class SequenceNumber<T> implements Comparable<SequenceNumber<T>>
{
  private final T sequenceNumber;
  private final boolean isExclusive;
  private final boolean useExclusive;

  protected SequenceNumber(T sequenceNumber, boolean useExclusive, boolean isExclusive)
  {
    this.sequenceNumber = sequenceNumber;
    this.useExclusive = useExclusive;
    this.isExclusive = isExclusive;
  }

  public T get()
  {
    return sequenceNumber;
  }

  public boolean isExclusive()
  {
    return useExclusive && isExclusive;
  }


  @Override
  public int hashCode()
  {
    return Objects.hash(sequenceNumber, useExclusive, isExclusive);
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
    SequenceNumber<?> that = (SequenceNumber<?>) o;
    return isExclusive == that.isExclusive &&
           useExclusive == that.useExclusive &&
           Objects.equals(sequenceNumber, that.sequenceNumber);
  }
}
