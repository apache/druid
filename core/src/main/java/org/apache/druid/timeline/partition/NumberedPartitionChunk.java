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

package org.apache.druid.timeline.partition;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

public class NumberedPartitionChunk<T> implements PartitionChunk<T>
{
  private final int chunkNumber;
  private final int chunks;
  private final T object;

  public static <T> NumberedPartitionChunk<T> make(
      int chunkNumber,
      int chunks,
      T obj
  )
  {
    return new NumberedPartitionChunk<T>(chunkNumber, chunks, obj);
  }

  public NumberedPartitionChunk(
      int chunkNumber,
      int chunks,
      T object
  )
  {
    Preconditions.checkArgument(chunkNumber >= 0, "chunkNumber >= 0");
    Preconditions.checkArgument(chunks >= 0, "chunks >= 0");
    this.chunkNumber = chunkNumber;
    this.chunks = chunks;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(final PartitionChunk<T> other)
  {
    if (other instanceof NumberedPartitionChunk) {
      NumberedPartitionChunk<T> castedOther = (NumberedPartitionChunk<T>) other;
      if (castedOther.getChunkNumber() < castedOther.chunks) {
        return other.getChunkNumber() == chunkNumber + 1;
      } else {
        return other.getChunkNumber() > chunkNumber;
      }
    } else {
      return false;
    }
  }

  @Override
  public boolean isStart()
  {
    return chunks > 0 ? chunkNumber == 0 : true;
  }

  @Override
  public boolean isEnd()
  {
    return chunks > 0 ? chunkNumber == chunks - 1 : true;
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> other)
  {
    if (other instanceof NumberedPartitionChunk) {
      final NumberedPartitionChunk castedOther = (NumberedPartitionChunk) other;
      return ComparisonChain.start()
                            .compare(chunks, castedOther.chunks)
                            .compare(chunkNumber, castedOther.chunkNumber)
                            .result();
    } else {
      throw new IllegalArgumentException("Cannot compare against something that is not a NumberedPartitionChunk.");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return compareTo((NumberedPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(chunks, chunkNumber);
  }

  @Override
  public String toString()
  {
    return "NumberedPartitionChunk{" +
           "chunkNumber=" + chunkNumber +
           ", chunks=" + chunks +
           ", object=" + object +
           '}';
  }
}
