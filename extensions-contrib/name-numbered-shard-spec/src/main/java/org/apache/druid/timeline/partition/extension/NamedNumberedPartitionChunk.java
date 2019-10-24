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

package org.apache.druid.timeline.partition.extension;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.apache.druid.timeline.partition.PartitionChunk;

public class NamedNumberedPartitionChunk<T> implements PartitionChunk<T>
{
  private final int chunkNumber;
  private final int chunks;
  private final String chunkGroup;
  private final T object;

  public static <T> NamedNumberedPartitionChunk<T> make(
      int chunkNumber,
      int chunks,
      String chunkGroup,
      T obj
  )
  {
    return new NamedNumberedPartitionChunk<T>(chunkNumber, chunks, chunkGroup, obj);
  }

  public NamedNumberedPartitionChunk(
      int chunkNumber,
      int chunks,
      String chunkGroup,
      T object
  )
  {
    Preconditions.checkArgument(chunkNumber >= 0, "chunkNumber >= 0");
    Preconditions.checkArgument(chunks >= 0, "chunks >= 0");
    this.chunkNumber = chunkNumber;
    this.chunks = chunks;
    this.chunkGroup = chunkGroup;
    this.object = object;
  }

  public String getChunkGroup()
  {
    return chunkGroup;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> other)
  {
    if (other instanceof NamedNumberedPartitionChunk) {
      NamedNumberedPartitionChunk<T> castedOther = (NamedNumberedPartitionChunk<T>) other;
      if (!castedOther.getChunkGroup().equals(this.chunkGroup)) {
        return false;
      }
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
  public Object getChunkIdentifier()
  {
    return chunkGroup + "_" + chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> other)
  {
    if (other instanceof NamedNumberedPartitionChunk) {
      final NamedNumberedPartitionChunk castedOther = (NamedNumberedPartitionChunk) other;
      return ComparisonChain.start()
          .compare(chunkGroup, castedOther.chunkGroup)
          .compare(chunks, castedOther.chunks)
          .compare(chunkNumber, castedOther.chunkNumber)
          .result();
    } else {
      throw new IllegalArgumentException("Cannot compare against something that is not a NamedNumberedPartitionChunk.");
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

    return compareTo((NamedNumberedPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(chunkGroup, chunks, chunkNumber);
  }
}
