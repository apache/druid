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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

/**
 * PartitionChunk corresponding to {@link NumberedOverwriteShardSpec}
 */
public class NumberedOverwritingPartitionChunk<T> implements PartitionChunk<T>
{
  private final int chunkId;
  private final T object;

  public NumberedOverwritingPartitionChunk(int chunkId, T object)
  {
    Preconditions.checkArgument(
        chunkId >= PartitionIds.NON_ROOT_GEN_START_PARTITION_ID && chunkId < PartitionIds.NON_ROOT_GEN_END_PARTITION_ID,
        "partitionNum[%s] >= %s && partitionNum[%s] < %s",
        chunkId,
        PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
        chunkId,
        PartitionIds.NON_ROOT_GEN_END_PARTITION_ID
    );

    this.chunkId = chunkId;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> other)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStart()
  {
    return true;
  }

  @Override
  public boolean isEnd()
  {
    return true;
  }

  @Override
  public int getChunkNumber()
  {
    return chunkId;
  }

  @Override
  public int compareTo(PartitionChunk<T> o)
  {
    if (o instanceof NumberedOverwritingPartitionChunk) {
      final NumberedOverwritingPartitionChunk<T> that = (NumberedOverwritingPartitionChunk<T>) o;
      return Integer.compare(chunkId, that.chunkId);
    } else {
      throw new IAE("Cannot compare against [%s]", o.getClass().getName());
    }
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
    NumberedOverwritingPartitionChunk<?> that = (NumberedOverwritingPartitionChunk<?>) o;
    return chunkId == that.chunkId;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(chunkId);
  }

  @Override
  public String toString()
  {
    return "NumberedOverwritingPartitionChunk{" +
           "chunkId=" + chunkId +
           ", object=" + object +
           '}';
  }
}
