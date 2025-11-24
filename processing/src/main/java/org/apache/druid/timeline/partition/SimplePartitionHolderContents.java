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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Contents for {@link PartitionHolder} when segment locking was not used, and therefore no chunks have
 * {@link Overshadowable#getMinorVersion()}.
 */
public class SimplePartitionHolderContents<T extends Overshadowable<T>> implements PartitionHolderContents<T>
{
  private final TreeMap<PartitionChunk<T>, PartitionChunk<T>> holderMap = new TreeMap<>();

  /**
   * Map of {@link PartitionChunk#getChunkNumber()} to
   */
  private final Int2ObjectMap<PartitionChunk<T>> chunkForPartition = new Int2ObjectOpenHashMap<>();

  @Override
  public boolean isEmpty()
  {
    return holderMap.isEmpty();
  }

  @Override
  public boolean visibleChunksAreConsistent()
  {
    return true;
  }

  @Override
  public boolean addChunk(PartitionChunk<T> chunk)
  {
    if (chunk.getObject().getMinorVersion() != 0) {
      throw DruidException.defensive("Cannot handle chunk with minorVersion[%d]", chunk.getObject().getMinorVersion());
    }

    final PartitionChunk<T> existingChunk = chunkForPartition.put(chunk.getChunkNumber(), chunk);
    if (existingChunk != null && !existingChunk.equals(chunk)) {
      throw DruidException.defensive(
          "existingChunk[%s] is different from newChunk[%s] for partitionId[%d]",
          existingChunk,
          chunk,
          chunk.getChunkNumber()
      );
    } else {
      holderMap.put(chunk, chunk);
      return existingChunk == null;
    }
  }

  @Override
  @Nullable
  public PartitionChunk<T> removeChunk(PartitionChunk<T> chunk)
  {
    final PartitionChunk<T> retVal = holderMap.remove(chunk);
    if (retVal != null) {
      chunkForPartition.remove(chunk.getChunkNumber(), retVal);
    }
    return retVal;
  }

  @Override
  public PartitionChunk<T> getChunk(int partitionNum)
  {
    return chunkForPartition.get(partitionNum);
  }

  @Override
  public Iterator<PartitionChunk<T>> visibleChunksIterator()
  {
    return holderMap.keySet().iterator();
  }

  @Override
  public List<PartitionChunk<T>> getOvershadowedChunks()
  {
    return List.of();
  }

  @Override
  public PartitionHolderContents<T> copyVisible()
  {
    final SimplePartitionHolderContents<T> retVal = new SimplePartitionHolderContents<>();
    for (PartitionChunk<T> chunk : holderMap.keySet()) {
      retVal.addChunk(chunk);
    }
    return retVal;
  }

  @Override
  public PartitionHolderContents<T> deepCopy()
  {
    // All chunks are always visible, so copyVisible() and deepCopy() are the same thing.
    return copyVisible();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimplePartitionHolderContents<?> that = (SimplePartitionHolderContents<?>) o;
    return Objects.equals(holderMap, that.holderMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(holderMap);
  }

  @Override
  public String toString()
  {
    return "SimplePartitionHolderContents{" +
           "holderMap=" + holderMap +
           '}';
  }
}
