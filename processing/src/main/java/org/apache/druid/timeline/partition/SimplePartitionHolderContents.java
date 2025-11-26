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

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Contents for {@link PartitionHolder} when segment locking was not used, and therefore no chunks have
 * {@link Overshadowable#getMinorVersion()}.
 */
public class SimplePartitionHolderContents<T extends Overshadowable<T>> implements PartitionHolderContents<T>
{
  /**
   * Map of {@link PartitionChunk#getChunkNumber()} to the actual {@link PartitionChunk}.
   */
  private final Int2ObjectMap<PartitionChunk<T>> chunkForPartition = new Int2ObjectAVLTreeMap<>();

  @Override
  public boolean isEmpty()
  {
    return chunkForPartition.isEmpty();
  }

  @Override
  public boolean areVisibleChunksConsistent()
  {
    return true;
  }

  @Override
  public boolean addChunk(PartitionChunk<T> chunk)
  {
    if (chunk.getObject().getMinorVersion() != 0) {
      throw DruidException.defensive("Cannot handle chunk with minorVersion[%d]", chunk.getObject().getMinorVersion());
    }

    final PartitionChunk<T> existingChunk = chunkForPartition.putIfAbsent(chunk.getChunkNumber(), chunk);
    if (existingChunk != null) {
      if (!existingChunk.equals(chunk)) {
        throw DruidException.defensive(
            "existingChunk[%s] is different from newChunk[%s] for partitionId[%d]",
            existingChunk,
            chunk,
            chunk.getChunkNumber()
        );
      } else {
        // A new chunk of the same major version and partitionId can be added in segment handoff
        // from stream ingestion tasks to historicals
        return false;
      }
    } else {
      return true;
    }
  }

  @Override
  @Nullable
  public PartitionChunk<T> removeChunk(PartitionChunk<T> chunk)
  {
    final PartitionChunk<T> knownChunk = chunkForPartition.get(chunk.getChunkNumber());
    if (knownChunk == null) {
      return null;
    }

    if (!knownChunk.equals(chunk)) {
      throw DruidException.defensive(
          "Unexpected state: Same partitionId[%d], but known partition[%s] is different from the input partition[%s]",
          chunk.getChunkNumber(),
          knownChunk,
          chunk
      );
    }

    return chunkForPartition.remove(chunk.getChunkNumber());
  }

  @Override
  public PartitionChunk<T> getChunk(int partitionNum)
  {
    return chunkForPartition.get(partitionNum);
  }

  @Override
  public Iterator<PartitionChunk<T>> visibleChunksIterator()
  {
    return chunkForPartition.values().iterator();
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
    for (PartitionChunk<T> chunk : chunkForPartition.values()) {
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
    return Objects.equals(chunkForPartition, that.chunkForPartition);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(chunkForPartition);
  }

  @Override
  public String toString()
  {
    return "SimplePartitionHolderContents{" +
           "chunkForPartition=" + chunkForPartition +
           '}';
  }
}
