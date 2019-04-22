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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class AtomicUpdateGroup<T extends Overshadowable<T>> implements Overshadowable<AtomicUpdateGroup<T>>
{
  // TODO: map??
  private final List<PartitionChunk<T>> chunks = new ArrayList<>();

  public AtomicUpdateGroup(PartitionChunk<T> chunk)
  {
    this.chunks.add(chunk);
  }

  public void add(PartitionChunk<T> chunk)
  {
    if (isFull()) {
      throw new IAE("Can't add more chunk[%s] to atomicUpdateGroup[%s]", chunk, chunks);
    }
    if (!isSameAtomicUpdateGroup(chunks.get(0), chunk)) {
      throw new IAE("Can't add chunk[%s] to a different atomicUpdateGroup[%s]", chunk, chunks);
    }
    if (replaceChunkWith(chunk) == null) {
      chunks.add(chunk);
    }
  }

  public void remove(PartitionChunk<T> chunk)
  {
    if (chunks.isEmpty()) {
      throw new ISE("Can't remove chunk[%s] from empty atomicUpdateGroup", chunk);
    }

    if (!isSameAtomicUpdateGroup(chunks.get(0), chunk)) {
      throw new IAE("Can't remove chunk[%s] from a different atomicUpdateGroup[%s]", chunk, chunks);
    }

    chunks.remove(chunk);
  }

  public boolean isFull()
  {
    return chunks.size() == chunks.get(0).getObject().getAtomicUpdateGroupSize();
  }

  public boolean isEmpty()
  {
    return chunks.isEmpty();
  }

  public List<PartitionChunk<T>> getChunks()
  {
    return chunks;
  }

  @Nullable
  public PartitionChunk<T> findChunk(int partitionId)
  {
    return chunks.stream().filter(chunk -> chunk.getChunkNumber() == partitionId).findFirst().orElse(null);
  }

  @Nullable
  public PartitionChunk<T> replaceChunkWith(PartitionChunk<T> newChunk)
  {
    PartitionChunk<T> oldChunk = null;
    for (int i = 0; i < chunks.size(); i++) {
      if (newChunk.getChunkNumber() == chunks.get(i).getChunkNumber()) {
        oldChunk = chunks.set(i, newChunk);
        break;
      }
    }
    return oldChunk;
  }

  @Override
  public int getStartRootPartitionId()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getStartRootPartitionId();
  }

  @Override
  public short getStartRootPartitionIdAsShort()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getStartRootPartitionIdAsShort();
  }

  @Override
  public int getEndRootPartitionId()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getEndRootPartitionId();
  }

  @Override
  public short getEndRootPartitionIdAsShort()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getEndRootPartitionIdAsShort();
  }

  @Override
  public short getMinorVersion()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getMinorVersion();
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    Preconditions.checkState(!isEmpty(), "Empty atomicUpdateGroup");
    return chunks.get(0).getObject().getAtomicUpdateGroupSize();
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
    AtomicUpdateGroup<?> that = (AtomicUpdateGroup<?>) o;
    return Objects.equals(chunks, that.chunks);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(chunks);
  }

  @Override
  public String toString()
  {
    return "AtomicUpdateGroup{" +
           "chunks=" + chunks +
           '}';
  }

  private static <T extends Overshadowable<T>> boolean isSameAtomicUpdateGroup(
      PartitionChunk<T> c1,
      PartitionChunk<T> c2
  )
  {
    return c1.getObject().getStartRootPartitionId() == c2.getObject().getStartRootPartitionId()
        && c1.getObject().getEndRootPartitionId() == c2.getObject().getEndRootPartitionId()
        && c1.getObject().getMinorVersion() == c2.getObject().getMinorVersion()
        && c1.getObject().getAtomicUpdateGroupSize() == c2.getObject().getAtomicUpdateGroupSize();
  }
}
