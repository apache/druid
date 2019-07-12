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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.TreeMap;

/**
 * An object that clumps together multiple other objects which each represent a shard of some space.
 */
public class PartitionHolder<T> implements Iterable<PartitionChunk<T>>
{
  private final TreeMap<PartitionChunk<T>, PartitionChunk<T>> holderMap;

  public PartitionHolder(PartitionChunk<T> initialChunk)
  {
    this.holderMap = new TreeMap<>();
    add(initialChunk);
  }

  public PartitionHolder(List<PartitionChunk<T>> initialChunks)
  {
    this.holderMap = new TreeMap<>();
    for (PartitionChunk<T> chunk : initialChunks) {
      add(chunk);
    }
  }

  public PartitionHolder(PartitionHolder<T> partitionHolder)
  {
    this.holderMap = new TreeMap<>();
    this.holderMap.putAll(partitionHolder.holderMap);
  }

  public boolean add(PartitionChunk<T> chunk)
  {
    return holderMap.putIfAbsent(chunk, chunk) == null;
  }

  @Nullable
  public PartitionChunk<T> remove(PartitionChunk<T> chunk)
  {
    return holderMap.remove(chunk);
  }

  public boolean isEmpty()
  {
    return holderMap.isEmpty();
  }

  public boolean isComplete()
  {
    if (holderMap.isEmpty()) {
      return false;
    }

    Iterator<PartitionChunk<T>> iter = holderMap.keySet().iterator();

    PartitionChunk<T> curr = iter.next();

    if (!curr.isStart()) {
      return false;
    }

    if (curr.isEnd()) {
      return true;
    }

    while (iter.hasNext()) {
      PartitionChunk<T> next = iter.next();
      if (!curr.abuts(next)) {
        return false;
      }

      if (next.isEnd()) {
        return true;
      }
      curr = next;
    }

    return false;
  }

  public PartitionChunk<T> getChunk(final int partitionNum)
  {
    final Iterator<PartitionChunk<T>> retVal = Iterators.filter(
        holderMap.keySet().iterator(),
        input -> input.getChunkNumber() == partitionNum
    );

    return retVal.hasNext() ? retVal.next() : null;
  }

  @Override
  public Iterator<PartitionChunk<T>> iterator()
  {
    return holderMap.keySet().iterator();
  }

  @Override
  public Spliterator<PartitionChunk<T>> spliterator()
  {
    return holderMap.keySet().spliterator();
  }

  public Iterable<T> payloads()
  {
    return Iterables.transform(this, PartitionChunk::getObject);
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

    PartitionHolder that = (PartitionHolder) o;

    if (!holderMap.equals(that.holderMap)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return holderMap.hashCode();
  }

  @Override
  public String toString()
  {
    return "PartitionHolder{" +
           "holderMap=" + holderMap +
           '}';
  }
}
