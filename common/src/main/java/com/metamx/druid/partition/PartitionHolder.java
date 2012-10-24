package com.metamx.druid.partition;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * An object that clumps together multiple other objects which each represent a shard of some space.
 */
public class PartitionHolder<T> implements Iterable<PartitionChunk<T>>
{
  private final TreeSet<PartitionChunk<T>> holderSet;

  public PartitionHolder(PartitionChunk<T> initialChunk)
  {
    this.holderSet = Sets.newTreeSet();
    add(initialChunk);
  }

  public PartitionHolder(List<PartitionChunk<T>> initialChunks)
  {
    this.holderSet = Sets.newTreeSet();
    for (PartitionChunk<T> chunk : initialChunks) {
      add(chunk);
    }
  }

  public PartitionHolder(PartitionHolder partitionHolder)
  {
    this.holderSet = Sets.newTreeSet();
    this.holderSet.addAll(partitionHolder.holderSet);
  }

  public void add(PartitionChunk<T> chunk)
  {
    holderSet.add(chunk);
  }

  public PartitionChunk<T> remove(PartitionChunk<T> chunk)
  {
    // Somewhat funky implementation in order to return the removed object as it exists in the set
    PartitionChunk<T> element = holderSet.tailSet(chunk, true).first();
    if (chunk.equals(element)) {
      holderSet.remove(element);
      return element;
    }
    return null;
  }

  public boolean isEmpty()
  {
    return holderSet.isEmpty();
  }

  public boolean isComplete()
  {
    if (holderSet.isEmpty()) {
      return false;
    }

    Iterator<PartitionChunk<T>> iter = holderSet.iterator();

    PartitionChunk<T> curr = iter.next();
    if (!curr.isStart()) {
      return false;
    }

    while (iter.hasNext()) {
      PartitionChunk<T> next = iter.next();
      if (!curr.abuts(next)) {
        return false;
      }
      curr = next;
    }

    if (!curr.isEnd()) {
      return false;
    }

    return true;
  }

  public PartitionChunk<T> getChunk(final int partitionNum) {
    final Iterator<PartitionChunk<T>> retVal = Iterators.filter(
        holderSet.iterator(), new Predicate<PartitionChunk<T>>()
        {
          @Override
          public boolean apply(PartitionChunk<T> input)
          {
            return input.getChunkNumber() == partitionNum;
          }
        }
    );

    return retVal.hasNext() ? retVal.next() : null;
  }

  @Override
  public Iterator<PartitionChunk<T>> iterator()
  {
    return holderSet.iterator();
  }

  public Iterable<T> payloads()
  {
    return Iterables.transform(
        this,
        new Function<PartitionChunk<T>, T>()
        {
          @Override
          public T apply(PartitionChunk<T> input)
          {
            return input.getObject();
          }
        }
    );
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

    if (!holderSet.equals(that.holderSet)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return holderSet.hashCode();
  }

  @Override
  public String toString()
  {
    return "PartitionHolder{" +
           "holderSet=" + holderSet +
           '}';
  }
}
