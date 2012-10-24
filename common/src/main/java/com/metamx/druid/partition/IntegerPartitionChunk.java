package com.metamx.druid.partition;

import com.google.common.collect.Ordering;

import java.util.Comparator;

/**
 */
public class IntegerPartitionChunk<T> implements PartitionChunk<T>
{
  Comparator<Integer> comparator = Ordering.<Integer>natural().nullsFirst();

  private final Integer start;
  private final Integer end;
  private final int chunkNumber;
  private final T object;

  public static <T> IntegerPartitionChunk<T> make(Integer start, Integer end, int chunkNumber, T obj)
  {
    return new IntegerPartitionChunk<T>(start, end, chunkNumber, obj);
  }

  public IntegerPartitionChunk(
      Integer start,
      Integer end,
      int chunkNumber,
      T object
  )
  {
    this.start = start;
    this.end = end;
    this.chunkNumber = chunkNumber;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> chunk)
  {
    if (chunk instanceof IntegerPartitionChunk) {
      IntegerPartitionChunk<T> intChunk = (IntegerPartitionChunk<T>) chunk;

      if (intChunk.isStart()) {
        return false;
      }
      return intChunk.start.equals(end);
    }

    return false;
  }

  @Override
  public boolean isStart()
  {
    return start == null;
  }

  @Override
  public boolean isEnd()
  {
    return end == null;
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    if (chunk instanceof IntegerPartitionChunk) {
      IntegerPartitionChunk<T> intChunk = (IntegerPartitionChunk<T>) chunk;

      int retVal = comparator.compare(start, intChunk.start);
      if (retVal == 0) {
        retVal = comparator.compare(end, intChunk.end);
      }

      return retVal;
    }
    throw new IllegalArgumentException("Cannot compare against something that is not a StringPartitionChunk.");
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

    return compareTo((IntegerPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    int result = start != null ? start.hashCode() : 0;
    result = 31 * result + (end != null ? end.hashCode() : 0);
    result = 31 * result + (object != null ? object.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "IntegerPartitionChunk{" +
           "start=" + start +
           ", end=" + end +
           ", object=" + object +
           '}';
  }
}
