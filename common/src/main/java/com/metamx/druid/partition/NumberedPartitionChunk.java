package com.metamx.druid.partition;

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
    Preconditions.checkArgument(chunkNumber < chunks, "chunkNumber < chunks");
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
    return other instanceof NumberedPartitionChunk && other.getChunkNumber() == chunkNumber + 1;
  }

  @Override
  public boolean isStart()
  {
    return chunkNumber == 0;
  }

  @Override
  public boolean isEnd()
  {
    return chunkNumber == chunks - 1;
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
}
