package com.metamx.druid.partition;

public class LinearPartitionChunk <T> implements PartitionChunk<T>
{
  private final int chunkNumber;
  private final T object;

  public static <T> LinearPartitionChunk<T> make(int chunkNumber, T obj)
  {
    return new LinearPartitionChunk<T>(chunkNumber, obj);
  }

  public LinearPartitionChunk(
      int chunkNumber,
      T object
  )
  {
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
    return true; // always complete
  }

  @Override
  public boolean isStart()
  {
    return true; // always complete
  }

  @Override

  public boolean isEnd()
  {
    return true; // always complete
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    if (chunk instanceof LinearPartitionChunk) {
      LinearPartitionChunk<T> linearChunk = (LinearPartitionChunk<T>) chunk;

      return chunkNumber - chunk.getChunkNumber();
    }
    throw new IllegalArgumentException("Cannot compare against something that is not a LinearPartitionChunk.");
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

    return compareTo((LinearPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return chunkNumber;
  }
}
