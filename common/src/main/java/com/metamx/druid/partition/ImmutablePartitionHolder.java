package com.metamx.druid.partition;

/**
 */
public class ImmutablePartitionHolder<T> extends PartitionHolder<T>
{
  public ImmutablePartitionHolder(PartitionHolder partitionHolder)
  {
    super(partitionHolder);
  }

  @Override
  public PartitionChunk<T> remove(PartitionChunk<T> tPartitionChunk)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(PartitionChunk<T> tPartitionChunk)
  {
    throw new UnsupportedOperationException();
  }
}
