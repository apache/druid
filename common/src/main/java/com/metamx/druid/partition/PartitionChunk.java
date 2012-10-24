package com.metamx.druid.partition;

/**
 * A PartitionChunk represents a chunk of a partitioned(sharded) space.  It has knowledge of whether it is
 * the start of the domain of partitions, the end of the domain, if it abuts another partition and where it stands
 * inside of a sorted collection of partitions.
 *
 * The ordering of PartitionChunks is based entirely upon the partition boundaries defined inside the concrete
 * PartitionChunk class.  That is, the payload (the object returned by getObject()) should *not* be involved in
 * comparisons between PartitionChunk objects.
 */
public interface PartitionChunk<T> extends Comparable<PartitionChunk<T>>
{
  /**
   * Returns the payload, generally an object that can be used to perform some action against the shard.
   *
   * @return the payload
   */
  public T getObject();

  /**
   * Determines if this PartitionChunk abuts another PartitionChunk.  A sequence of abutting PartitionChunks should
   * start with an object where isStart() == true and eventually end with an object where isEnd() == true.
   *
   * @param chunk input chunk
   * @return true if this chunk abuts the input chunk
   */
  public boolean abuts(PartitionChunk<T> chunk);

  /**
   * Returns true if this chunk is the beginning of the partition. Most commonly, that means it represents the range
   * [-infinity, X) for some concrete X.
   *
   * @return true if the chunk is the beginning of the partition
   */
  public boolean isStart();

  /**
   * Returns true if this chunk is the end of the partition.  Most commonly, that means it represents the range
   * [X, infinity] for some concrete X.
   *
   * @return true if the chunk is the beginning of the partition
   */
  public boolean isEnd();

  /**
   * Returns the partition chunk number of this PartitionChunk.  I.e. if there are 4 partitions in total and this
   * is the 3rd partition, it will return 2
   *
   * @return the sequential numerical id of this partition chunk
   */
  public int getChunkNumber();
}
