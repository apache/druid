/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.timeline.partition;

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
