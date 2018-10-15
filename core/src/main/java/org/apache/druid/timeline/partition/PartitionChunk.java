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
  T getObject();

  /**
   * Determines if this PartitionChunk abuts another PartitionChunk.  A sequence of abutting PartitionChunks should
   * start with an object where isStart() == true and eventually end with an object where isEnd() == true.
   *
   * @param chunk input chunk
   * @return true if this chunk abuts the input chunk
   */
  boolean abuts(PartitionChunk<T> chunk);

  /**
   * Returns true if this chunk is the beginning of the partition. Most commonly, that means it represents the range
   * [-infinity, X) for some concrete X.
   *
   * @return true if the chunk is the beginning of the partition
   */
  boolean isStart();

  /**
   * Returns true if this chunk is the end of the partition.  Most commonly, that means it represents the range
   * [X, infinity] for some concrete X.
   *
   * @return true if the chunk is the beginning of the partition
   */
  boolean isEnd();

  /**
   * Returns the partition chunk number of this PartitionChunk.  I.e. if there are 4 partitions in total and this
   * is the 3rd partition, it will return 2
   *
   * @return the sequential numerical id of this partition chunk
   */
  int getChunkNumber();
}
