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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * Contents for {@link PartitionHolder}.
 *
 * @see SimplePartitionHolderContents implementation when segment locking is not in play
 * @see OvershadowableManager implementation when segment locking is in play
 */
public interface PartitionHolderContents<T>
{
  /**
   * Whether this holder is empty.
   */
  boolean isEmpty();

  /**
   * Whether all visible chunks are consistent, meaning they can possibly be considered for
   * {@link PartitionHolder#isComplete()}. When segment locking is not being used, all chunks
   * are consistent, so this always returns true.
   */
  boolean areVisibleChunksConsistent();

  /**
   * Adds a chunk.
   *
   * @return true if no chunk previously existed with these partition boundaries
   */
  boolean addChunk(PartitionChunk<T> chunk);

  /**
   * Removes and returns a chunk with the same partition boundaries as the provided chunk. Returns null if
   * no such chunk exists.
   */
  @Nullable
  PartitionChunk<T> removeChunk(PartitionChunk<T> partitionChunk);

  /**
   * Returns the chunk with a given partition ID, or null if none exists.
   */
  @Nullable
  PartitionChunk<T> getChunk(int partitionId);

  /**
   * Iterates through all visible chunks. When segment locking is not being used, all chunks are visible,
   * so this returns all chunks.
   */
  Iterator<PartitionChunk<T>> visibleChunksIterator();

  /**
   * Returns chunks that are tracked but not visible. When segment locking is not being used, all chunks are
   * visible, so this returns nothing.
   */
  List<PartitionChunk<T>> getOvershadowedChunks();

  /**
   * Returns a copy of this holder with only visible chunks.
   */
  PartitionHolderContents<T> copyVisible();

  /**
   * Returns a copy of this holder with all chunks.
   */
  PartitionHolderContents<T> deepCopy();
}
