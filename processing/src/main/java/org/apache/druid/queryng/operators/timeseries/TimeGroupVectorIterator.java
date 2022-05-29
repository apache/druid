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

package org.apache.druid.queryng.operators.timeseries;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import java.util.Iterator;

/**
 * Double iterator over the query intervals within a segment interval. First
 * clips the query interval to that covered by a segment, then splits that
 * overall interval into a set of contiguous granularity-sized groups. The
 * iterator iterates over these groups. A group may be empty: the caller can
 * decide whether to omit such empty groups.
 * <p>
 * This iterator is for vectorized access in which values are fetched in batches
 * of a given size. This leads to the second level of iteration: the batches
 * within each group.
 * <p>
 * The iterator starts before the first group. Call {@link #nextGroup()} to
 * move to the first group, which will return {@code false} if there are no
 * groups at all (the query interval does not overlap the segment interval, or
 * the segment is empty.)
 * <p>
 * Then, call {@nextBatch()} to prepare vectors for that batch. It returns
 * {@code false} at the end of the batch, and the cycle repeats.
 * <p>
 * In summary:<pre><code>
 * TimeGroupVectorIterator iter = ...
 * while (iter.nextGroup()) {
 *   // Initialize the batch
 *   while (iter.nextBatch()) {
 *     // Process the batch
 *   }
 *   // Finalize the batch
 * }</code></pre>
 *
 */
public class TimeGroupVectorIterator
{
  private enum State
  {
    START,
    EMPTY_GROUP,
    START_GROUP,
    IN_GROUP,
    END_OF_GROUP,
    EOF
  }
  private final VectorCursor cursor;
  private final VectorCursorGranularizer granularizer;
  private final Granularity granularity;
  private final Iterator<Interval> intervalIter;
  private final boolean includeEmptyGroups;
  private Interval interval;
  private State state = State.START;

  public TimeGroupVectorIterator(
      final StorageAdapter storageAdapter,
      final VectorCursor cursor,
      final Granularity granularity,
      final Interval queryInterval,
      final boolean includeEmptyGroups
  )
  {
    this.cursor = cursor;
    this.includeEmptyGroups = includeEmptyGroups;
    this.granularity = granularity;
    this.granularizer = VectorCursorGranularizer.create(
        storageAdapter,
        cursor,
        granularity,
        queryInterval
    );
    if (granularizer == null) {
      state = State.EOF;
      intervalIter = null;
    } else {
      state = State.START;
      intervalIter = granularizer.getBucketIterable().iterator();
    }
  }

  public boolean nextGroup()
  {
    // Loop to find a group, which could be empty if requested.
    while (true) {
      if (state == State.EOF) {
        return false;
      }
      if (!intervalIter.hasNext()) {
        state = State.EOF;
        interval = null;
        return false;
      }

      // Inititialze within the current batch.
      interval = intervalIter.next();
      granularizer.setCurrentOffsets(interval);

      // Has data for this group
      if (granularizer.getEndOffset() > granularizer.getStartOffset()) {
        state = State.START_GROUP;
        return true;
      }

      // No data. Could be end of batch, or just the end of the group within
      // the batch. The granularizer will tell us.
      if (!granularizer.advanceCursorWithinBucket()) {
        // Definitely no data: the batch has data for another group.
        if (includeEmptyGroups) {
          state = State.EMPTY_GROUP;
          return true;
        } else {
          continue;
        }
      }

      // The granularizer advanced the cursor. The cursor uses an
      // advance-then-check protocol, so do the check.
      if (cursor.isDone()) {
        // No more batches.
        if (includeEmptyGroups) {
          state = State.EMPTY_GROUP;
          return true;
        } else {
          state = State.EOF;
          interval = null;
          return false;
        }
      }

      // We have a batch. Do the data check again. Any data would be at the start.
      granularizer.setCurrentOffsets(interval);
      if (granularizer.getEndOffset() > 0) {
        state = State.START_GROUP;
        return true;
      }

      // Empty group
      if (includeEmptyGroups) {
        state = State.EMPTY_GROUP;
        return true;
      }

      // Try the next group
    }
  }

  public boolean nextBatch()
  {
    switch (state) {
      case START_GROUP:
        state = State.IN_GROUP;
        return true;
      case EMPTY_GROUP:
        state = State.END_OF_GROUP;
        return true;
      case END_OF_GROUP:
      case EOF:
        return false;
      default:
    }
    if (!granularizer.advanceCursorWithinBucket()) {
      state = State.END_OF_GROUP;
      return false;
    }

    // We advanced. EOF?
    if (cursor.isDone()) {
      state = State.END_OF_GROUP;
      return false;
    }

    // We have a batch. But, does it contain any data?
    granularizer.setCurrentOffsets(interval);
    return (granularizer.getEndOffset() > granularizer.getStartOffset());
  }

  public Granularity granularity()
  {
    return granularity;
  }

  public Interval group()
  {
    return interval;
  }

  public boolean batchIsEmpty()
  {
    return startOffset() == endOffset();
  }

  /**
   * Consistent start offset for empty groups.
   */
  public int startOffset()
  {
    if (state == State.IN_GROUP) {
      return granularizer.getStartOffset();
    } else {
      return 0;
    }
  }

  /**
   * Consistent end offset for empty groups.
   */
  public int endOffset()
  {
    if (state == State.IN_GROUP) {
      return granularizer.getEndOffset();
    } else {
      return 0;
    }
  }
}
