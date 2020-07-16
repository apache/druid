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

package org.apache.druid.segment.join;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Represents a deep, left-heavy join of a left-hand side baseSegment onto a series of right-hand side clauses.
 *
 * In other words, logically the operation is: join(join(join(baseSegment, clauses[0]), clauses[1]), clauses[2]) etc.
 */
public class HashJoinSegment implements SegmentReference
{
  private final SegmentReference baseSegment;
  private final List<JoinableClause> clauses;
  private final JoinFilterPreAnalysis joinFilterPreAnalysis;

  /**
   * @param baseSegment           The left-hand side base segment
   * @param clauses               The right-hand side clauses. The caller is responsible for ensuring that there are no
   *                              duplicate prefixes or prefixes that shadow each other across the clauses
   * @param joinFilterPreAnalysis Pre-analysis for the query we expect to run on this segment
   */
  public HashJoinSegment(
      SegmentReference baseSegment,
      List<JoinableClause> clauses,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    this.baseSegment = baseSegment;
    this.clauses = clauses;
    this.joinFilterPreAnalysis = joinFilterPreAnalysis;

    // Verify 'clauses' is nonempty (otherwise it's a waste to create this object, and the caller should know)
    if (clauses.isEmpty()) {
      throw new IAE("'clauses' is empty, no need to create HashJoinSegment");
    }
  }

  @Override
  public SegmentId getId()
  {
    return baseSegment.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    // __time column will come from the baseSegment, so use its data interval.
    return baseSegment.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    // Even if baseSegment is a QueryableIndex, we don't want to expose it, since we've modified its behavior
    // too much while wrapping it.
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new HashJoinSegmentStorageAdapter(baseSegment.asStorageAdapter(), clauses, joinFilterPreAnalysis);
  }

  @Override
  public void close() throws IOException
  {
    baseSegment.close();
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    Closer closer = Closer.create();
    try {
      boolean acquireFailed = baseSegment.acquireReferences().map(closeable -> {
        closer.register(closeable);
        return false;
      }).orElse(true);

      for (JoinableClause joinClause : clauses) {
        if (acquireFailed) {
          break;
        }
        acquireFailed |= joinClause.acquireReferences().map(closeable -> {
          closer.register(closeable);
          return false;
        }).orElse(true);
      }
      if (acquireFailed) {
        CloseQuietly.close(closer);
        return Optional.empty();
      } else {
        return Optional.of(closer);
      }
    }
    catch (Exception ex) {
      CloseQuietly.close(closer);
      return Optional.empty();
    }
  }
}
