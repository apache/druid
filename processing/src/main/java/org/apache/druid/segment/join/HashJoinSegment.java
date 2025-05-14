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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.rowsandcols.CursorFactoryRowsAndColumns;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.SimpleTopNOptimizationInspector;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.TopNOptimizationInspector;
import org.apache.druid.segment.WrappedTimeBoundaryInspector;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Represents a deep, left-heavy join of a left-hand side baseSegment onto a series of right-hand side clauses.
 * <p>
 * In other words, logically the operation is: join(join(join(baseSegment, clauses[0]), clauses[1]), clauses[2]) etc.
 */
public class HashJoinSegment implements SegmentReference
{
  private static final Logger log = new Logger(HashJoinSegment.class);

  private final SegmentReference baseSegment;
  @Nullable
  private final Filter baseFilter;
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
      @Nullable Filter baseFilter,
      List<JoinableClause> clauses,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    this.baseSegment = baseSegment;
    this.baseFilter = baseFilter;
    this.clauses = clauses;
    this.joinFilterPreAnalysis = joinFilterPreAnalysis;

    // Verify this virtual segment is doing something useful (otherwise it's a waste to create this object)
    if (clauses.isEmpty() && baseFilter == null) {
      throw new IAE("'clauses' and 'baseFilter' are both empty, no need to create HashJoinSegment");
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

  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (CursorFactory.class.equals(clazz)) {
      return (T) new HashJoinSegmentCursorFactory(
          baseSegment.as(CursorFactory.class),
          baseFilter,
          clauses,
          joinFilterPreAnalysis
      );
    } else if (CloseableShapeshifter.class.equals(clazz)) {
      return (T) new CursorFactoryRowsAndColumns(as(CursorFactory.class));
    } else if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) WrappedTimeBoundaryInspector.create(baseSegment.as(TimeBoundaryInspector.class));
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      // if the baseFilter is not null, then rows from underlying cursor can be potentially filtered.
      // otherwise, a filtering inner or left join can also filter rows.
      return (T) new SimpleTopNOptimizationInspector(
          baseFilter == null && clauses.stream().allMatch(
              clause -> clause.getJoinType().isLefty() || clause.getCondition().isAlwaysTrue()
          )
      );
    }
    return null;
  }

  @Override
  public void validateOrElseThrow(PolicyEnforcer policyEnforcer)
  {
    baseSegment.validateOrElseThrow(policyEnforcer);
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
        acquireFailed = joinClause.acquireReferences().map(closeable -> {
          closer.register(closeable);
          return false;
        }).orElse(true);
      }
      if (acquireFailed) {
        CloseableUtils.closeAndWrapExceptions(closer);
        return Optional.empty();
      } else {
        return Optional.of(closer);
      }
    }
    catch (Throwable e) {
      // acquireReferences is not permitted to throw exceptions.
      CloseableUtils.closeAndSuppressExceptions(closer, e::addSuppressed);
      log.warn(e, "Exception encountered while trying to acquire reference");
      return Optional.empty();
    }
  }
}
