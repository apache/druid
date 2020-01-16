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

import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class HashJoinEngine
{
  private HashJoinEngine()
  {
    // No instantiation.
  }

  /**
   * Creates a cursor that represents the join of {@param leftCursor} with {@param joinableClause}. The resulting
   * cursor may generate nulls on the left-hand side (for righty joins; see {@link JoinType#isRighty()}) or on
   * the right-hand side (for lefty joins; see {@link JoinType#isLefty()}). Columns that start with the
   * joinable clause's prefix (see {@link JoinableClause#getPrefix()}) will come from the Joinable's column selector
   * factory, and all other columns will come from the leftCursor's column selector factory.
   *
   * Ensuring that the joinable clause's prefix does not conflict with any columns from "leftCursor" is the
   * responsibility of the caller. If there is such a conflict (for example, if the joinable clause's prefix is "j.",
   * and the leftCursor has a field named "j.j.abrams"), then the field from the leftCursor will be shadowed and will
   * not be queryable through the returned Cursor. This happens even if the right-hand joinable doesn't actually have a
   * column with this name.
   */
  public static Cursor makeJoinCursor(final Cursor leftCursor, final JoinableClause joinableClause)
  {
    final ColumnSelectorFactory leftColumnSelectorFactory = leftCursor.getColumnSelectorFactory();
    final JoinMatcher joinMatcher = joinableClause.getJoinable()
                                                  .makeJoinMatcher(
                                                      leftColumnSelectorFactory,
                                                      joinableClause.getCondition(),
                                                      joinableClause.getJoinType().isRighty()
                                                  );

    class JoinColumnSelectorFactory implements ColumnSelectorFactory
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        if (joinableClause.includesColumn(dimensionSpec.getDimension())) {
          return joinMatcher.getColumnSelectorFactory()
                            .makeDimensionSelector(
                                dimensionSpec.withDimension(joinableClause.unprefix(dimensionSpec.getDimension()))
                            );
        } else {
          final DimensionSelector leftSelector = leftColumnSelectorFactory.makeDimensionSelector(dimensionSpec);

          if (!joinableClause.getJoinType().isRighty()) {
            return leftSelector;
          } else {
            return new PossiblyNullDimensionSelector(leftSelector, joinMatcher::matchingRemainder);
          }
        }
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String column)
      {
        if (joinableClause.includesColumn(column)) {
          return joinMatcher.getColumnSelectorFactory().makeColumnValueSelector(joinableClause.unprefix(column));
        } else {
          final ColumnValueSelector<?> leftSelector = leftColumnSelectorFactory.makeColumnValueSelector(column);

          if (!joinableClause.getJoinType().isRighty()) {
            return leftSelector;
          } else {
            return new PossiblyNullColumnValueSelector<>(leftSelector, joinMatcher::matchingRemainder);
          }
        }
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (joinableClause.includesColumn(column)) {
          return joinMatcher.getColumnSelectorFactory().getColumnCapabilities(joinableClause.unprefix(column));
        } else {
          return leftColumnSelectorFactory.getColumnCapabilities(column);
        }
      }
    }

    final JoinColumnSelectorFactory joinColumnSelectorFactory = new JoinColumnSelectorFactory();

    class JoinCursor implements Cursor
    {
      public void initialize()
      {
        matchCurrentPosition();

        if (!joinableClause.getJoinType().isLefty()) {
          while (!joinMatcher.hasMatch() && !isDone()) {
            advance();
            matchCurrentPosition();
          }
        }
      }

      @Override
      @Nonnull
      public ColumnSelectorFactory getColumnSelectorFactory()
      {
        return joinColumnSelectorFactory;
      }

      @Override
      @Nonnull
      public DateTime getTime()
      {
        return leftCursor.getTime();
      }

      @Override
      public void advance()
      {
        advanceUninterruptibly();
        BaseQuery.checkInterrupted();
      }

      private void matchCurrentPosition()
      {
        if (leftCursor.isDone()) {
          if (joinableClause.getJoinType().isRighty() && !joinMatcher.matchingRemainder()) {
            // Warning! The way this engine handles "righty" joins is flawed: it generates the 'remainder' rows
            // per-segment, but this should really be done globally. This should be improved in the future.
            joinMatcher.matchRemainder();
          }
        } else {
          joinMatcher.matchCondition();
        }
      }

      @Override
      public void advanceUninterruptibly()
      {
        if (joinMatcher.hasMatch()) {
          joinMatcher.nextMatch();

          if (joinMatcher.hasMatch()) {
            return;
          }
        }

        assert !joinMatcher.hasMatch();

        do {
          // No more right-hand side matches; advance the left-hand side.
          leftCursor.advanceUninterruptibly();

          // Update joinMatcher state to match new cursor position.
          matchCurrentPosition();

          // If this is not a left/full join, and joinMatcher didn't match anything, then keep advancing until we find
          // left rows that have matching right rows.
        } while (!joinableClause.getJoinType().isLefty()
                 && !joinMatcher.hasMatch()
                 && !leftCursor.isDone());
      }

      @Override
      public boolean isDone()
      {
        return leftCursor.isDone() && !joinMatcher.hasMatch();
      }

      @Override
      public boolean isDoneOrInterrupted()
      {
        return isDone() || Thread.currentThread().isInterrupted();
      }

      @Override
      public void reset()
      {
        leftCursor.reset();
        joinMatcher.reset();
      }
    }

    final JoinCursor joinCursor = new JoinCursor();
    joinCursor.initialize();
    return joinCursor;
  }
}
