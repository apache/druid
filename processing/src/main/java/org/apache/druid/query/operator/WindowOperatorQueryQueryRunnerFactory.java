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

package org.apache.druid.query.operator;

import com.google.common.base.Function;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class WindowOperatorQueryQueryRunnerFactory implements QueryRunnerFactory<RowsAndColumns, WindowOperatorQuery>
{
  public static final WindowOperatorQueryQueryToolChest TOOLCHEST = new WindowOperatorQueryQueryToolChest();

  @Override
  public QueryRunner<RowsAndColumns> createRunner(Segment segment)
  {
    return (queryPlus, responseContext) ->
        new OperatorSequence(() -> {
          Operator op = new SegmentToRowsAndColumnsOperator(segment);

          final List<Interval> intervals = queryPlus.getQuery().getIntervals();
          if (intervals.size() != 1) {
            throw DruidException.defensive("Can only handle a single interval, got [%s]", intervals);
          }

          final Interval interval = intervals.get(0);
          if (!Intervals.isEternity(interval)) {
            op = new LimitTimeIntervalOperator(op, interval);
          }

          for (OperatorFactory leaf : ((WindowOperatorQuery) queryPlus.getQuery()).getLeafOperators()) {
            op = leaf.wrap(op);
          }

          return op;
        });
  }

  @Override
  public QueryRunner<RowsAndColumns> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<RowsAndColumns>> queryRunners
  )
  {
    // This merge is extremely naive, there is no ordering being imposed over the data, nor is there any attempt
    // to shrink the size of the data before pushing it across the wire.  This code implementation is intended more
    // to make this work for tests and less to work in production.  That's why the WindowOperatorQuery forces
    // a super-secret context parameter to be set to actually allow it to run a query that pushes all the way down
    // like this. When this gets fixed, we can remove that parameter.
    return (queryPlus, responseContext) -> Sequences.concat(
        Sequences.map(
            Sequences.simple(queryRunners),
            new Function<QueryRunner<RowsAndColumns>, Sequence<RowsAndColumns>>()
            {
              @SuppressWarnings("ConstantConditions")
              @Nullable
              @Override
              public Sequence<RowsAndColumns> apply(
                  @Nullable QueryRunner<RowsAndColumns> input
              )
              {
                return Sequences.map(
                    input.run(queryPlus, responseContext),
                    new Function<>()
                    {
                      @Nullable
                      @Override
                      public RowsAndColumns apply(@Nullable RowsAndColumns input)
                      {
                        if (input instanceof LazilyDecoratedRowsAndColumns) {
                          return input.as(FrameRowsAndColumns.class);
                        }
                        return input;
                      }
                    }
                );

              }
            }
        )
    );
  }

  @Override
  public QueryToolChest<RowsAndColumns, WindowOperatorQuery> getToolchest()
  {
    return TOOLCHEST;
  }

}
