/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.timeboundary;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.granularity.AllGranularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Cursor;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;


/**
 */
public class TimeBoundaryQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final TimeBoundaryQueryQueryToolChest toolChest = new TimeBoundaryQueryQueryToolChest();
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeBoundaryQueryRunnerFactory(QueryWatcher queryWatcher)
  {
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> createRunner(final Segment segment)
  {
    return new TimeBoundaryQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeBoundaryResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery> getToolchest()
  {
    return toolChest;
  }

  private static class TimeBoundaryQueryRunner implements QueryRunner<Result<TimeBoundaryResultValue>>
  {
    private final StorageAdapter adapter;
    private final Function<Cursor, Result<DateTime>> skipToFirstMatching;

    public TimeBoundaryQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter();
      this.skipToFirstMatching = new Function<Cursor, Result<DateTime>>()
      {
        @Override
        public Result<DateTime> apply(Cursor cursor)
        {
          if (cursor.isDone()) {
            return null;
          }
          final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);
          final DateTime timestamp = new DateTime(timestampColumnSelector.get());
          return new Result<>(adapter.getInterval().getStart(), timestamp);
        }
      };
    }

    private DateTime getTimeBoundary(StorageAdapter adapter, TimeBoundaryQuery legacyQuery, boolean descending)
    {
      final Sequence<Result<DateTime>> resultSequence = QueryRunnerHelper.makeCursorBasedQuery(
          adapter,
          legacyQuery.getQuerySegmentSpec().getIntervals(),
          Filters.toFilter(legacyQuery.getDimensionsFilter()),
          descending,
          new AllGranularity(),
          this.skipToFirstMatching
      );
      final List<Result<DateTime>> resultList = Sequences.toList(
          Sequences.limit(resultSequence, 1),
          Lists.<Result<DateTime>>newArrayList()
      );
      if (resultList.size() > 0) {
        return resultList.get(0).getValue();
      }

      return null;
    }

    @Override
    public Sequence<Result<TimeBoundaryResultValue>> run(
        final Query<Result<TimeBoundaryResultValue>> input,
        final Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TimeBoundaryQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeBoundaryQuery.class);
      }

      final TimeBoundaryQuery legacyQuery = (TimeBoundaryQuery) input;

      return new BaseSequence<>(
          new BaseSequence.IteratorMaker<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>()
          {
            @Override
            public Iterator<Result<TimeBoundaryResultValue>> make()
            {
              if (adapter == null) {
                throw new ISE(
                    "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
                );
              }
              final DateTime minTime;
              final DateTime maxTime;

              if (legacyQuery.getDimensionsFilter() != null) {
                minTime = getTimeBoundary(adapter, legacyQuery, false);
                if (minTime == null) {
                  maxTime = null;
                } else {
                  maxTime = getTimeBoundary(adapter, legacyQuery, true);
                }
              } else {
                minTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MAX_TIME)
                          ? null
                          : adapter.getMinTime();
                maxTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MIN_TIME)
                          ? null
                          : adapter.getMaxTime();
              }

              return legacyQuery.buildResult(
                  adapter.getInterval().getStart(),
                  minTime,
                  maxTime
              ).iterator();
            }

            @Override
            public void cleanup(Iterator<Result<TimeBoundaryResultValue>> toClean)
            {

            }
          }
      );
    }
  }
}
