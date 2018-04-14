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

package io.druid.query.timeseries;

import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.filter.Filter;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeseriesQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private final TimeseriesQueryQueryToolChest toolChest;
  private final TimeseriesQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> createRunner(final Segment segment)
  {
    return new TimeseriesQueryRunner(engine, segment.asStorageAdapter());
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeseriesResultValue>>> queryRunners
  )
  {
    if (!queryRunners.iterator().hasNext()) {
      return (queryPlus, responseContext) -> {
        TimeseriesQuery ts = (TimeseriesQuery) queryPlus.getQuery();
        return engine.process(ts, EmptyStorageAdapter.INSTANCE);
      };
    }
    return new ChainedExecutionQueryRunner<>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> getToolchest()
  {
    return toolChest;
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
  {
    private final TimeseriesQueryEngine engine;
    private final StorageAdapter adapter;

    private TimeseriesQueryRunner(TimeseriesQueryEngine engine, StorageAdapter adapter)
    {
      this.engine = engine;
      this.adapter = adapter;
    }

    @Override
    public Sequence<Result<TimeseriesResultValue>> run(
        QueryPlus<Result<TimeseriesResultValue>> queryPlus,
        Map<String, Object> responseContext
    )
    {
      Query<Result<TimeseriesResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof TimeseriesQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
      }

      return engine.process((TimeseriesQuery) input, adapter);
    }
  }

  private static class EmptyStorageAdapter implements StorageAdapter
  {
    static final EmptyStorageAdapter INSTANCE = new EmptyStorageAdapter();

    private EmptyStorageAdapter() {}

    @Override
    public String getSegmentIdentifier()
    {
      return null;
    }

    @Override
    public Interval getInterval()
    {
      return null;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
      return null;
    }

    @Override
    public Iterable<String> getAvailableMetrics()
    {
      return null;
    }

    @Override
    public int getDimensionCardinality(String column)
    {
      return 0;
    }

    @Override
    public DateTime getMinTime()
    {
      return null;
    }

    @Override
    public DateTime getMaxTime()
    {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMinValue(String column)
    {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue(String column)
    {
      return null;
    }

    @Override
    public Capabilities getCapabilities()
    {
      return null;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }

    @Override
    public String getColumnTypeName(String column)
    {
      return null;
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public DateTime getMaxIngestedEventTime()
    {
      return null;
    }

    @Override
    public Metadata getMetadata()
    {
      return null;
    }

    @Override
    public Sequence<Cursor> makeCursors(
        @Nullable Filter filter,
        Interval interval,
        VirtualColumns virtualColumns,
        Granularity gran,
        boolean descending,
        @Nullable QueryMetrics<?> queryMetrics
    )
    {
      return Sequences.simple(Collections.singletonList(new Cursor()
      {
        private ColumnSelectorFactory columnSelectorFactory = RowBasedColumnSelectorFactory.create(() -> null, null);

        @Override
        public ColumnSelectorFactory getColumnSelectorFactory()
        {
          return columnSelectorFactory;
        }

        @Override
        public DateTime getTime()
        {
          return null;
        }

        @Override
        public void advance()
        {

        }

        @Override
        public void advanceUninterruptibly()
        {

        }

        @Override
        public void advanceTo(int offset)
        {

        }

        @Override
        public boolean isDone()
        {
          return true;
        }

        @Override
        public boolean isDoneOrInterrupted()
        {
          return true;
        }

        @Override
        public void reset()
        {

        }
      }));
    }
  }
}
