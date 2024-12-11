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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryEngine;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class CursorHolderPreaggTest extends InitializedNullHandlingTest
{
  private CloseableStupidPool<ByteBuffer> bufferPool;
  private GroupingEngine groupingEngine;
  private TopNQueryEngine topNQueryEngine;
  private TimeseriesQueryEngine timeseriesQueryEngine;

  private CursorFactory cursorFactory;
  private Segment segment;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  @Before
  public void setup()
  {
    bufferPool = closer.closeLater(
        new CloseableStupidPool<>(
            "CursorHolderPreaggTest-bufferPool",
            () -> ByteBuffer.allocate(50000)
        )
    );
    topNQueryEngine = new TopNQueryEngine(bufferPool);
    timeseriesQueryEngine = new TimeseriesQueryEngine(bufferPool);
    CloseableDefaultBlockingPool<ByteBuffer> mergePool =
        new CloseableDefaultBlockingPool<>(
            () -> ByteBuffer.allocate(50000),
            4
        );
    GroupByStatsProvider groupByStatsProvider = new GroupByStatsProvider();
    groupingEngine = new GroupingEngine(
        new DruidProcessingConfig(),
        GroupByQueryConfig::new,
        new GroupByResourcesReservationPool(
            closer.closeLater(mergePool),
            new GroupByQueryConfig()
        ),
        TestHelper.makeJsonMapper(),
        TestHelper.makeSmileMapper(),
        (query, future) -> {
        },
        groupByStatsProvider
    );

    this.cursorFactory = new CursorFactory()
    {
      private final RowSignature rowSignature = RowSignature.builder()
                                                            .add("a", ColumnType.STRING)
                                                            .add("b", ColumnType.STRING)
                                                            .add("cnt", ColumnType.LONG)
                                                            .build();

      private final Pair<Cursor, Closeable> cursorAndCloser = IterableRowsCursorHelper.getCursorFromIterable(
          ImmutableList.of(
              new Object[]{"a", "aa", 5L},
              new Object[]{"a", "aa", 6L},
              new Object[]{"b", "bb", 7L}
          ),
          rowSignature
      );

      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
      {
        return new CursorHolder()
        {
          @Nullable
          @Override
          public Cursor asCursor()
          {
            return cursorAndCloser.lhs;
          }

          @Override
          public boolean isPreAggregated()
          {
            return true;
          }

          @Nullable
          @Override
          public List<AggregatorFactory> getAggregatorsForPreAggregated()
          {
            return spec.getAggregators()
                       .stream().map(AggregatorFactory::getCombiningFactory)
                       .collect(Collectors.toList());
          }

          @Override
          public void close()
          {
            CloseableUtils.closeAndWrapExceptions(cursorAndCloser.rhs);
          }
        };
      }

      @Override
      public RowSignature getRowSignature()
      {
        return rowSignature;
      }

      @Override
      @Nullable
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return rowSignature.getColumnCapabilities(column);
      }
    };

    segment = new Segment()
    {
      @Override
      public SegmentId getId()
      {
        return SegmentId.dummy("test");
      }

      @Override
      public Interval getDataInterval()
      {
        return Intervals.ETERNITY;
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return null;
      }

      @Override
      public CursorFactory asCursorFactory()
      {
        return cursorFactory;
      }

      @Override
      public void close()
      {

      }
    };
  }

  @Test
  public void testTopn()
  {
    final TopNQuery topNQuery = new TopNQueryBuilder().dataSource("test")
                                                      .granularity(Granularities.ALL)
                                                      .intervals(ImmutableList.of(Intervals.ETERNITY))
                                                      .dimension("a")
                                                      .aggregators(new CountAggregatorFactory("cnt"))
                                                      .metric("cnt")
                                                      .threshold(10)
                                                      .build();
    Sequence<Result<TopNResultValue>> results = topNQueryEngine.query(
        topNQuery,
        segment,
        null
    );

    List<Result<TopNResultValue>> rows = results.toList();
    Assert.assertEquals(1, rows.size());
    // the cnt column is treated as pre-aggregated, so the values of the rows are summed
    Assert.assertEquals(2, rows.get(0).getValue().getValue().size());
    Assert.assertEquals(11L, rows.get(0).getValue().getValue().get(0).getLongMetric("cnt").longValue());
    Assert.assertEquals(7L, rows.get(0).getValue().getValue().get(1).getLongMetric("cnt").longValue());
  }

  @Test
  public void testGroupBy()
  {
    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(Intervals.ETERNITY)
                                           .addDimension("a")
                                           .addDimension("b")
                                           .addAggregator(new CountAggregatorFactory("cnt"))
                                           .build();

    Sequence<ResultRow> results = groupingEngine.process(
        query,
        cursorFactory,
        null,
        bufferPool,
        null
    );
    List<ResultRow> rows = results.toList();
    Assert.assertEquals(2, rows.size());
    // the cnt column is treated as pre-aggregated, so the values of the rows are summed
    Assert.assertArrayEquals(new Object[]{"a", "aa", 11L}, rows.get(0).getArray());
    Assert.assertArrayEquals(new Object[]{"b", "bb", 7L}, rows.get(1).getArray());
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery timeseriesQuery = Druids.newTimeseriesQueryBuilder()
                                            .dataSource("test")
                                            .intervals(ImmutableList.of(Intervals.ETERNITY))
                                            .granularity(Granularities.ALL)
                                            .aggregators(new CountAggregatorFactory("cnt"))
                                            .build();
    Sequence<Result<TimeseriesResultValue>> results = timeseriesQueryEngine.process(
        timeseriesQuery,
        cursorFactory,
        null,
        null
    );
    List<Result<TimeseriesResultValue>> rows = results.toList();
    Assert.assertEquals(1, rows.size());
    // the cnt column is treated as pre-aggregated, so the values of the rows are summed
    Assert.assertEquals(18L, (long) rows.get(0).getValue().getLongMetric("cnt"));
  }
}
