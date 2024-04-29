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
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MapVirtualColumnGroupByTest extends InitializedNullHandlingTest
{
  private QueryRunner<ResultRow> runner;

  @Before
  public void setup() throws IOException
  {
    final IncrementalIndex incrementalIndex = MapVirtualColumnTestBase.generateIndex();
    final GroupByQueryConfig config = new GroupByQueryConfig();
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(new DefaultBlockingPool<>(() -> ByteBuffer.allocate(1024), 1), config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return null;
          }

          @Override
          public int intermediateComputeSizeBytes()
          {
            return 10 * 1024 * 1024;
          }

          @Override
          public int getNumMergeBuffers()
          {
            return 1;
          }

          @Override
          public int getNumThreads()
          {
            return 1;
          }
        },
        () -> config,
        new StupidPool<>("map-virtual-column-groupby-test", () -> ByteBuffer.allocate(1024)),
        groupByResourcesReservationPool,
        TestHelper.makeJsonMapper(),
        new DefaultObjectMapper(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        groupingEngine,
        new GroupByQueryQueryToolChest(groupingEngine, groupByResourcesReservationPool)
    );

    runner = QueryRunnerTestHelper.makeQueryRunner(
        factory,
        SegmentId.dummy("index"),
        new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("index")),
        "incremental"
    );
  }

  @Test
  public void testWithMapColumn()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params", "params")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> runner.run(QueryPlus.wrap(query)).toList()
    );
    Assert.assertEquals("Unable to group on the column[params]", t.getMessage());
  }


  @Test
  public void testWithSubColumn()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params.key3", "params.key3")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final List<ResultRow> result = runner.run(QueryPlus.wrap(query)).toList();
    final List<ResultRow> expected = ImmutableList.of(
        new MapBasedRow(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            MapVirtualColumnTestBase.mapOf("count", 1L, "params.key3", "value3")
        ),
        new MapBasedRow(DateTimes.of("2011-01-12T00:00:00.000Z"), MapVirtualColumnTestBase.mapOf("count", 2L))
    ).stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testWithSubColumnWithFilter()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        new EqualityFilter("params.key3", ColumnType.STRING, "value3", null),
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params.key3", "params.key3")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final List<ResultRow> result = runner.run(QueryPlus.wrap(query)).toList();
    final List<ResultRow> expected = ImmutableList.of(
        new MapBasedRow(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            MapVirtualColumnTestBase.mapOf("count", 1L, "params.key3", "value3")
        )
    ).stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testWithSubColumnWithPredicateFilter()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        new InDimFilter("params.key3", ImmutableList.of("value1", "value3"), null),
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params.key3", "params.key3")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final List<ResultRow> result = runner.run(QueryPlus.wrap(query)).toList();
    final List<ResultRow> expected = ImmutableList.of(
        new MapBasedRow(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            MapVirtualColumnTestBase.mapOf("count", 1L, "params.key3", "value3")
        )
    ).stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testWithSubColumnWithNotFilter()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        NotDimFilter.of(new EqualityFilter("params.key3", ColumnType.STRING, "value3", null)),
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params.key3", "params.key3")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final List<ResultRow> result = runner.run(QueryPlus.wrap(query)).toList();
    final List<ResultRow> expected;
    if (NullHandling.sqlCompatible()) {
      expected = Collections.emptyList();
    } else {
      expected = ImmutableList.of(
          new MapBasedRow(DateTimes.of("2011-01-12T00:00:00.000Z"), MapVirtualColumnTestBase.mapOf("count", 2L))
      ).stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());
    }

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testWithSubColumnWithNotPredicateFilter()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2011/2012"))),
        VirtualColumns.create(ImmutableList.of(new MapVirtualColumn("keys", "values", "params"))),
        NotDimFilter.of(new InDimFilter("params.key3", ImmutableList.of("value1", "value3"), null)),
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("params.key3", "params.key3")),
        ImmutableList.of(new CountAggregatorFactory("count")),
        null,
        null,
        null,
        null,
        null
    );

    final List<ResultRow> result = runner.run(QueryPlus.wrap(query)).toList();
    final List<ResultRow> expected;
    if (NullHandling.sqlCompatible()) {
      expected = Collections.emptyList();
    } else {
      expected = ImmutableList.of(
          new MapBasedRow(DateTimes.of("2011-01-12T00:00:00.000Z"), MapVirtualColumnTestBase.mapOf("count", 2L))
      ).stream().map(row -> ResultRow.fromLegacyRow(row, query)).collect(Collectors.toList());
    }

    Assert.assertEquals(expected, result);
  }
}
