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

package org.apache.druid.segment.virtual;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DummyStringVirtualColumnTest extends InitializedNullHandlingTest
{
  private static final String VSTRING_DIM = "vstring";
  private static final String COUNT = "count";

  private final List<Segment> mmappedSegments;
  private final List<Segment> inMemorySegments;
  private final List<Segment> mixedSegments;

  private final AggregationTestHelper topNTestHelper;
  private final AggregationTestHelper groupByTestHelper;

  public DummyStringVirtualColumnTest()
  {
    QueryableIndexSegment queryableIndexSegment = new QueryableIndexSegment(
        TestIndex.getMMappedTestIndex(),
        SegmentId.dummy(QueryRunnerTestHelper.DATA_SOURCE)
    );
    IncrementalIndexSegment incrementalIndexSegment = new IncrementalIndexSegment(
        TestIndex.getIncrementalTestIndex(),
        SegmentId.dummy(QueryRunnerTestHelper.DATA_SOURCE)
    );

    mmappedSegments = Lists.newArrayList(queryableIndexSegment, queryableIndexSegment);
    inMemorySegments = Lists.newArrayList(incrementalIndexSegment, incrementalIndexSegment);
    mixedSegments = Lists.newArrayList(incrementalIndexSegment, queryableIndexSegment);

    topNTestHelper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        Collections.emptyList(),
        null
    );

    groupByTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Collections.emptyList(),
        new GroupByQueryConfig(),
        null
    );
  }

  @Test
  public void testGroupByWithMMappedSegments()
  {
    testGroupBy(mmappedSegments, true, true);
    testGroupBy(mmappedSegments, true, false);
    testGroupBy(mmappedSegments, false, true);
  }

  @Test
  public void testGroupByWithInMemorySegments()
  {
    testGroupBy(inMemorySegments, true, true);
    testGroupBy(inMemorySegments, true, false);

    try {
      testGroupBy(inMemorySegments, false, true);
      Assert.fail("must need row based methods");
    }
    catch (Exception ex) {
    }
  }

  @Test
  public void testGroupByWithMixedSegments()
  {
    testGroupBy(mixedSegments, true, true);
    testGroupBy(mixedSegments, true, false);

    try {
      testGroupBy(mixedSegments, false, true);
      Assert.fail("must need row based methods");
    }
    catch (Exception ex) {
    }
  }

  @Test
  public void testGroupByWithSelectFilterWithMMappedSegments()
  {
    testGroupByWithSelectFilter(mmappedSegments, true, false, false, false);
    testGroupByWithSelectFilter(mmappedSegments, true, false, true, true);
    testGroupByWithSelectFilter(mmappedSegments, false, true, true, true);
    testGroupByWithSelectFilter(mmappedSegments, true, true, true, false);
  }

  @Test
  public void testGroupByWithSelectFilterWithInMemorySegments()
  {
    testGroupByWithSelectFilter(inMemorySegments, true, false, false, false);
    testGroupByWithSelectFilter(inMemorySegments, true, true, true, false);

    try {
      testGroupByWithSelectFilter(inMemorySegments, true, true, true, true);
      Assert.fail("value matchers must be required");
    }
    catch (Exception ex) {

    }
  }

  @Test
  public void testGroupByWithSelectFilterWithMixedSegments()
  {
    testGroupByWithSelectFilter(mixedSegments, true, false, false, false);
    testGroupByWithSelectFilter(mixedSegments, true, true, true, false);

    try {
      testGroupByWithSelectFilter(mixedSegments, true, true, true, true);
      Assert.fail("value matchers must be required");
    }
    catch (Exception ex) {

    }
  }

  @Test
  public void testGroupByWithRegexFilterWithMMappedSegments()
  {
    testGroupByWithRegexFilter(mmappedSegments, true, false, false, false);
    testGroupByWithRegexFilter(mmappedSegments, true, false, true, true);
    testGroupByWithRegexFilter(mmappedSegments, false, true, true, true);
    testGroupByWithRegexFilter(mmappedSegments, true, true, true, false);
  }

  @Test
  public void testGroupByWithRegexFilterWithInMemorySegments()
  {
    testGroupByWithRegexFilter(inMemorySegments, true, false, false, false);
    testGroupByWithRegexFilter(inMemorySegments, true, true, true, false);

    try {
      testGroupByWithRegexFilter(inMemorySegments, true, true, true, true);
      Assert.fail("value matchers must be required");
    }
    catch (Exception ex) {

    }
  }

  @Test
  public void testGroupByWithRegexFilterWithMixedSegments()
  {
    testGroupByWithRegexFilter(mixedSegments, true, false, false, false);
    testGroupByWithRegexFilter(mixedSegments, true, true, true, false);

    try {
      testGroupByWithRegexFilter(mixedSegments, true, true, true, true);
      Assert.fail("value matchers must be required");
    }
    catch (Exception ex) {

    }
  }

  @Test
  public void testTopNWithMMappedSegments()
  {
    testTopN(mmappedSegments, true, true);
    testTopN(mmappedSegments, true, false);
    testTopN(mmappedSegments, false, true);
  }

  @Test
  public void testTopNWithInMemorySegments()
  {
    testTopN(inMemorySegments, true, true);
    testTopN(inMemorySegments, true, false);

    try {
      testTopN(inMemorySegments, false, true);
      Assert.fail("must need row based methods");
    }
    catch (Exception ex) {
    }
  }

  @Test
  public void testTopNWithMixedSegments()
  {
    testTopN(mixedSegments, true, true);
    testTopN(mixedSegments, true, false);

    try {
      testTopN(mixedSegments, false, true);
      Assert.fail("must need row based methods");
    }
    catch (Exception ex) {
    }
  }

  private void testGroupBy(List<Segment> segments, boolean enableRowBasedMethods, boolean enableColumnBasedMethods)
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(
            new DummyStringVirtualColumn(QueryRunnerTestHelper.MARKET_DIMENSION, VSTRING_DIM,
                                         enableRowBasedMethods, enableColumnBasedMethods,
                                         false, true
            )
        )
        .addDimension(VSTRING_DIM)
        .setAggregatorSpecs(new CountAggregatorFactory(COUNT))
        .setInterval("2000/2030")
        .addOrderByColumn(VSTRING_DIM)
        .build();

    List<ResultRow> rows = groupByTestHelper.runQueryOnSegmentsObjs(segments, query).toList();

    List<ResultRow> expectedRows = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 1674L, VSTRING_DIM, "spot"),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 372L, VSTRING_DIM, "total_market"),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 372L, VSTRING_DIM, "upfront")
    );

    TestHelper.assertExpectedObjects(expectedRows, rows, "failed");
  }

  private void testGroupByWithSelectFilter(
      List<Segment> segments,
      boolean enableRowBasedMethods,
      boolean enableColumnBasedMethods,
      boolean enableBitmaps,
      boolean disableValueMatchers
  )
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(
            new DummyStringVirtualColumn(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                VSTRING_DIM,
                enableRowBasedMethods,
                enableColumnBasedMethods,
                enableBitmaps,
                disableValueMatchers
            )
        )
        .addDimension(VSTRING_DIM)
        .setAggregatorSpecs(new CountAggregatorFactory(COUNT))
        .setInterval("2000/2030")
        .addOrderByColumn(VSTRING_DIM)
        .setDimFilter(new SelectorDimFilter(VSTRING_DIM, "spot", null))
        .build();

    List<ResultRow> rows = groupByTestHelper.runQueryOnSegmentsObjs(segments, query).toList();

    List<ResultRow> expectedRows = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 1674L, VSTRING_DIM, "spot")
    );

    TestHelper.assertExpectedObjects(expectedRows, rows, "failed");
  }

  private void testGroupByWithRegexFilter(
      List<Segment> segments,
      boolean enableRowBasedMethods,
      boolean enableColumnBasedMethods,
      boolean enableBitmaps,
      boolean disableValueMatchers
  )
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setVirtualColumns(
            new DummyStringVirtualColumn(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                VSTRING_DIM,
                enableRowBasedMethods,
                enableColumnBasedMethods,
                enableBitmaps,
                disableValueMatchers
            )
        )
        .addDimension(VSTRING_DIM)
        .setAggregatorSpecs(new CountAggregatorFactory(COUNT))
        .setInterval("2000/2030")
        .addOrderByColumn(VSTRING_DIM)
        .setDimFilter(new RegexDimFilter(VSTRING_DIM, "(spot)|(upfront)", null))
        .build();

    List<ResultRow> rows = groupByTestHelper.runQueryOnSegmentsObjs(segments, query).toList();

    List<ResultRow> expectedRows = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 1674L, VSTRING_DIM, "spot"),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "2000", COUNT, 372L, VSTRING_DIM, "upfront")
    );

    TestHelper.assertExpectedObjects(expectedRows, rows, "failed");
  }

  private void testTopN(
      List<Segment> segments,
      boolean enableRowBasedMethods,
      boolean enableColumnBasedMethods
  )
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension(VSTRING_DIM)
        .metric(COUNT)
        .threshold(1)
        .aggregators(new CountAggregatorFactory(COUNT))
        .virtualColumns(new DummyStringVirtualColumn(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            VSTRING_DIM,
            enableRowBasedMethods,
            enableColumnBasedMethods,
            false,
            true
        ))
        .intervals("2000/2030")
        .build();

    List rows = topNTestHelper.runQueryOnSegmentsObjs(segments, query).toList();

    List<Result<TopNResultValue>> expectedRows = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.<String, Object>builder()
                        .put(COUNT, 1674L)
                        .put(VSTRING_DIM, "spot")
                        .build()
                )
            )
        )
    );

    TestHelper.assertExpectedResults(expectedRows, (List<Result<TopNResultValue>>) rows, "failed");
  }
}
