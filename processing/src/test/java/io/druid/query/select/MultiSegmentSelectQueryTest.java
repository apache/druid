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

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharSource;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class MultiSegmentSelectQueryTest
{
  private static final SelectQueryQueryToolChest toolChest = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  private static final QueryRunnerFactory factory = new SelectQueryRunnerFactory(
      toolChest,
      new SelectQueryEngine(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  private static Segment segment0;
  private static Segment segment1;

  private static QueryRunner runner;

  @BeforeClass
  public static void setup() throws IOException
  {
    CharSource v_0112 = CharSource.wrap(StringUtils.join(SelectQueryRunnerTest.V_0112, "\n"));
    CharSource v_0113 = CharSource.wrap(StringUtils.join(SelectQueryRunnerTest.V_0113, "\n"));

    IncrementalIndex index0 = TestIndex.loadIncrementalIndex(newIncrementalIndex("2011-01-12T00:00:00.000Z"), v_0112);
    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(newIncrementalIndex("2011-01-13T00:00:00.000Z"), v_0113);

    segment0 = new IncrementalIndexSegment(index0, makeIdentifier(index0));
    segment1 = new IncrementalIndexSegment(index1, makeIdentifier(index1));

    runner = QueryRunnerTestHelper.makeFilteringQueryRunner(Arrays.asList(segment0, segment1), factory);
  }

  private static String makeIdentifier(IncrementalIndex index)
  {
    Interval interval = index.getInterval();
    return DataSegment.makeDataSegmentIdentifier(
        QueryRunnerTestHelper.dataSource,
        interval.getStart(),
        interval.getEnd(),
        "v",
        new NoneShardSpec()
    );
  }

  private static IncrementalIndex newIncrementalIndex(String minTimeStamp) {
    return newIncrementalIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIncrementalIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime(minTimeStamp).getMillis())
        .withQueryGranularity(QueryGranularity.NONE)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new OnheapIncrementalIndex(schema, maxRowCount);
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
  }

  @Test
  public void testAllGranularity()
  {
    PagingSpec pagingSpec = new PagingSpec(null, 3);
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        SelectQueryRunnerTest.I_0112_0114,
        false,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.dimensions),
        Arrays.<String>asList(),
        pagingSpec,
        null
    );

    for (int[] expected : new int[][]{
        {2, 0, 3}, {5, 0, 3}, {8, 0, 3}, {11, 0, 3}, {12, 1, 3},
        {0, 4, 3}, {0, 7, 3}, {0, 10, 3}, {0, 12, 2}, {0, 13, 0}
    }) {
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );
      Assert.assertEquals(1, results.size());

      SelectResultValue value = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = value.getPagingIdentifiers();

      if (expected[0] != 0) {
        Assert.assertEquals(expected[0], pagingIdentifiers.get(segment0.getIdentifier()).intValue());
      }
      if (expected[1] != 0) {
        Assert.assertEquals(expected[1], pagingIdentifiers.get(segment1.getIdentifier()).intValue());
      }
      Assert.assertEquals(expected[2], value.getEvents().size());

      query = query.withPagingSpec(toNextPager(3, pagingIdentifiers));
    }
  }

  @Test
  public void testDayGranularity()
  {
    PagingSpec pagingSpec = new PagingSpec(null, 3);
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        SelectQueryRunnerTest.I_0112_0114,
        false,
        null,
        QueryRunnerTestHelper.dayGran,
        DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.dimensions),
        Arrays.<String>asList(),
        pagingSpec,
        null
    );

    for (int[] expected : new int[][]{
        {2, 2, 3, 3}, {5, 5, 3, 3}, {8, 8, 3, 3}, {11, 11, 3, 3}, {12, 12, 1, 1}
    }) {
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );
      Assert.assertEquals(2, results.size());

      SelectResultValue value0 = results.get(0).getValue();
      SelectResultValue value1 = results.get(1).getValue();

      Map<String, Integer> pagingIdentifiers0 = value0.getPagingIdentifiers();
      Map<String, Integer> pagingIdentifiers1 = value1.getPagingIdentifiers();

      Assert.assertEquals(1, pagingIdentifiers0.size());
      Assert.assertEquals(1, pagingIdentifiers1.size());

      if (expected[0] != 0) {
        Assert.assertEquals(expected[0], pagingIdentifiers0.get(segment0.getIdentifier()).intValue());
      }
      if (expected[1] != 0) {
        Assert.assertEquals(expected[1], pagingIdentifiers1.get(segment1.getIdentifier()).intValue());
      }
      Assert.assertEquals(expected[2], value0.getEvents().size());
      Assert.assertEquals(expected[3], value1.getEvents().size());

      query = query.withPagingSpec(toNextPager(3, pagingIdentifiers0, pagingIdentifiers1));
    }
  }

  @SafeVarargs
  private final PagingSpec toNextPager(int threshold, Map<String, Integer>... pagers)
  {
    LinkedHashMap<String, Integer> next = Maps.newLinkedHashMap();
    for (Map<String, Integer> pager : pagers) {
      for (Map.Entry<String, Integer> entry : pager.entrySet()) {
        next.put(entry.getKey(), entry.getValue() + 1);
      }
    }
    return new PagingSpec(next, threshold);
  }
}
