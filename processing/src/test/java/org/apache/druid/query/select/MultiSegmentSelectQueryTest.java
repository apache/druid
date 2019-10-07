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

package org.apache.druid.query.select;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnionQueryRunner;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class MultiSegmentSelectQueryTest
{
  private static final Supplier<SelectQueryConfig> CONFIG_SUPPLIER = Suppliers.ofInstance(new SelectQueryConfig(true));

  private static final SelectQueryQueryToolChest TOOL_CHEST = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
  );

  private static final QueryRunnerFactory FACTORY = new SelectQueryRunnerFactory(
      TOOL_CHEST,
      new SelectQueryEngine(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  // time modified version of druid.sample.numeric.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t100.000000",
      "2011-01-12T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t100.000000",
      "2011-01-12T03:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t100.000000",
      "2011-01-12T04:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t100.000000",
      "2011-01-12T05:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t100.000000",
      "2011-01-12T06:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t100.000000",
      "2011-01-12T07:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T08:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T09:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1000.000000",
      "2011-01-12T10:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1000.000000",
      "2011-01-12T11:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t800.000000\tvalue",
      "2011-01-12T12:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t800.000000\tvalue"
  };

  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-13T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-13T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-13T03:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t114.947403",
      "2011-01-13T04:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t104.465767",
      "2011-01-13T05:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t102.851683",
      "2011-01-13T06:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t108.863011",
      "2011-01-13T07:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t111.356672",
      "2011-01-13T08:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t106.236928",
      "2011-01-13T09:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1040.945505",
      "2011-01-13T10:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1689.012875",
      "2011-01-13T11:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t826.060182\tvalue",
      "2011-01-13T12:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1564.617729\tvalue"
  };

  public static final String[] V_OVERRIDE = {
      "2011-01-12T04:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t999.000000",
      "2011-01-12T05:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t999.000000",
      "2011-01-12T06:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t999.000000",
      "2011-01-12T07:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t999.000000"
  };

  private static Segment segment0;
  private static Segment segment1;
  private static Segment segment_override;  // this makes segment0 split into three logical segments

  private static List<String> segmentIdentifiers;

  private static QueryRunner runner;

  @BeforeClass
  public static void setup() throws IOException
  {
    CharSource v_0112 = CharSource.wrap(StringUtils.join(V_0112, "\n"));
    CharSource v_0113 = CharSource.wrap(StringUtils.join(V_0113, "\n"));
    CharSource v_override = CharSource.wrap(StringUtils.join(V_OVERRIDE, "\n"));

    IncrementalIndex index0 = TestIndex.loadIncrementalIndex(newIndex("2011-01-12T00:00:00.000Z"), v_0112);
    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(newIndex("2011-01-13T00:00:00.000Z"), v_0113);
    IncrementalIndex index2 = TestIndex.loadIncrementalIndex(newIndex("2011-01-12T04:00:00.000Z"), v_override);

    segment0 = new IncrementalIndexSegment(index0, makeIdentifier(index0, "v1"));
    segment1 = new IncrementalIndexSegment(index1, makeIdentifier(index1, "v1"));
    segment_override = new IncrementalIndexSegment(index2, makeIdentifier(index2, "v2"));

    VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = new VersionedIntervalTimeline<>(StringComparators.LEXICOGRAPHIC);
    timeline.add(
        index0.getInterval(),
        "v1",
        new SingleElementPartitionChunk<>(ReferenceCountingSegment.wrapRootGenerationSegment(segment0))
    );
    timeline.add(
        index1.getInterval(),
        "v1",
        new SingleElementPartitionChunk<>(ReferenceCountingSegment.wrapRootGenerationSegment(segment1))
    );
    timeline.add(
        index2.getInterval(),
        "v2",
        new SingleElementPartitionChunk<>(ReferenceCountingSegment.wrapRootGenerationSegment(segment_override))
    );

    segmentIdentifiers = new ArrayList<>();
    for (TimelineObjectHolder<String, ?> holder : timeline.lookup(Intervals.of("2011-01-12/2011-01-14"))) {
      segmentIdentifiers.add(makeIdentifier(holder.getInterval(), holder.getVersion()).toString());
    }

    runner = QueryRunnerTestHelper.makeFilteringQueryRunner(timeline, FACTORY);
  }

  private static SegmentId makeIdentifier(IncrementalIndex index, String version)
  {
    return makeIdentifier(index.getInterval(), version);
  }

  private static SegmentId makeIdentifier(Interval interval, String version)
  {
    return SegmentId.of(QueryRunnerTestHelper.DATA_SOURCE, interval, version, NoneShardSpec.instance());
  }

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of(minTimeStamp).getMillis())
        .withQueryGranularity(Granularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRowCount)
        .buildOnheap();
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
    IOUtils.closeQuietly(segment_override);
  }

  @Parameterized.Parameters(name = "fromNext={0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(false, true));
  }

  private final boolean fromNext;

  public MultiSegmentSelectQueryTest(boolean fromNext)
  {
    this.fromNext = fromNext;
  }

  private Druids.SelectQueryBuilder newBuilder()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE))
                 .intervals(SelectQueryRunnerTest.I_0112_0114_SPEC)
                 .granularity(QueryRunnerTestHelper.ALL_GRAN)
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.DIMENSIONS))
                 .pagingSpec(PagingSpec.newSpec(3));
  }

  @Test
  public void testAllGranularity()
  {
    runAllGranularityTest(
        newBuilder().build(),
        new int[][]{
            {2, -1, -1, -1, 3}, {3, 1, -1, -1, 3}, {-1, 3, 0, -1, 3}, {-1, -1, 3, -1, 3}, {-1, -1, 4, 1, 3},
            {-1, -1, -1, 4, 3}, {-1, -1, -1, 7, 3}, {-1, -1, -1, 10, 3}, {-1, -1, -1, 12, 2}, {-1, -1, -1, 13, 0}
        }
    );

    runAllGranularityTest(
        newBuilder().descending(true).build(),
        new int[][]{
            {0, 0, 0, -3, 3}, {0, 0, 0, -6, 3}, {0, 0, 0, -9, 3}, {0, 0, 0, -12, 3}, {0, 0, -2, -13, 3},
            {0, 0, -5, 0, 3}, {0, -3, 0, 0, 3}, {-2, -4, 0, 0, 3}, {-4, 0, 0, 0, 2}, {-5, 0, 0, 0, 0}
        }
    );
  }

  private void runAllGranularityTest(SelectQuery query, int[][] expectedOffsets)
  {
    for (int[] expected : expectedOffsets) {
      List<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
      Assert.assertEquals(1, results.size());

      SelectResultValue value = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = value.getPagingIdentifiers();

      Map<String, Integer> merged = PagingSpec.merge(Collections.singletonList(pagingIdentifiers));

      for (int i = 0; i < 4; i++) {
        if (query.isDescending() ^ expected[i] >= 0) {
          Assert.assertEquals(expected[i], pagingIdentifiers.get(segmentIdentifiers.get(i)).intValue());
        }
      }
      Assert.assertEquals(expected[4], value.getEvents().size());

      query = query.withPagingSpec(toNextCursor(merged, query, 3));
    }
  }

  @Test
  public void testDayGranularity()
  {
    runDayGranularityTest(
        newBuilder().granularity(QueryRunnerTestHelper.DAY_GRAN).build(),
        new int[][]{
            {2, -1, -1, 2, 3, 0, 0, 3}, {3, 1, -1, 5, 1, 2, 0, 3}, {-1, 3, 0, 8, 0, 2, 1, 3},
            {-1, -1, 3, 11, 0, 0, 3, 3}, {-1, -1, 4, 12, 0, 0, 1, 1}, {-1, -1, 5, 13, 0, 0, 0, 0}
        }
    );

    runDayGranularityTest(
        newBuilder().granularity(QueryRunnerTestHelper.DAY_GRAN).descending(true).build(),
        new int[][]{
            {0, 0, -3, -3, 0, 0, 3, 3}, {0, -1, -5, -6, 0, 1, 2, 3}, {0, -4, 0, -9, 0, 3, 0, 3},
            {-3, 0, 0, -12, 3, 0, 0, 3}, {-4, 0, 0, -13, 1, 0, 0, 1}, {-5, 0, 0, -14, 0, 0, 0, 0}
        }
    );
  }

  private void runDayGranularityTest(SelectQuery query, int[][] expectedOffsets)
  {
    for (int[] expected : expectedOffsets) {
      List<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
      Assert.assertEquals(2, results.size());

      SelectResultValue value0 = results.get(0).getValue();
      SelectResultValue value1 = results.get(1).getValue();

      Map<String, Integer> pagingIdentifiers0 = value0.getPagingIdentifiers();
      Map<String, Integer> pagingIdentifiers1 = value1.getPagingIdentifiers();

      Map<String, Integer> merged = PagingSpec.merge(Arrays.asList(pagingIdentifiers0, pagingIdentifiers1));

      for (int i = 0; i < 4; i++) {
        if (query.isDescending() ^ expected[i] >= 0) {
          Assert.assertEquals(expected[i], merged.get(segmentIdentifiers.get(i)).intValue());
        }
      }

      query = query.withPagingSpec(toNextCursor(merged, query, 3));
    }
  }

  @Test
  public void testPagingIdentifiersForUnionDatasource()
  {
    Druids.SelectQueryBuilder selectQueryBuilder = Druids
        .newSelectQueryBuilder()
        .dataSource(
            new UnionDataSource(
                ImmutableList.of(
                    new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
                    new TableDataSource("testing-2")
                )
            )
        )
        .intervals(SelectQueryRunnerTest.I_0112_0114_SPEC)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.DIMENSIONS))
        .pagingSpec(PagingSpec.newSpec(3));

    SelectQuery query = selectQueryBuilder.build();
    QueryRunner unionQueryRunner = new UnionQueryRunner(runner);

    List<Result<SelectResultValue>> results = unionQueryRunner.run(QueryPlus.wrap(query)).toList();

    Map<String, Integer> pagingIdentifiers = results.get(0).getValue().getPagingIdentifiers();
    query = query.withPagingSpec(toNextCursor(PagingSpec.merge(Collections.singletonList(pagingIdentifiers)), query, 3));

    unionQueryRunner.run(QueryPlus.wrap(query)).toList();
  }

  private PagingSpec toNextCursor(Map<String, Integer> merged, SelectQuery query, int threshold)
  {
    if (!fromNext) {
      merged = PagingSpec.next(merged, query.isDescending());
    }
    return new PagingSpec(merged, threshold, fromNext);
  }
}
