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
import com.google.common.io.CharSource;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
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

  // time modified version of druid.sample.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
      "2011-01-12T01:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
      "2011-01-12T02:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
      "2011-01-12T03:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
      "2011-01-12T04:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
      "2011-01-12T05:00:00.000Z	spot	news	preferred	npreferred	100.000000",
      "2011-01-12T06:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
      "2011-01-12T07:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
      "2011-01-12T08:00:00.000Z	spot	travel	preferred	tpreferred	100.000000",
      "2011-01-12T09:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1000.000000",
      "2011-01-12T10:00:00.000Z	total_market	premium	preferred	ppreferred	1000.000000",
      "2011-01-12T11:00:00.000Z	upfront	mezzanine	preferred	mpreferred	800.000000	value",
      "2011-01-12T12:00:00.000Z	upfront	premium	preferred	ppreferred	800.000000	value"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
      "2011-01-13T01:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
      "2011-01-13T02:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
      "2011-01-13T03:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
      "2011-01-13T04:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
      "2011-01-13T05:00:00.000Z	spot	news	preferred	npreferred	102.851683",
      "2011-01-13T06:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
      "2011-01-13T07:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
      "2011-01-13T08:00:00.000Z	spot	travel	preferred	tpreferred	106.236928",
      "2011-01-13T09:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1040.945505",
      "2011-01-13T10:00:00.000Z	total_market	premium	preferred	ppreferred	1689.012875",
      "2011-01-13T11:00:00.000Z	upfront	mezzanine	preferred	mpreferred	826.060182	value",
      "2011-01-13T12:00:00.000Z	upfront	premium	preferred	ppreferred	1564.617729	value"
  };

  public static final String[] V_OVERRIDE = {
      "2011-01-12T04:00:00.000Z	spot	automotive	preferred	apreferred	999.000000",
      "2011-01-12T05:00:00.000Z	spot	business	preferred	bpreferred	999.000000",
      "2011-01-12T06:00:00.000Z	spot	entertainment	preferred	epreferred	999.000000",
      "2011-01-12T07:00:00.000Z	spot	health	preferred	hpreferred	999.000000"
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

    VersionedIntervalTimeline<String, Segment> timeline = new VersionedIntervalTimeline(StringComparators.LEXICOGRAPHIC);
    timeline.add(index0.getInterval(), "v1", new SingleElementPartitionChunk(segment0));
    timeline.add(index1.getInterval(), "v1", new SingleElementPartitionChunk(segment1));
    timeline.add(index2.getInterval(), "v2", new SingleElementPartitionChunk(segment_override));

    segmentIdentifiers = Lists.newArrayList();
    for (TimelineObjectHolder<String, ?> holder : timeline.lookup(new Interval("2011-01-12/2011-01-14"))) {
      segmentIdentifiers.add(makeIdentifier(holder.getInterval(), holder.getVersion()));
    }

    runner = QueryRunnerTestHelper.makeFilteringQueryRunner(timeline, factory);
  }

  private static String makeIdentifier(IncrementalIndex index, String version)
  {
    return makeIdentifier(index.getInterval(), version);
  }

  private static String makeIdentifier(Interval interval, String version)
  {
    return DataSegment.makeDataSegmentIdentifier(
        QueryRunnerTestHelper.dataSource,
        interval.getStart(),
        interval.getEnd(),
        version,
        NoneShardSpec.instance()
    );
  }

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime(minTimeStamp).getMillis())
        .withQueryGranularity(QueryGranularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new OnheapIncrementalIndex(schema, true, maxRowCount);
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
    IOUtils.closeQuietly(segment_override);
  }

  @Parameterized.Parameters(name = "fromNext={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
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
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                 .intervals(SelectQueryRunnerTest.I_0112_0114)
                 .granularity(QueryRunnerTestHelper.allGran)
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.dimensions))
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
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );
      Assert.assertEquals(1, results.size());

      SelectResultValue value = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = value.getPagingIdentifiers();

      Map<String, Integer> merged = PagingSpec.merge(Arrays.asList(pagingIdentifiers));

      for (int i = 0; i < 4; i++) {
        if (query.isDescending() ^ expected[i] >= 0) {
          Assert.assertEquals(
              expected[i], pagingIdentifiers.get(segmentIdentifiers.get(i)).intValue()
          );
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
        newBuilder().granularity(QueryRunnerTestHelper.dayGran).build(),
        new int[][]{
            {2, -1, -1, 2, 3, 0, 0, 3}, {3, 1, -1, 5, 1, 2, 0, 3}, {-1, 3, 0, 8, 0, 2, 1, 3},
            {-1, -1, 3, 11, 0, 0, 3, 3}, {-1, -1, 4, 12, 0, 0, 1, 1}, {-1, -1, 5, 13, 0, 0, 0, 0}
        }
    );

    runDayGranularityTest(
        newBuilder().granularity(QueryRunnerTestHelper.dayGran).descending(true).build(),
        new int[][]{
            {0, 0, -3, -3, 0, 0, 3, 3}, {0, -1, -5, -6, 0, 1, 2, 3}, {0, -4, 0, -9, 0, 3, 0, 3},
            {-3, 0, 0, -12, 3, 0, 0, 3}, {-4, 0, 0, -13, 1, 0, 0, 1}, {-5, 0, 0, -14, 0, 0, 0, 0}
        }
    );
  }

  private void runDayGranularityTest(SelectQuery query, int[][] expectedOffsets)
  {
    for (int[] expected : expectedOffsets) {
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );
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

  private PagingSpec toNextCursor(Map<String, Integer> merged, SelectQuery query, int threshold)
  {
    if (!fromNext) {
      merged = PagingSpec.next(merged, query.isDescending());
    }
    return new PagingSpec(merged, threshold, fromNext);
  }
}
