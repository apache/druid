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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.io.CharSource;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeBoundaryQueryRunnerTest
{
  @Parameterized.Parameters(name="{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER)
        )
    );
  }

  private final QueryRunner runner;
  private static final QueryRunnerFactory factory = new TimeBoundaryQueryRunnerFactory(
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );
  private static Segment segment0;
  private static Segment segment1;
  private static List<String> segmentIdentifiers;

  public TimeBoundaryQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  // Adapted from MultiSegmentSelectQueryTest, with modifications to make filtering meaningful
  public static final String[] V_0112 = {
      "2011-01-12T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t100.000000",
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t100.000000",
      "2011-01-13T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
  };
  public static final String[] V_0113 = {
      "2011-01-14T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-14T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-15T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-15T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-16T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-16T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-16T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-17T01:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-17T02:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
  };

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime(minTimeStamp).getMillis())
        .withQueryGranularity(Granularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRowCount)
        .buildOnheap();
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
        new NoneShardSpec()
    );
  }

  private QueryRunner getCustomRunner() throws IOException
  {
    CharSource v_0112 = CharSource.wrap(StringUtils.join(V_0112, "\n"));
    CharSource v_0113 = CharSource.wrap(StringUtils.join(V_0113, "\n"));

    IncrementalIndex index0 = TestIndex.loadIncrementalIndex(newIndex("2011-01-12T00:00:00.000Z"), v_0112);
    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(newIndex("2011-01-14T00:00:00.000Z"), v_0113);

    segment0 = new IncrementalIndexSegment(index0, makeIdentifier(index0, "v1"));
    segment1 = new IncrementalIndexSegment(index1, makeIdentifier(index1, "v1"));

    VersionedIntervalTimeline<String, Segment> timeline = new VersionedIntervalTimeline(StringComparators.LEXICOGRAPHIC);
    timeline.add(index0.getInterval(), "v1", new SingleElementPartitionChunk(segment0));
    timeline.add(index1.getInterval(), "v1", new SingleElementPartitionChunk(segment1));

    segmentIdentifiers = Lists.newArrayList();
    for (TimelineObjectHolder<String, ?> holder : timeline.lookup(new Interval("2011-01-12/2011-01-17"))) {
      segmentIdentifiers.add(makeIdentifier(holder.getInterval(), holder.getVersion()));
    }

    return QueryRunnerTestHelper.makeFilteringQueryRunner(timeline, factory);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFilteredTimeBoundaryQuery() throws IOException
  {
    QueryRunner customRunner = getCustomRunner();
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .filters("quality", "automotive")
                                                .build();
    Assert.assertTrue(timeBoundaryQuery.hasFilters());
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        customRunner.run(timeBoundaryQuery, context),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );

    Assert.assertTrue(Iterables.size(results) > 0);

    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertEquals(new DateTime("2011-01-13T00:00:00.000Z"), minTime);
    Assert.assertEquals(new DateTime("2011-01-16T00:00:00.000Z"), maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFilteredTimeBoundaryQueryNoMatches() throws IOException
  {
    QueryRunner customRunner = getCustomRunner();
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .filters("quality", "foobar") // foobar dimension does not exist
                                                .build();
    Assert.assertTrue(timeBoundaryQuery.hasFilters());
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        customRunner.run(timeBoundaryQuery, context),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );

    Assert.assertTrue(Iterables.size(results) == 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundary()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .build();
    Assert.assertFalse(timeBoundaryQuery.hasFilters());
    HashMap<String,Object> context = new HashMap<String, Object>();
    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        runner.run(timeBoundaryQuery, context),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), minTime);
    Assert.assertEquals(new DateTime("2011-04-15T00:00:00.000Z"), maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMax()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .bound(TimeBoundaryQuery.MAX_TIME)
                                                .build();
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        runner.run(timeBoundaryQuery, context),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertNull(minTime);
    Assert.assertEquals(new DateTime("2011-04-15T00:00:00.000Z"), maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMin()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .bound(TimeBoundaryQuery.MIN_TIME)
                                                .build();
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        runner.run(timeBoundaryQuery, context),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), minTime);
    Assert.assertNull(maxTime);
  }

  @Test
  public void testMergeResults() throws Exception
  {
    List<Result<TimeBoundaryResultValue>> results = Arrays.asList(
        new Result<>(
            new DateTime(),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    "maxTime", "2012-01-01",
                    "minTime", "2011-01-01"
                )
            )
        ),
        new Result<>(
            new DateTime(),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    "maxTime", "2012-02-01",
                    "minTime", "2011-01-01"
                )
            )
        )
    );

    TimeBoundaryQuery query = new TimeBoundaryQuery(new TableDataSource("test"), null, null, null, null);
    Iterable<Result<TimeBoundaryResultValue>> actual = query.mergeResults(results);

    Assert.assertTrue(actual.iterator().next().getValue().getMaxTime().equals(new DateTime("2012-02-01")));
  }

  @Test
  public void testMergeResultsEmptyResults() throws Exception
  {
    List<Result<TimeBoundaryResultValue>> results = Lists.newArrayList();

    TimeBoundaryQuery query = new TimeBoundaryQuery(new TableDataSource("test"), null, null, null, null);
    Iterable<Result<TimeBoundaryResultValue>> actual = query.mergeResults(results);

    Assert.assertFalse(actual.iterator().hasNext());
  }
}
