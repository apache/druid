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

package org.apache.druid.query.timeboundary;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQueryRunner;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
@ParameterizedClass
@MethodSource("constructorFeeder")
public class TimeBoundaryQueryRunnerTest extends InitializedNullHandlingTest
{
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER),
            true
        )
    );
  }

  private final TestQueryRunner<Result<TimeBoundaryResultValue>> runner;

  public TimeBoundaryQueryRunnerTest(
      TestQueryRunner<Result<TimeBoundaryResultValue>> runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testFilteredTimeBoundaryQuery()
  {
    // "automotive" rows appear at both ends of the segment, so the boundary is the boundary of the segment.
    assertTimeBoundary(
        new SelectorDimFilter("quality", "automotive", null),
        null,
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        DateTimes.of("2011-04-15T00:00:00.000Z")
    );
  }

  @Test
  public void testFilteredTimeBoundaryQueryNarrowerThanSegment()
  {
    // Only four rows have "index" >= 1700, and they all lie strictly inside the segment: the earliest is on
    // 2011-01-30 and the latest is on 2011-03-31.
    assertTimeBoundary(
        new RangeFilter("index", ColumnType.DOUBLE, 1700.0, null, false, null, null),
        null,
        DateTimes.of("2011-01-30T00:00:00.000Z"),
        DateTimes.of("2011-03-31T00:00:00.000Z")
    );
  }

  @Test
  public void testTimeFilteredTimeBoundaryQuery()
  {
    // There are no rows on the edges of the query interval: the earliest row inside it is on 2011-01-20T01, and the
    // latest is on 2011-01-22.
    assertTimeBoundary(
        null,
        Intervals.of("2011-01-20T00:00:00.000Z/2011-01-23T00:00:00.000Z"),
        DateTimes.of("2011-01-20T01:00:00.000Z"),
        DateTimes.of("2011-01-22T00:00:00.000Z")
    );
  }

  @Test
  public void testFilteredTimeBoundaryQueryNoMatches()
  {
    // "foobar" quality does not exist.
    assertTimeBoundary(new SelectorDimFilter("quality", "foobar", null), null, null, null);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundary()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .build();
    Assertions.assertFalse(timeBoundaryQuery.hasFilters());
    Iterable<Result<TimeBoundaryResultValue>> results = runner.run(QueryPlus.wrap(timeBoundaryQuery)).toList();
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assertions.assertEquals(DateTimes.of("2011-01-12T00:00:00.000Z"), minTime);
    Assertions.assertEquals(DateTimes.of("2011-04-15T00:00:00.000Z"), maxTime);
  }

  @Test
  public void testTimeBoundaryInlineData()
  {
    final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
        ImmutableList.of(new Object[]{DateTimes.of("2000-01-02").getMillis()}),
        RowSignature.builder().addTimeColumn().build()
    );

    TimeBoundaryQuery timeBoundaryQuery =
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource(inlineDataSource)
              .build();

    Assertions.assertFalse(timeBoundaryQuery.hasFilters());
    final QueryRunner<Result<TimeBoundaryResultValue>> theRunner =
        new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER).createRunner(
            new RowBasedSegment<>(
                Sequences.simple(inlineDataSource.getRows()),
                inlineDataSource.rowAdapter(),
                inlineDataSource.getRowSignature()
            )
        );
    Iterable<Result<TimeBoundaryResultValue>> results = theRunner.run(QueryPlus.wrap(timeBoundaryQuery)).toList();
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assertions.assertEquals(DateTimes.of("2000-01-02"), minTime);
    Assertions.assertEquals(DateTimes.of("2000-01-02"), maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryArrayResults()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .bound(null)
                                                .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();
    Assertions.assertThrows(
        UOE.class,
        () -> new TimeBoundaryQueryQueryToolChest().resultsAsArrays(
            timeBoundaryQuery,
            runner.run(QueryPlus.wrap(timeBoundaryQuery), context)
        ).toList()
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMax()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .bound(TimeBoundaryQuery.MAX_TIME)
                                                .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();
    Iterable<Result<TimeBoundaryResultValue>> results = runner.run(QueryPlus.wrap(timeBoundaryQuery), context).toList();
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assertions.assertNull(minTime);
    Assertions.assertEquals(DateTimes.of("2011-04-15T00:00:00.000Z"), maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMaxArraysResults()
  {
    TimeBoundaryQuery maxTimeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                   .dataSource("testing")
                                                   .bound(TimeBoundaryQuery.MAX_TIME)
                                                   .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();
    List<Object[]> maxTime = new TimeBoundaryQueryQueryToolChest().resultsAsArrays(
        maxTimeBoundaryQuery,
        runner.run(QueryPlus.wrap(maxTimeBoundaryQuery), context)
    ).toList();

    Long maxTimeMillis = (Long) maxTime.get(0)[0];
    Assertions.assertEquals(DateTimes.of("2011-04-15T00:00:00.000Z"), new DateTime(maxTimeMillis, DateTimeZone.UTC));
    Assertions.assertEquals(1, maxTime.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMin()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .bound(TimeBoundaryQuery.MIN_TIME)
                                                .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();
    Iterable<Result<TimeBoundaryResultValue>> results = runner.run(QueryPlus.wrap(timeBoundaryQuery), context).toList();
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assertions.assertEquals(DateTimes.of("2011-01-12T00:00:00.000Z"), minTime);
    Assertions.assertNull(maxTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundaryMinArraysResults()
  {
    TimeBoundaryQuery minTimeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                   .dataSource("testing")
                                                   .bound(TimeBoundaryQuery.MIN_TIME)
                                                   .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();
    List<Object[]> minTime = new TimeBoundaryQueryQueryToolChest().resultsAsArrays(
        minTimeBoundaryQuery,
        runner.run(QueryPlus.wrap(minTimeBoundaryQuery), context)
    ).toList();

    Long minTimeMillis = (Long) minTime.get(0)[0];
    Assertions.assertEquals(DateTimes.of("2011-01-12T00:00:00.000Z"), new DateTime(minTimeMillis, DateTimeZone.UTC));
    Assertions.assertEquals(1, minTime.size());
  }

  @Test
  public void testMergeResults()
  {
    List<Result<TimeBoundaryResultValue>> results = Arrays.asList(
        new Result<>(
            DateTimes.nowUtc(),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    "maxTime", "2012-01-01",
                    "minTime", "2011-01-01"
                )
            )
        ),
        new Result<>(
            DateTimes.nowUtc(),
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

    Assertions.assertTrue(actual.iterator().next().getValue().getMaxTime().equals(DateTimes.of("2012-02-01")));
  }

  @Test
  public void testMergeResultsEmptyResults()
  {
    List<Result<TimeBoundaryResultValue>> results = new ArrayList<>();

    TimeBoundaryQuery query = new TimeBoundaryQuery(new TableDataSource("test"), null, null, null, null);
    Iterable<Result<TimeBoundaryResultValue>> actual = query.mergeResults(results);

    Assertions.assertFalse(actual.iterator().hasNext());
  }

  /**
   * Run a time boundary query against {@link #runner} for every "bound" and every vectorization mode that the
   * runner's segment supports, and verify the min and max time. Null expectations mean that the query is expected to
   * return no results at all.
   */
  private void assertTimeBoundary(
      @Nullable final DimFilter filter,
      @Nullable final Interval interval,
      @Nullable final DateTime expectedMinTime,
      @Nullable final DateTime expectedMaxTime
  )
  {
    final List<QueryContexts.Vectorize> vectorizeValues =
        new ArrayList<>(Arrays.asList(QueryContexts.Vectorize.FALSE, QueryContexts.Vectorize.TRUE));

    if (runner.getSegment().as(QueryableIndex.class) != null) {
      vectorizeValues.add(QueryContexts.Vectorize.FORCE);
    }

    for (final String bound : Arrays.asList(TimeBoundaryQuery.MIN_TIME, TimeBoundaryQuery.MAX_TIME, null)) {
      for (final QueryContexts.Vectorize vectorize : vectorizeValues) {
        final String message = StringUtils.join(new Object[]{runner.getName(), bound, vectorize}, ' ');
        final TimeBoundaryQuery query =
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                  .filters(filter)
                  .intervals(
                      interval == null
                      ? null
                      : new MultipleIntervalSegmentSpec(ImmutableList.of(interval))
                  )
                  .bound(bound)
                  .context(
                      QueryContext.ofMap(
                          QueryContextParameters.VECTORIZE,
                          vectorize,
                          QueryContextParameters.VECTOR_SIZE,
                          7
                      )
                  )
                  .build();

        Assertions.assertEquals(filter != null, query.hasFilters(), message);

        final ResponseContext context = ConcurrentResponseContext.createEmpty();
        context.initializeMissingSegments();
        final List<Result<TimeBoundaryResultValue>> results =
            runner.run(QueryPlus.wrap(query), context).toList();

        final DateTime expectedMinTimeForBound =
            TimeBoundaryQuery.MAX_TIME.equals(bound) ? null : expectedMinTime;
        final DateTime expectedMaxTimeForBound =
            TimeBoundaryQuery.MIN_TIME.equals(bound) ? null : expectedMaxTime;

        if (expectedMinTimeForBound == null && expectedMaxTimeForBound == null) {
          Assertions.assertEquals(Collections.emptyList(), results, message);
        } else {
          final TimeBoundaryResultValue val = Iterables.getOnlyElement(results).getValue();
          Assertions.assertEquals(expectedMinTimeForBound, val.getMinTime(), message);
          Assertions.assertEquals(expectedMaxTimeForBound, val.getMaxTime(), message);
        }
      }
    }
  }
}
