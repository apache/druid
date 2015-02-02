/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.timeboundary;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeBoundaryQueryRunnerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.makeQueryRunners(
        new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER)
    );
  }

  private final QueryRunner runner;

  public TimeBoundaryQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBoundary()
  {
    TimeBoundaryQuery timeBoundaryQuery = Druids.newTimeBoundaryQueryBuilder()
                                                .dataSource("testing")
                                                .build();
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

    TimeBoundaryQuery query = new TimeBoundaryQuery(new TableDataSource("test"), null, null, null);
    Iterable<Result<TimeBoundaryResultValue>> actual = query.mergeResults(results);

    Assert.assertTrue(actual.iterator().next().getValue().getMaxTime().equals(new DateTime("2012-02-01")));
  }

  @Test
  public void testMergeResultsEmptyResults() throws Exception
  {
    List<Result<TimeBoundaryResultValue>> results = Lists.newArrayList();

    TimeBoundaryQuery query = new TimeBoundaryQuery(new TableDataSource("test"), null, null, null);
    Iterable<Result<TimeBoundaryResultValue>> actual = query.mergeResults(results);

    Assert.assertFalse(actual.iterator().hasNext());
  }
}
