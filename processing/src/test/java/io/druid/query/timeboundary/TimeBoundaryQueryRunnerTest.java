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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

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

    Iterable<Result<TimeBoundaryResultValue>> results = Sequences.toList(
        runner.run(timeBoundaryQuery),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TimeBoundaryResultValue val = results.iterator().next().getValue();
    DateTime minTime = val.getMinTime();
    DateTime maxTime = val.getMaxTime();

    Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), minTime);
    Assert.assertEquals(new DateTime("2011-04-15T00:00:00.000Z"), maxTime);
  }
}
