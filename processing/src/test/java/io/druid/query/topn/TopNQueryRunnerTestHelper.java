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

package io.druid.query.topn;

import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class TopNQueryRunnerTestHelper
{
  @SuppressWarnings("unchecked")
  public static Collection<?> makeQueryRunners(
      QueryRunnerFactory factory
  )
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    return Arrays.asList(
        new Object[][]{
            {
                makeQueryRunner(factory, new IncrementalIndexSegment(rtIndex))
            },
            {
                makeQueryRunner(factory, new QueryableIndexSegment(null, mMappedTestIndex))
            },
            {
                makeQueryRunner(factory, new QueryableIndexSegment(null, mergedRealtimeIndex))
            }
        }
    );
  }

  public static <T> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }
}