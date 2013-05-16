/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.result.Result;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class QueryRunnerHelper
{
  private static final Logger log = new Logger(QueryRunnerHelper.class);

  public static Aggregator[] makeAggregators(Cursor cursor, List<AggregatorFactory> aggregatorSpecs)
  {
    Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex] = spec.factorize(cursor);
      ++aggregatorIndex;
    }
    return aggregators;
  }

  public static <T> Iterable<Result<T>> makeCursorBasedQuery(
      final StorageAdapter adapter,
      List<Interval> queryIntervals,
      Filter filter,
      QueryGranularity granularity,
      Function<Cursor, Result<T>> mapFn
  )
  {
    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return FunctionalIterable
        .create(adapter.makeCursors(filter, queryIntervals.get(0), granularity))
        .transform(
            new Function<Cursor, Cursor>()
            {
              @Override
              public Cursor apply(@Nullable Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return input;
              }
            }
        )
        .keep(mapFn);
  }
}
