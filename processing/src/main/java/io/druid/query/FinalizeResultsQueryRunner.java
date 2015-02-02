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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;

import java.util.Map;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query, Map<String, Object> responseContext)
  {
    final boolean isBySegment = query.getContextBySegment(false);
    final boolean shouldFinalize = query.getContextFinalize(true);

    final Query<T> queryToRun;
    final Function<T, T> finalizerFn;
    final MetricManipulationFn metricManipulationFn;

    if (shouldFinalize) {
      queryToRun = query.withOverriddenContext(ImmutableMap.<String, Object>of("finalize", false));
      metricManipulationFn = MetricManipulatorFns.finalizing();

    } else {
      queryToRun = query;
      metricManipulationFn = MetricManipulatorFns.identity();
    }
    if (isBySegment) {
      finalizerFn = new Function<T, T>()
      {
        final Function<T, T> baseFinalizer = toolChest.makePostComputeManipulatorFn(
            query,
            metricManipulationFn
        );

        @Override
        @SuppressWarnings("unchecked")
        public T apply(T input)
        {
          Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;

          if (input == null) {
            throw new ISE("Cannot have a null result!");
          }

          BySegmentResultValue<T> resultsClass = result.getValue();

          return (T) new Result<BySegmentResultValueClass>(
              result.getTimestamp(),
              new BySegmentResultValueClass(
                  Lists.transform(resultsClass.getResults(), baseFinalizer),
                  resultsClass.getSegmentId(),
                  resultsClass.getInterval()
              )
          );
        }
      };
    } else {
      finalizerFn = toolChest.makePostComputeManipulatorFn(query, metricManipulationFn);
    }


    return Sequences.map(
        baseRunner.run(queryToRun, responseContext),
        finalizerFn
    );

  }
}
