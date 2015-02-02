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

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeBoundaryQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final TimeBoundaryQueryQueryToolChest toolChest = new TimeBoundaryQueryQueryToolChest();
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeBoundaryQueryRunnerFactory(QueryWatcher queryWatcher)
  {
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> createRunner(final Segment segment)
  {
    return new TimeBoundaryQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeBoundaryResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(
        queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery> getToolchest()
  {
    return toolChest;
  }

  private static class TimeBoundaryQueryRunner implements QueryRunner<Result<TimeBoundaryResultValue>>
  {
    private final StorageAdapter adapter;

    public TimeBoundaryQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter();
    }

    @Override
    public Sequence<Result<TimeBoundaryResultValue>> run(
        Query<Result<TimeBoundaryResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TimeBoundaryQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeBoundaryQuery.class);
      }

      final TimeBoundaryQuery legacyQuery = (TimeBoundaryQuery) input;

      return new BaseSequence<>(
          new BaseSequence.IteratorMaker<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>()
          {
            @Override
            public Iterator<Result<TimeBoundaryResultValue>> make()
            {
              if (adapter == null) {
                throw new ISE(
                    "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
                );
              }

              final DateTime minTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MAX_TIME)
                                       ? null
                                       : adapter.getMinTime();
              final DateTime maxTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MIN_TIME)
                                       ? null
                                       : adapter.getMaxTime();


              return legacyQuery.buildResult(
                  adapter.getInterval().getStart(),
                  minTime,
                  maxTime
              ).iterator();
            }

            @Override
            public void cleanup(Iterator<Result<TimeBoundaryResultValue>> toClean)
            {

            }
          }
      );
    }
  }
}
