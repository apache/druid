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

package com.metamx.druid.query.timeboundary;

import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.Segment;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeBoundaryQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final TimeBoundaryQueryQueryToolChest toolChest = new TimeBoundaryQueryQueryToolChest();

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
    return new ChainedExecutionQueryRunner<Result<TimeBoundaryResultValue>>(
        queryExecutor, toolChest.getOrdering(), queryRunners
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
    public Sequence<Result<TimeBoundaryResultValue>> run(Query<Result<TimeBoundaryResultValue>> input)
    {
      if (!(input instanceof TimeBoundaryQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      final TimeBoundaryQuery legacyQuery = (TimeBoundaryQuery) input;

      return new BaseSequence<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>(
          new BaseSequence.IteratorMaker<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>()
          {
            @Override
            public Iterator<Result<TimeBoundaryResultValue>> make()
            {
              return legacyQuery.buildResult(
                  adapter.getInterval().getStart(),
                  adapter.getMinTime(),
                  adapter.getMaxTime()
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
