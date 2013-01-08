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

package com.metamx.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.guava.ExecutorExecutingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.Segment;
import com.metamx.druid.query.ConcatQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.metadata.SegmentMetadataQuery;
import com.metamx.druid.query.metadata.SegmentMetadataQueryEngine;
import com.metamx.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import com.metamx.druid.result.SegmentMetadataResultValue;
import com.metamx.druid.result.Result;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SegmentMetadataQueryRunnerFactory implements QueryRunnerFactory<Result<SegmentMetadataResultValue>, SegmentMetadataQuery>
{
  private static final SegmentMetadataQueryQueryToolChest toolChest = new SegmentMetadataQueryQueryToolChest()
  {
    @Override
    public QueryRunner<Result<SegmentMetadataResultValue>> mergeResults(QueryRunner<Result<SegmentMetadataResultValue>> runner)
    {
      return new ConcatQueryRunner<Result<SegmentMetadataResultValue>>(Sequences.simple(ImmutableList.of(runner)));
    }
  };


  @Override
  public QueryRunner<Result<SegmentMetadataResultValue>> createRunner(final Segment adapter)
  {
    return new QueryRunner<Result<SegmentMetadataResultValue>>()
    {
      @Override
      public Sequence<Result<SegmentMetadataResultValue>> run(Query<Result<SegmentMetadataResultValue>> query)
      {
        if (!(query instanceof SegmentMetadataQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", query.getClass(), SegmentMetadataQuery.class);
        }
        return new SegmentMetadataQueryEngine().process((SegmentMetadataQuery) query, adapter.asStorageAdapter());
      }
    };
  }

  @Override
  public QueryRunner<Result<SegmentMetadataResultValue>> mergeRunners(
      final ExecutorService queryExecutor, Iterable<QueryRunner<Result<SegmentMetadataResultValue>>> queryRunners
  )
  {
    return new ConcatQueryRunner<Result<SegmentMetadataResultValue>>(
            Sequences.map(
                Sequences.simple(queryRunners),
                new Function<QueryRunner<Result<SegmentMetadataResultValue>>, QueryRunner<Result<SegmentMetadataResultValue>>>()
                {
                  @Override
                  public QueryRunner<Result<SegmentMetadataResultValue>> apply(final QueryRunner<Result<SegmentMetadataResultValue>> input)
                  {
                    return new QueryRunner<Result<SegmentMetadataResultValue>>()
                    {
                      @Override
                      public Sequence<Result<SegmentMetadataResultValue>> run(final Query<Result<SegmentMetadataResultValue>> query)
                      {

                        Future<Sequence<Result<SegmentMetadataResultValue>>> future = queryExecutor.submit(
                            new Callable<Sequence<Result<SegmentMetadataResultValue>>>()
                            {
                              @Override
                              public Sequence<Result<SegmentMetadataResultValue>> call() throws Exception
                              {
                                return new ExecutorExecutingSequence<Result<SegmentMetadataResultValue>>(
                                    input.run(query),
                                    queryExecutor
                                );
                              }
                            }
                        );
                        try {
                          return future.get();
                        }
                        catch (InterruptedException e) {
                          throw Throwables.propagate(e);
                        }
                        catch (ExecutionException e) {
                          throw Throwables.propagate(e);
                        }
                      }
                    };
                  }
                }
            )
        );
  }

  @Override
  public QueryToolChest getToolchest()
  {
    return toolChest;
  }
}
