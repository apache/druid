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

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.collections.StupidPool;
import io.druid.guice.annotations.Global;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 */
public class TopNQueryRunnerFactory implements QueryRunnerFactory<Result<TopNResultValue>, TopNQuery>
{
  private final StupidPool<ByteBuffer> computationBufferPool;
  private final TopNQueryQueryToolChest toolchest;

  @Inject
  public TopNQueryRunnerFactory(
      @Global StupidPool<ByteBuffer> computationBufferPool,
      TopNQueryQueryToolChest toolchest
  )
  {
    this.computationBufferPool = computationBufferPool;
    this.toolchest = toolchest;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> createRunner(final Segment segment)
  {
    final TopNQueryEngine queryEngine = new TopNQueryEngine(computationBufferPool);
    return new QueryRunner<Result<TopNResultValue>>()
    {
      @Override
      public Sequence<Result<TopNResultValue>> run(Query<Result<TopNResultValue>> input)
      {
        if (!(input instanceof TopNQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TopNQuery.class);
        }

        final TopNQuery legacyQuery = (TopNQuery) input;

        return new BaseSequence<Result<TopNResultValue>, Iterator<Result<TopNResultValue>>>(
            new BaseSequence.IteratorMaker<Result<TopNResultValue>, Iterator<Result<TopNResultValue>>>()
            {
              @Override
              public Iterator<Result<TopNResultValue>> make()
              {
                return queryEngine.query(legacyQuery, segment.asStorageAdapter()).iterator();
              }

              @Override
              public void cleanup(Iterator<Result<TopNResultValue>> toClean)
              {

              }
            }
        );
      }
    };

  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TopNResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<TopNResultValue>>(
        queryExecutor, toolchest.getOrdering(), queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TopNResultValue>, TopNQuery> getToolchest()
  {
    return toolchest;
  }
}
