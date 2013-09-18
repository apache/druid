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
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.MergeIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A QueryRunner that combines a list of other QueryRunners and executes them in parallel on an executor.
 * <p/>
 * When using this, it is important to make sure that the list of QueryRunners provided is fully flattened.
 * If, for example, you were to pass a list of a Chained QueryRunner (A) and a non-chained QueryRunner (B).  Imagine
 * A has 2 QueryRunner chained together (Aa and Ab), the fact that the Queryables are run in parallel on an
 * executor would mean that the Queryables are actually processed in the order
 * <p/>
 * A -> B -> Aa -> Ab
 * <p/>
 * That is, the two sub queryables for A would run *after* B is run, effectively meaning that the results for B
 * must be fully cached in memory before the results for Aa and Ab are computed.
 */
public class ChainedExecutionQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ChainedExecutionQueryRunner.class);

  private final Iterable<QueryRunner<T>> queryables;
  private final ExecutorService exec;
  private final Ordering<T> ordering;

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      Ordering<T> ordering,
      QueryRunner<T>... queryables
  )
  {
    this(exec, ordering, Arrays.asList(queryables));
  }

  public ChainedExecutionQueryRunner(
      ExecutorService exec,
      Ordering<T> ordering,
      Iterable<QueryRunner<T>> queryables
  )
  {
    this.exec = exec;
    this.ordering = ordering;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final int priority = Integer.parseInt(query.getContextValue("priority", "0"));

    return new BaseSequence<T, Iterator<T>>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            // Make it a List<> to materialize all of the values (so that it will submit everything to the executor)
            List<Future<List<T>>> futures = Lists.newArrayList(
                Iterables.transform(
                    queryables,
                    new Function<QueryRunner<T>, Future<List<T>>>()
                    {
                      @Override
                      public Future<List<T>> apply(final QueryRunner<T> input)
                      {
                        return exec.submit(
                            new PrioritizedCallable<List<T>>(priority)
                            {
                              @Override
                              public List<T> call() throws Exception
                              {
                                try {
                                  return Sequences.toList(input.run(query), Lists.<T>newArrayList());
                                }
                                catch (Exception e) {
                                  log.error(e, "Exception with one of the sequences!");
                                  throw Throwables.propagate(e);
                                }
                              }
                            }
                        );
                      }
                    }
                )
            );

            return new MergeIterable<T>(
                ordering.nullsFirst(),
                Iterables.transform(
                    futures,
                    new Function<Future<List<T>>, Iterable<T>>()
                    {
                      @Override
                      public Iterable<T> apply(Future<List<T>> input)
                      {
                        try {
                          return input.get();
                        }
                        catch (InterruptedException e) {
                          throw new RuntimeException(e);
                        }
                        catch (ExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                )
            ).iterator();
          }

          @Override
          public void cleanup(Iterator<T> tIterator)
          {

          }
        }
    );
  }
}
