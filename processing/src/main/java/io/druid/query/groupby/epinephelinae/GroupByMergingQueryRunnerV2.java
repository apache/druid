/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.collections.BlockingPool;
import io.druid.collections.ReferenceCountingResourceHolder;
import io.druid.collections.Releaser;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryWatcher;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GroupByMergingQueryRunnerV2 implements QueryRunner
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV2.class);
  private static final String CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION = "mergeRunnersUsingChainedExecution";

  private final GroupByQueryConfig config;
  private final Iterable<QueryRunner<Row>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final int concurrencyHint;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;

  public GroupByMergingQueryRunnerV2(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<Row>> queryables,
      int concurrencyHint,
      BlockingPool<ByteBuffer> mergeBufferPool,
      ObjectMapper spillMapper
  )
  {
    this.config = config;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.concurrencyHint = concurrencyHint;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
  }

  @Override
  public Sequence<Row> run(final Query queryParam, final Map responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryParam;
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    // CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION is here because realtime servers use nested mergeRunners calls
    // (one for the entire query and one for each sink). We only want the outer call to actually do merging with a
    // merge buffer, otherwise the query will allocate too many merge buffers. This is potentially sub-optimal as it
    // will involve materializing the results for each sink before starting to feed them into the outer merge buffer.
    // I'm not sure of a better way to do this without tweaking how realtime servers do queries.
    final boolean forceChainedExecution = query.getContextBoolean(
        CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION,
        false
    );
    final GroupByQuery queryForRunners = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(CTX_KEY_MERGE_RUNNERS_USING_CHAINED_EXECUTION, true)
    );

    if (BaseQuery.getContextBySegment(query, false) || forceChainedExecution) {
      return new ChainedExecutionQueryRunner(exec, queryWatcher, queryables).run(query, responseContext);
    }

    final AggregatorFactory[] combiningAggregatorFactories = new AggregatorFactory[query.getAggregatorSpecs().size()];
    for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
      combiningAggregatorFactories[i] = query.getAggregatorSpecs().get(i).getCombiningFactory();
    }

    final File temporaryStorageDirectory = new File(
        System.getProperty("java.io.tmpdir"),
        String.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final int priority = BaseQuery.getContextPriority(query, 0);

    // Figure out timeoutAt time now, so we can apply the timeout to both the mergeBufferPool.take and the actual
    // query processing together.
    final Number queryTimeout = query.getContextValue(QueryContextKeys.TIMEOUT, null);
    final long timeoutAt = queryTimeout == null
                           ? JodaUtils.MAX_INSTANT
                           : System.currentTimeMillis() + queryTimeout.longValue();

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Row, CloseableGrouperIterator<RowBasedKey, Row>>()
        {
          @Override
          public CloseableGrouperIterator<RowBasedKey, Row> make()
          {
            final List<ReferenceCountingResourceHolder> resources = Lists.newArrayList();

            try {
              final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
                  temporaryStorageDirectory,
                  querySpecificConfig.getMaxOnDiskStorage()
              );
              final ReferenceCountingResourceHolder<LimitedTemporaryStorage> temporaryStorageHolder =
                  ReferenceCountingResourceHolder.fromCloseable(temporaryStorage);
              resources.add(temporaryStorageHolder);

              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder;
              try {
                // This will potentially block if there are no merge buffers left in the pool.
                final long timeout = timeoutAt - System.currentTimeMillis();
                if (timeout <= 0 || (mergeBufferHolder = mergeBufferPool.take(timeout)) == null) {
                  throw new QueryInterruptedException(new TimeoutException());
                }
                resources.add(mergeBufferHolder);
              }
              catch (InterruptedException e) {
                throw new QueryInterruptedException(e);
              }

              Pair<Grouper<RowBasedKey>, Accumulator<Grouper<RowBasedKey>, Row>> pair = RowBasedGrouperHelper.createGrouperAccumulatorPair(
                  query,
                  false,
                  config,
                  mergeBufferHolder.get(),
                  concurrencyHint,
                  temporaryStorage,
                  spillMapper,
                  combiningAggregatorFactories
              );
              final Grouper<RowBasedKey> grouper = pair.lhs;
              final Accumulator<Grouper<RowBasedKey>, Row> accumulator = pair.rhs;

              final ReferenceCountingResourceHolder<Grouper<RowBasedKey>> grouperHolder =
                  ReferenceCountingResourceHolder.fromCloseable(grouper);
              resources.add(grouperHolder);

              ListenableFuture<List<Boolean>> futures = Futures.allAsList(
                  Lists.newArrayList(
                      Iterables.transform(
                          queryables,
                          new Function<QueryRunner<Row>, ListenableFuture<Boolean>>()
                          {
                            @Override
                            public ListenableFuture<Boolean> apply(final QueryRunner<Row> input)
                            {
                              if (input == null) {
                                throw new ISE(
                                    "Null queryRunner! Looks to be some segment unmapping action happening"
                                );
                              }

                              return exec.submit(
                                  new AbstractPrioritizedCallable<Boolean>(priority)
                                  {
                                    @Override
                                    public Boolean call() throws Exception
                                    {
                                      try (
                                          Releaser bufferReleaser = mergeBufferHolder.increment();
                                          Releaser grouperReleaser = grouperHolder.increment()
                                      ) {
                                        final Object retVal = input.run(queryForRunners, responseContext)
                                                                   .accumulate(grouper, accumulator);

                                        // Return true if OK, false if resources were exhausted.
                                        return retVal == grouper;
                                      }
                                      catch (QueryInterruptedException e) {
                                        throw e;
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
                  )
              );

              waitForFutureCompletion(query, futures, timeoutAt - System.currentTimeMillis());

              return RowBasedGrouperHelper.makeGrouperIterator(
                  grouper,
                  query,
                  new Closeable()
                  {
                    @Override
                    public void close() throws IOException
                    {
                      for (Closeable closeable : Lists.reverse(resources)) {
                        CloseQuietly.close(closeable);
                      }
                    }
                  }
              );
            }
            catch (Throwable e) {
              // Exception caught while setting up the iterator; release resources.
              for (Closeable closeable : Lists.reverse(resources)) {
                CloseQuietly.close(closeable);
              }
              throw e;
            }
          }

          @Override
          public void cleanup(CloseableGrouperIterator<RowBasedKey, Row> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  private void waitForFutureCompletion(
      GroupByQuery query,
      ListenableFuture<List<Boolean>> future,
      long timeout
  )
  {
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQuery(query, future);
      }

      if (timeout <= 0) {
        throw new TimeoutException();
      }

      final List<Boolean> results = future.get(timeout, TimeUnit.MILLISECONDS);

      for (Boolean result : results) {
        if (!result) {
          future.cancel(true);
          throw new ResourceLimitExceededException("Grouping resources exhausted");
        }
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }

}
