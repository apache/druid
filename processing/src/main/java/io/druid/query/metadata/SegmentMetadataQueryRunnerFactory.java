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

package io.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SegmentMetadataQueryRunnerFactory implements QueryRunnerFactory<SegmentAnalysis, SegmentMetadataQuery>
{
  private static final Logger log = new Logger(SegmentMetadataQueryRunnerFactory.class);


  private final SegmentMetadataQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public SegmentMetadataQueryRunnerFactory(
      SegmentMetadataQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<SegmentAnalysis> createRunner(final Segment segment)
  {
    return new QueryRunner<SegmentAnalysis>()
    {
      @Override
      public Sequence<SegmentAnalysis> run(QueryPlus<SegmentAnalysis> inQ, Map<String, Object> responseContext)
      {
        SegmentMetadataQuery updatedQuery = ((SegmentMetadataQuery) inQ.getQuery())
            .withFinalizedAnalysisTypes(toolChest.getConfig());
        final SegmentAnalyzer analyzer = new SegmentAnalyzer(updatedQuery.getAnalysisTypes());
        final Map<String, ColumnAnalysis> analyzedColumns = analyzer.analyze(segment);
        final long numRows = analyzer.numRows(segment);
        long totalSize = 0;

        if (analyzer.analyzingSize()) {
          // Initialize with the size of the whitespace, 1 byte per
          totalSize = analyzedColumns.size() * numRows;
        }

        Map<String, ColumnAnalysis> columns = Maps.newTreeMap();
        ColumnIncluderator includerator = updatedQuery.getToInclude();
        for (Map.Entry<String, ColumnAnalysis> entry : analyzedColumns.entrySet()) {
          final String columnName = entry.getKey();
          final ColumnAnalysis column = entry.getValue();

          if (!column.isError()) {
            totalSize += column.getSize();
          }
          if (includerator.include(columnName)) {
            columns.put(columnName, column);
          }
        }
        List<Interval> retIntervals = updatedQuery.analyzingInterval() ?
                                      Collections.singletonList(segment.getDataInterval()) : null;

        final Map<String, AggregatorFactory> aggregators;
        Metadata metadata = null;
        if (updatedQuery.hasAggregators()) {
          metadata = segment.asStorageAdapter().getMetadata();
          if (metadata != null && metadata.getAggregators() != null) {
            aggregators = Maps.newHashMap();
            for (AggregatorFactory aggregator : metadata.getAggregators()) {
              aggregators.put(aggregator.getName(), aggregator);
            }
          } else {
            aggregators = null;
          }
        } else {
          aggregators = null;
        }

        final TimestampSpec timestampSpec;
        if (updatedQuery.hasTimestampSpec()) {
          if (metadata == null) {
            metadata = segment.asStorageAdapter().getMetadata();
          }
          timestampSpec = metadata != null ? metadata.getTimestampSpec() : null;
        } else {
          timestampSpec = null;
        }

        final Granularity queryGranularity;
        if (updatedQuery.hasQueryGranularity()) {
          if (metadata == null) {
            metadata = segment.asStorageAdapter().getMetadata();
          }
          queryGranularity = metadata != null ? metadata.getQueryGranularity() : null;
        } else {
          queryGranularity = null;
        }

        Boolean rollup = null;
        if (updatedQuery.hasRollup()) {
          if (metadata == null) {
            metadata = segment.asStorageAdapter().getMetadata();
          }
          rollup = metadata != null ? metadata.isRollup() : null;
          if (rollup == null) {
            // in this case, this segment is built before no-rollup function is coded,
            // thus it is built with rollup
            rollup = Boolean.TRUE;
          }
        }

        return Sequences.simple(
            Collections.singletonList(
                new SegmentAnalysis(
                    segment.getIdentifier(),
                    retIntervals,
                    columns,
                    totalSize,
                    numRows,
                    aggregators,
                    timestampSpec,
                    queryGranularity,
                    rollup
                )
            )
        );
      }
    };
  }

  @Override
  public QueryRunner<SegmentAnalysis> mergeRunners(
      ExecutorService exec, Iterable<QueryRunner<SegmentAnalysis>> queryRunners
  )
  {
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);
    return new ConcatQueryRunner<SegmentAnalysis>(
        Sequences.map(
            Sequences.simple(queryRunners),
            new Function<QueryRunner<SegmentAnalysis>, QueryRunner<SegmentAnalysis>>()
            {
              @Override
              public QueryRunner<SegmentAnalysis> apply(final QueryRunner<SegmentAnalysis> input)
              {
                return new QueryRunner<SegmentAnalysis>()
                {
                  @Override
                  public Sequence<SegmentAnalysis> run(
                      final QueryPlus<SegmentAnalysis> queryPlus,
                      final Map<String, Object> responseContext
                  )
                  {
                    final Query<SegmentAnalysis> query = queryPlus.getQuery();
                    final int priority = QueryContexts.getPriority(query);
                    final QueryPlus<SegmentAnalysis> threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
                    final ListenableFuture<Sequence<SegmentAnalysis>> future = queryExecutor.submit(
                        new AbstractPrioritizedCallable<Sequence<SegmentAnalysis>>(priority)
                        {
                          @Override
                          public Sequence<SegmentAnalysis> call() throws Exception
                          {
                            return Sequences.simple(
                                Sequences.toList(
                                    input.run(threadSafeQueryPlus, responseContext),
                                    new ArrayList<>()
                                )
                            );
                          }
                        }
                    );
                    try {
                      queryWatcher.registerQuery(query, future);
                      if (QueryContexts.hasTimeout(query)) {
                        return future.get(QueryContexts.getTimeout(query), TimeUnit.MILLISECONDS);
                      } else {
                        return future.get();
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
                };
              }
            }
        )
    );
  }

  @Override
  public QueryToolChest<SegmentAnalysis, SegmentMetadataQuery> getToolchest()
  {
    return toolChest;
  }
}
