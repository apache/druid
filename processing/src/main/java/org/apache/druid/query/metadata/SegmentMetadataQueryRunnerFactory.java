/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.ConcatQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
      public Sequence<SegmentAnalysis> run(QueryPlus<SegmentAnalysis> inQ, ResponseContext responseContext)
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

        Map<String, ColumnAnalysis> columns = new TreeMap<>();
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
            aggregators = new HashMap<>();
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
                    segment.getId().toString(),
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
      ExecutorService exec,
      Iterable<QueryRunner<SegmentAnalysis>> queryRunners
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
                      final ResponseContext responseContext
                  )
                  {
                    final Query<SegmentAnalysis> query = queryPlus.getQuery();
                    final int priority = QueryContexts.getPriority(query);
                    final QueryPlus<SegmentAnalysis> threadSafeQueryPlus = queryPlus.withoutThreadUnsafeState();
                    final ListenableFuture<Sequence<SegmentAnalysis>> future = queryExecutor.submit(
                        new AbstractPrioritizedCallable<Sequence<SegmentAnalysis>>(priority)
                        {
                          @Override
                          public Sequence<SegmentAnalysis> call()
                          {
                            return Sequences.simple(input.run(threadSafeQueryPlus, responseContext).toList());
                          }
                        }
                    );
                    try {
                      queryWatcher.registerQueryFuture(query, future);
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
                      throw new RuntimeException(e);
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
