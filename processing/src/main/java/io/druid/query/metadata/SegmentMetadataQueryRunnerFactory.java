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

package io.druid.query.metadata;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.ConcatQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SegmentMetadataQueryRunnerFactory implements QueryRunnerFactory<SegmentAnalysis, SegmentMetadataQuery>
{
  private static final SegmentAnalyzer analyzer = new SegmentAnalyzer();
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
      public Sequence<SegmentAnalysis> run(Query<SegmentAnalysis> inQ)
      {
        SegmentMetadataQuery query = (SegmentMetadataQuery) inQ;

        final QueryableIndex index = segment.asQueryableIndex();
        if (index == null) {
          return Sequences.empty();
        }

        final Map<String, ColumnAnalysis> analyzedColumns = analyzer.analyze(index);

        // Initialize with the size of the whitespace, 1 byte per
        long totalSize = analyzedColumns.size() * index.getNumRows();

        Map<String, ColumnAnalysis> columns = Maps.newTreeMap();
        ColumnIncluderator includerator = query.getToInclude();
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

        return Sequences.simple(
            Arrays.asList(
                new SegmentAnalysis(
                    segment.getIdentifier(),
                    Arrays.asList(segment.getDataInterval()),
                    columns,
                    totalSize
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
                  public Sequence<SegmentAnalysis> run(final Query<SegmentAnalysis> query)
                  {
                    final int priority = query.getContextPriority(0);
                    ListenableFuture<Sequence<SegmentAnalysis>> future = queryExecutor.submit(
                        new AbstractPrioritizedCallable<Sequence<SegmentAnalysis>>(priority)
                        {
                          @Override
                          public Sequence<SegmentAnalysis> call() throws Exception
                          {
                            return input.run(query);
                          }
                        }
                    );
                    try {
                      queryWatcher.registerQuery(query, future);
                      final Number timeout = query.getContextValue("timeout", (Number) null);
                      return timeout == null ? future.get() : future.get(timeout.longValue(), TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
                      future.cancel(true);
                      throw new QueryInterruptedException("Query interrupted");
                    }
                    catch(CancellationException e) {
                      throw new QueryInterruptedException("Query cancelled");
                    }
                    catch(TimeoutException e) {
                      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
                      future.cancel(true);
                      throw new QueryInterruptedException("Query timeout");
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
