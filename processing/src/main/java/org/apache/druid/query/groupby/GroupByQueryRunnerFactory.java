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

package org.apache.druid.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<ResultRow, GroupByQuery>
{
  private final GroupingEngine groupingEngine;
  private final GroupByQueryQueryToolChest toolChest;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupingEngine groupingEngine,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.groupingEngine = groupingEngine;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<ResultRow> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, groupingEngine);
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return new QueryRunner<ResultRow>()
    {
      @Override
      public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
      {
        QueryRunner<ResultRow> rowQueryRunner = groupingEngine.mergeRunners(queryProcessingPool, queryRunners);
        return rowQueryRunner.run(queryPlus, responseContext);
      }
    };
  }

  @Override
  public QueryToolChest<ResultRow, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<ResultRow>
  {
    private final StorageAdapter adapter;
    private final GroupingEngine groupingEngine;

    public GroupByQueryRunner(Segment segment, final GroupingEngine groupingEngine)
    {
      this.adapter = segment.asStorageAdapter();
      this.groupingEngine = groupingEngine;
    }

    @Override
    public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
    {
      Query<ResultRow> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      GroupByQuery groupByQuery = (GroupByQuery) query;

      Sequence<ResultRow> process = groupingEngine.process(groupByQuery, adapter,
          (GroupByQueryMetrics) queryPlus.getQueryMetrics());

      return wrapSummaryRowIfNeeded(groupByQuery, process);
    }
  }

  @VisibleForTesting
  public GroupingEngine getGroupingEngine()
  {
    return groupingEngine;
  }

  /**
   * Wraps the sequence around if for this query a summary row might be needed in case the input becomes empty.
   */
  public static Sequence<ResultRow> wrapSummaryRowIfNeeded(GroupByQuery query, Sequence<ResultRow> process)
  {
    if (!summaryRowPreconditions(query)) {
      return process;
    }

    final AtomicBoolean t = new AtomicBoolean();

    return Sequences.concat(
        Sequences.map(process, ent -> {
          t.set(true);
          return ent;
        }),
        Sequences.simple(() -> {
          if (t.get()) {
            return Collections.emptyIterator();
          }
          return summaryRowIterator(query);
        }));
  }

  private static boolean summaryRowPreconditions(GroupByQuery query)
  {
    LimitSpec limit = query.getLimitSpec();
    if (limit instanceof DefaultLimitSpec) {
      DefaultLimitSpec limitSpec = (DefaultLimitSpec) limit;
      if (limitSpec.getLimit() == 0 || limitSpec.getOffset() > 0) {
        return false;
      }
    }
    if (!query.getDimensions().isEmpty()) {
      return false;
    }
    if (query.getGranularity().isFinerThan(Granularities.ALL)) {
      return false;
    }
    return true;
  }

  private static Iterator<ResultRow> summaryRowIterator(GroupByQuery q)
  {
    List<AggregatorFactory> aggSpec = q.getAggregatorSpecs();
    Object[] values = new Object[aggSpec.size()];
    for (int i = 0; i < aggSpec.size(); i++) {
      values[i] = aggSpec.get(i).factorize(new AllNullColumnSelectorFactory()).get();
    }
    return Collections.singleton(ResultRow.of(values)).iterator();
  }

}
