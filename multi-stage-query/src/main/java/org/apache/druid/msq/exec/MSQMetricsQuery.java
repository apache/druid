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

package org.apache.druid.msq.exec;

import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Placeholder {@link Query} that exists only to be passed to {@link QueryMetrics#query}. Used by MSQ when there is
 * no native query available, such as on workers, or on controllers when using {@link QueryDefMSQSpec}. It cannot be
 * executed or serialized.
 */
public class MSQMetricsQuery extends BaseQuery<Object>
{
  private MSQMetricsQuery(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
  }

  /**
   * Creates a query that reads the provided tables and intervals.
   */
  public static MSQMetricsQuery create(
      final Set<String> tableNames,
      final Collection<Interval> intervals,
      final QueryContext queryContext
  )
  {
    return new MSQMetricsQuery(
        makeDataSource(tableNames),
        new MultipleIntervalSegmentSpec(new ArrayList<>(intervals)),
        queryContext.asMap()
    );
  }

  @Override
  public String getType()
  {
    return MSQMetrics.QUERY_METRIC_TYPE;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Nullable
  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public QueryRunner<Object> getRunner(final QuerySegmentWalker walker)
  {
    throw new UnsupportedOperationException("Query is for metrics only");
  }

  @Override
  public Query<Object> withOverriddenContext(final Map<String, Object> contextOverride)
  {
    return new MSQMetricsQuery(getDataSource(), getQuerySegmentSpec(), computeOverridenContext(contextOverride));
  }

  @Override
  public Query<Object> withQuerySegmentSpec(final QuerySegmentSpec spec)
  {
    return new MSQMetricsQuery(getDataSource(), spec, getContext());
  }

  @Override
  public Query<Object> withDataSource(final DataSource dataSource)
  {
    return new MSQMetricsQuery(dataSource, getQuerySegmentSpec(), getContext());
  }

  /**
   * Returns a {@link DataSource} whose {@link DataSource#getTableNames()} are the provided table names.
   */
  private static DataSource makeDataSource(final Set<String> tableNames)
  {
    if (tableNames.isEmpty()) {
      // No tables, such as for a query that only reads external data.
      return InlineDataSource.fromIterable(List.of(), RowSignature.empty());
    } else if (tableNames.size() == 1) {
      return new TableDataSource(tableNames.iterator().next());
    } else {
      final List<DataSource> dataSources = new ArrayList<>();
      for (final String tableName : tableNames) {
        dataSources.add(new TableDataSource(tableName));
      }
      return new UnionDataSource(dataSources);
    }
  }
}
