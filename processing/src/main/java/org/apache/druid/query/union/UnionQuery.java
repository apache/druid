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

package org.apache.druid.query.union;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Finalization;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class UnionQuery implements Query<Object>
{
  @JsonProperty("context")
  protected final Map<String, Object> context;

  @JsonProperty("queries")
  protected final List<Query<?>> queries;

  public UnionQuery(List<Query<?>> queries)
  {
    this(queries, queries.get(0).getContext());
  }

  @JsonCreator
  public UnionQuery(
      @JsonProperty("queries") List<Query<?>> queries,
      @JsonProperty("context") Map<String, Object> context)
  {
    Preconditions.checkArgument(queries.size() > 1, "union with fewer than 2 queries makes no sense");
    this.queries = queries;
    this.context = context;
  }

  @Override
  public DataSource getDataSource()
  {
    throw DruidException.defensive("This is not supported. Use getDataSources instead!");
  }

  public List<DataSource> getDataSources()
  {
    List<DataSource> dataSources = new ArrayList<>();
    for (Query<?> query : queries) {
      dataSources.add(query.getDataSource());
    }
    return dataSources;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return getClass().getSimpleName();
  }

  @Override
  public QueryRunner<Object> getRunner(QuerySegmentWalker walker)
  {
    throw DruidException.defensive("Use QueryToolChest to get a Runner");
  }

  @Override
  public List<Interval> getIntervals()
  {
    return Collections.emptyList();
  }

  @Override
  public Duration getDuration()
  {
    throw methodNotSupported();
  }

  @Override
  public Granularity getGranularity()
  {
    return Granularities.ALL;
  }

  @Override
  public DateTimeZone getTimezone()
  {
    throw methodNotSupported();
  }

  @Override
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public Ordering<Object> getResultOrdering()
  {
    throw methodNotSupported();
  }

  @Override
  public Query<Object> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    List<Query<?>> newQueries = mapQueries(q -> q.withOverriddenContext(contextOverrides));
    return new UnionQuery(newQueries, QueryContexts.override(getContext(), contextOverrides));
  }

  @Override
  public Query<Object> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw methodNotSupported();
  }

  @Override
  public Query<Object> withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.QUERY_ID, id));
  }

  @Override
  public String getId()
  {
    return context().getString(BaseQuery.QUERY_ID);
  }

  @Override
  public Query<Object> withSubQueryId(String subQueryId)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.SUB_QUERY_ID, subQueryId));
  }

  @Override
  public String getSubQueryId()
  {
    return context().getString(BaseQuery.SUB_QUERY_ID);
  }

  @Override
  public Query<Object> withDataSource(DataSource dataSource)
  {
    throw new RuntimeException("This method is not supported. Use withDataSources instead!");
  }

  public Query<Object> withDataSources(List<DataSource> children)
  {
    Preconditions.checkArgument(queries.size() == children.size(), "Number of children must match number of queries");
    List<Query<?>> newQueries = new ArrayList<>();
    for (int i = 0; i < queries.size(); i++) {
      newQueries.add(queries.get(i).withDataSource(children.get(i)));
    }
    return new UnionQuery(newQueries, context);
  }

  List<Query<?>> mapQueries(Function<Query<?>, Query<?>> mapFn)
  {
    List<Query<?>> newQueries = new ArrayList<>();
    for (Query<?> query : queries) {
      newQueries.add(mapFn.apply(query));
    }
    return newQueries;
  }

  @Override
  public String toString()
  {
    return "UnionQuery [context=" + context + ", queries=" + queries + "]";
  }

  @Override
  public DataSourceAnalysis getDataSourceAnalysis()
  {
    OpaqueDataSourceCover ds = new OpaqueDataSourceCover(new UnionDataSource(getDataSources()));
    return new DataSourceAnalysis(ds, null, null, Collections.emptyList());
  }

  private static class OpaqueDataSourceCover implements DataSource
  {
    private DataSource delegate;

    public OpaqueDataSourceCover(DataSource delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public Set<String> getTableNames()
    {
      return delegate.getTableNames();
    }

    @Override
    public List<DataSource> getChildren()
    {
      return delegate.getChildren();
    }

    @Override
    public DataSource withChildren(List<DataSource> children)
    {
      throw methodNotSupported();
    }

    @Override
    public boolean isCacheable(boolean isBroker)
    {
      return delegate.isCacheable(isBroker);
    }

    @Override
    public boolean isGlobal()
    {
      return delegate.isGlobal();
    }

    @Override
    public boolean isConcrete()
    {
      return delegate.isConcrete();
    }

    @Override
    public Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query, AtomicLong cpuTimeAcc)
    {
      throw methodNotSupported();
    }

    @Override
    public DataSource withUpdatedDataSource(DataSource newSource)
    {
      throw methodNotSupported();
    }

    @Override
    public byte[] getCacheKey()
    {
      return delegate.getCacheKey();
    }

    @Override
    public DataSourceAnalysis getAnalysis()
    {
      throw methodNotSupported();
    }
  }

  public static DruidException methodNotSupported()
  {
    return DruidException.defensive("Method not supported. This method is not expected to be called!");
  }

  @Override
  public RowSignature getResultRowSignature(Finalization finalization)
  {
    return queries.get(0).getResultRowSignature(finalization);
  }
}
