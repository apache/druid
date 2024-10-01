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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UnionQuery<T> implements Query<T>
{
  protected final Map<String, Object> context;
  protected final List<Query<?>> queries;

  public UnionQuery(List<Query<?>> queries2)
  {
    this(queries2, queries2.get(0).getContext());
  }

  public UnionQuery(List<Query<?>> queries, Map<String, Object> context)
  {
    this.queries = queries;
    this.context = context;
  }

  @Override
  public DataSource getDataSource()
  {
    throw new RuntimeException("This is not supported");
  }

  @Override
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
  public QueryRunner<T> getRunner(QuerySegmentWalker walker)
  {
    return new RealUnionQueryRunner<T>(this, walker);
  }

  @Override
  public List<Interval> getIntervals()
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Override
  public Duration getDuration()
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Override
  public Granularity getGranularity()
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Override
  public DateTimeZone getTimezone()
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Override
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Query<T> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new UnionQuery<T>(queries, QueryContexts.override(getContext(), contextOverrides));
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Override
  public Query<T> withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.QUERY_ID, id));
  }

  @Override
  public String getId()
  {
    return context().getString(BaseQuery.QUERY_ID);
  }

  @Override
  public Query<T> withSubQueryId(String subQueryId)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.SUB_QUERY_ID, subQueryId));
  }

  @Override
  public String getSubQueryId()
  {
    return context().getString(BaseQuery.SUB_QUERY_ID);
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }
}
