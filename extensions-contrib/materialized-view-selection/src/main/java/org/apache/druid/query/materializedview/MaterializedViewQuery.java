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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaterializedViewQuery helps to do materialized view selection automatically. 
 * 
 * Each MaterializedViewQuery contains a real query which type can be topn, timeseries or groupBy.
 * The real query will be optimized based on its dataSources and intervals. It will be converted into one or more
 * sub-queries, in which dataSources and intervals are replaced by derived dataSources and related sub-intervals.
 * 
 * Derived dataSources always have less dimensions, but contains all dimensions which real query required.
 */
public class MaterializedViewQuery<T> implements Query<T> 
{
  public static final String TYPE = "view";
  private final Query query;
  private final DataSourceOptimizer optimizer;
  
  @JsonCreator
  public MaterializedViewQuery(
      @JsonProperty("query") Query query,
      @JacksonInject DataSourceOptimizer optimizer
  )
  {
    Preconditions.checkArgument(
        query instanceof TopNQuery || query instanceof TimeseriesQuery || query instanceof GroupByQuery,
        "Only topN/timeseries/groupby query are supported"
    );
    this.query = query;
    this.optimizer = optimizer;
  }
  
  @JsonProperty("query")
  public Query getQuery()
  {
    return query;
  }
  
  public DataSourceOptimizer getOptimizer()
  {
    return optimizer;
  }
  
  @Override
  public DataSource getDataSource()
  {
    return query.getDataSource();
  }
  
  @Override
  public boolean hasFilters()
  {
    return query.hasFilters();
  }

  @Override
  public DimFilter getFilter()
  {
    return query.getFilter();
  }

  @Override
  public String getType()
  {
    return query.getType();
  }

  @Override
  public QueryRunner<T> getRunner(QuerySegmentWalker walker) 
  {
    return ((BaseQuery) query).getQuerySegmentSpec().lookup(this, walker);
  }

  @Override
  public List<Interval> getIntervals()
      
  {
    return query.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    return query.getDuration();
  }

  @Override
  public Granularity getGranularity()
  {
    return query.getGranularity();
  }

  @Override
  public DateTimeZone getTimezone()
  {
    return query.getTimezone();
  }

  @Override
  public Map<String, Object> getContext()
  {
    return query.getContext();
  }

  @Override
  public <ContextType> ContextType getContextValue(String key)
  {
    return (ContextType) query.getContextValue(key);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue) 
  {
    return (ContextType) query.getContextValue(key, defaultValue);
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue) 
  {
    return query.getContextBoolean(key, defaultValue);
  }

  @Override
  public boolean isDescending()
  {
    return query.isDescending();
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    return query.getResultOrdering();
  }

  @Override
  public MaterializedViewQuery withOverriddenContext(Map<String, Object> contextOverride) 
  {
    return new MaterializedViewQuery(query.withOverriddenContext(contextOverride), optimizer);
  }

  @Override
  public MaterializedViewQuery withQuerySegmentSpec(QuerySegmentSpec spec) 
  {
    return new MaterializedViewQuery(query.withQuerySegmentSpec(spec), optimizer);
  }

  @Override
  public MaterializedViewQuery withId(String id)
  {
    return new MaterializedViewQuery(query.withId(id), optimizer);
  }

  @Override
  public String getId()
  {
    return query.getId();
  }

  @Override
  public Query<T> withSubQueryId(String subQueryId)
  {
    return new MaterializedViewQuery<>(query.withSubQueryId(subQueryId), optimizer);
  }

  @Nullable
  @Override
  public String getSubQueryId()
  {
    return query.getSubQueryId();
  }

  @Override
  public MaterializedViewQuery withDataSource(DataSource dataSource) 
  {
    return new MaterializedViewQuery(query.withDataSource(dataSource), optimizer);
  }

  @Override
  public String toString()
  {
    return "MaterializedViewQuery{" +
           "query=" + query +
           "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaterializedViewQuery other = (MaterializedViewQuery) o;
    return other.getQuery().equals(query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(TYPE, query);
  }

}
