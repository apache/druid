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

package io.druid.query.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MaterializedViewQuery<T> implements Query<T> 
{
  public static final String VIEW = "view";
  private final Query query;
  
  @JsonCreator
  public MaterializedViewQuery(
      @JsonProperty("query") Query query
  )
  {
    this.query = query;
  }
  
  @JsonProperty("query")
  public Query getQuery()
  {
    return query;
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
    return new MaterializedViewQuery(query.withOverriddenContext(contextOverride));
  }

  @Override
  public MaterializedViewQuery withQuerySegmentSpec(QuerySegmentSpec spec) 
  {
    return new MaterializedViewQuery(query.withQuerySegmentSpec(spec));
  }

  @Override
  public MaterializedViewQuery withId(String id)
  {
    return new MaterializedViewQuery(query.withId(id));
  }

  @Override
  public String getId()
  {
    return query.getId();
  }

  @Override
  public MaterializedViewQuery withDataSource(DataSource dataSource) 
  {
    return new MaterializedViewQuery(query.withDataSource(dataSource));
  }

  @Override
  public String toString()
  {
    return "MaterializedViewQuery{" +
        "query=" + query.toString() +
        "}";
  }
  
  public Query getRealQuery()
  {
    return query;
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
    return other.getRealQuery().equals(query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(VIEW) + query.hashCode();
  }

}
