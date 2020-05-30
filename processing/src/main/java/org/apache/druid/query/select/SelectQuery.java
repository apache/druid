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

package org.apache.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Deprecated
public class SelectQuery implements Query<Object>
{
  static final String REMOVED_ERROR_MESSAGE =
      "The 'select' query has been removed, use 'scan' instead. See "
      + "https://druid.apache.org/docs/latest/querying/select-query.html for more details.";

  @JsonCreator
  public SelectQuery()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public DataSource getDataSource()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public boolean hasFilters()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public DimFilter getFilter()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public String getType()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public QueryRunner<Object> getRunner(QuerySegmentWalker walker)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public List<Interval> getIntervals()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Duration getDuration()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Granularity getGranularity()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public DateTimeZone getTimezone()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Map<String, Object> getContext()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public boolean isDescending()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Ordering<Object> getResultOrdering()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Query<Object> withOverriddenContext(Map<String, Object> contextOverride)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Query<Object> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Query<Object> withId(String id)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public String getId()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Query<Object> withSubQueryId(String subQueryId)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Nullable
  @Override
  public String getSubQueryId()
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }

  @Override
  public Query<Object> withDataSource(DataSource dataSource)
  {
    throw new RuntimeException(REMOVED_ERROR_MESSAGE);
  }
}
