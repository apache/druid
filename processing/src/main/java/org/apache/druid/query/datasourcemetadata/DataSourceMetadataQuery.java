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

package org.apache.druid.query.datasourcemetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class DataSourceMetadataQuery extends BaseQuery<Result<DataSourceMetadataResultValue>>
{
  private static final QuerySegmentSpec DEFAULT_SEGMENT_SPEC = new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);

  @JsonCreator
  public DataSourceMetadataQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec == null ? DEFAULT_SEGMENT_SPEC : querySegmentSpec, false, context);
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
    return Query.DATASOURCE_METADATA;
  }

  @Override
  public DataSourceMetadataQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverrides);
    return Druids.DataSourceMetadataQueryBuilder.copy(this).context(newContext).build();
  }

  @Override
  public DataSourceMetadataQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return Druids.DataSourceMetadataQueryBuilder.copy(this).intervals(spec).build();
  }

  @Override
  public Query<Result<DataSourceMetadataResultValue>> withDataSource(DataSource dataSource)
  {
    return Druids.DataSourceMetadataQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  public Iterable<Result<DataSourceMetadataResultValue>> buildResult(DateTime timestamp, DateTime maxIngestedEventTime)
  {
    return Collections.singletonList(new Result<>(timestamp, new DataSourceMetadataResultValue(maxIngestedEventTime)));
  }

  public Iterable<Result<DataSourceMetadataResultValue>> mergeResults(
      List<Result<DataSourceMetadataResultValue>> results
  )
  {
    if (results == null || results.isEmpty()) {
      return new ArrayList<>();
    }

    DateTime max = DateTimes.MIN;
    for (Result<DataSourceMetadataResultValue> result : results) {
      DateTime currMaxIngestedEventTime = result.getValue().getMaxIngestedEventTime();
      if (currMaxIngestedEventTime != null && currMaxIngestedEventTime.isAfter(max)) {
        max = currMaxIngestedEventTime;
      }
    }

    return buildResult(max, max);
  }

  @Override
  public String toString()
  {
    return "DataSourceMetadataQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", duration=" + getDuration() +
        '}';
  }

}
