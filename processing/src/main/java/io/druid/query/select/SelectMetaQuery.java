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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Result;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("selectMeta")
public class SelectMetaQuery extends BaseQuery<Result<SelectMetaResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;

  @JsonCreator
  public SelectMetaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return SELECT_META;
  }

  @Override
  public SelectMetaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        contextOverride
    );
  }

  @Override
  public SelectMetaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SelectMetaQuery(getDataSource(), spec, getDimensionsFilter(), getGranularity(), getContext());
  }

  @Override
  public SelectMetaQuery withDataSource(DataSource dataSource)
  {
    return new SelectMetaQuery(
        dataSource,
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getContext()
    );
  }

  public SelectMetaQuery withDimFilter(DimFilter filter)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getGranularity(),
        getContext()
    );
  }

  public SelectMetaQuery withQueryGranularity(QueryGranularity granularity)
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        granularity,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SelectQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", descending=" + isDescending() +
           ", dimFilter=" + dimFilter +
           ", granularity=" + granularity +
           '}';
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
    if (!super.equals(o)) {
      return false;
    }

    SelectMetaQuery that = (SelectMetaQuery) o;

    if (!Objects.equals(dimFilter, that.dimFilter)) {
      return false;
    }
    if (!Objects.equals(granularity, that.granularity)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    return result;
  }
}
