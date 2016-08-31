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
package io.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

@JsonTypeName("scan")
public class ScanQuery extends BaseQuery<ScanResultValue>
{
  public static final String SCAN = "scan";

  private final String resultFormat;
  private final int batchSize;
  private final int limit;
  private final DimFilter dimFilter;
  private final List<String> columns;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("limit") int limit,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.resultFormat = resultFormat;
    this.batchSize = (batchSize == 0) ? 4096 * 5 : batchSize;
    this.limit = (limit == 0) ? Integer.MAX_VALUE : limit;
    this.dimFilter = dimFilter;
    this.columns = columns;
  }

  @JsonProperty
  public String getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public int getBatchSize()
  {
    return batchSize;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SCAN;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  public Query<ScanResultValue> withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new ScanQuery(
        getDataSource(),
        querySegmentSpec,
        resultFormat,
        batchSize,
        limit,
        dimFilter,
        columns,
        getContext()
    );
  }

  @Override
  public Query<ScanResultValue> withDataSource(DataSource dataSource)
  {
    return new ScanQuery(
        dataSource,
        getQuerySegmentSpec(),
        resultFormat,
        batchSize,
        limit,
        dimFilter,
        columns,
        getContext()
    );
  }

  @Override
  public Query<ScanResultValue> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new ScanQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        resultFormat,
        batchSize,
        limit,
        dimFilter,
        columns,
        computeOverridenContext(contextOverrides)
    );
  }

  public ScanQuery withDimFilter(DimFilter dimFilter)
  {
    return new ScanQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        resultFormat,
        batchSize,
        limit,
        dimFilter,
        columns,
        getContext()
    );
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

    ScanQuery that = (ScanQuery) o;

    if (batchSize != that.batchSize) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }
    if (resultFormat != null ? !resultFormat.equals(that.resultFormat) : that.resultFormat != null) {
      return false;
    }
    if (dimFilter != null ? !dimFilter.equals(that.dimFilter) : that.dimFilter != null) {
      return false;
    }
    return columns != null ? columns.equals(that.columns) : that.columns == null;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (resultFormat != null ? resultFormat.hashCode() : 0);
    result = 31 * result + batchSize;
    result = 31 * result + limit;
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (columns != null ? columns.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ScanQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", descending=" + isDescending() +
           ", resultFormat='" + resultFormat + '\'' +
           ", batchSize=" + batchSize +
           ", limit=" + limit +
           ", dimFilter=" + dimFilter +
           ", columns=" + columns +
           '}';
  }
}
