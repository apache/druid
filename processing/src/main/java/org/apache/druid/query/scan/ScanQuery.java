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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ScanQuery extends BaseQuery<ScanResultValue>
{
  public static final String RESULT_FORMAT_LIST = "list";
  public static final String RESULT_FORMAT_COMPACTED_LIST = "compactedList";
  public static final String RESULT_FORMAT_VALUE_VECTOR = "valueVector";

  public static final String TIME_ORDER_ASCENDING = "ascending";
  public static final String TIME_ORDER_DESCENDING = "descending";
  public static final String TIME_ORDER_NONE = "none";

  private final VirtualColumns virtualColumns;
  private final String resultFormat;
  private final int batchSize;
  private final long limit;
  private final DimFilter dimFilter;
  private final List<String> columns;
  private final Boolean legacy;
  private final String timeOrder;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("limit") long limit,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("legacy") Boolean legacy,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("timeOrder") String timeOrder
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.resultFormat = resultFormat == null ? RESULT_FORMAT_LIST : resultFormat;
    this.batchSize = (batchSize == 0) ? 4096 * 5 : batchSize;
    this.limit = (limit == 0) ? Long.MAX_VALUE : limit;
    Preconditions.checkArgument(this.batchSize > 0, "batchSize must be greater than 0");
    Preconditions.checkArgument(this.limit > 0, "limit must be greater than 0");
    this.dimFilter = dimFilter;
    this.columns = columns;
    this.legacy = legacy;
    this.timeOrder = timeOrder == null ? TIME_ORDER_NONE : timeOrder;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
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
  public long getLimit()
  {
    return limit;
  }

  @JsonProperty
  public String getTimeOrder()
  {
    return timeOrder;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  @JsonProperty
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SCAN;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  /**
   * Compatibility mode with the legacy scan-query extension.
   */
  @JsonProperty
  public Boolean isLegacy()
  {
    return legacy;
  }

  public ScanQuery withNonNullLegacy(final ScanQueryConfig scanQueryConfig)
  {
    return Druids.ScanQueryBuilder.copy(this).legacy(legacy != null ? legacy : scanQueryConfig.isLegacy()).build();
  }

  @Override
  public Query<ScanResultValue> withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return Druids.ScanQueryBuilder.copy(this).intervals(querySegmentSpec).build();
  }

  @Override
  public Query<ScanResultValue> withDataSource(DataSource dataSource)
  {
    return Druids.ScanQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public Query<ScanResultValue> withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return Druids.ScanQueryBuilder.copy(this).context(computeOverriddenContext(getContext(), contextOverrides)).build();
  }

  public ScanQuery withDimFilter(DimFilter dimFilter)
  {
    return Druids.ScanQueryBuilder.copy(this).filters(dimFilter).build();
  }

  @Override
  public boolean equals(final Object o)
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
    final ScanQuery scanQuery = (ScanQuery) o;
    return batchSize == scanQuery.batchSize &&
           limit == scanQuery.limit &&
           legacy == scanQuery.legacy &&
           Objects.equals(virtualColumns, scanQuery.virtualColumns) &&
           Objects.equals(resultFormat, scanQuery.resultFormat) &&
           Objects.equals(dimFilter, scanQuery.dimFilter) &&
           Objects.equals(columns, scanQuery.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), virtualColumns, resultFormat, batchSize, limit, dimFilter, columns, legacy);
  }

  @Override
  public String toString()
  {
    return "ScanQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", virtualColumns=" + getVirtualColumns() +
           ", resultFormat='" + resultFormat + '\'' +
           ", batchSize=" + batchSize +
           ", limit=" + limit +
           ", dimFilter=" + dimFilter +
           ", columns=" + columns +
           ", legacy=" + legacy +
           '}';
  }
}
