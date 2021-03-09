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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ScanQuery extends BaseQuery<ScanResultValue>
{
  public enum ResultFormat
  {
    RESULT_FORMAT_LIST,
    RESULT_FORMAT_COMPACTED_LIST,
    RESULT_FORMAT_VALUE_VECTOR;

    @JsonValue
    @Override
    public String toString()
    {
      switch (this) {
        case RESULT_FORMAT_LIST:
          return "list";
        case RESULT_FORMAT_COMPACTED_LIST:
          return "compactedList";
        case RESULT_FORMAT_VALUE_VECTOR:
          return "valueVector";
        default:
          return "";
      }
    }

    @JsonCreator
    public static ResultFormat fromString(String name)
    {
      switch (name) {
        case "compactedList":
          return RESULT_FORMAT_COMPACTED_LIST;
        case "valueVector":
          return RESULT_FORMAT_VALUE_VECTOR;
        case "list":
          return RESULT_FORMAT_LIST;
        default:
          throw new UOE("Scan query result format [%s] is not supported.", name);
      }
    }
  }

  public enum Order
  {
    ASCENDING,
    DESCENDING,
    NONE;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static Order fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  /**
   * This context flag corresponds to whether the query is running on the "outermost" process (i.e. the process
   * the query is sent to).
   */
  public static final String CTX_KEY_OUTERMOST = "scanOutermost";

  private final VirtualColumns virtualColumns;
  private final ResultFormat resultFormat;
  private final int batchSize;
  private final long scanRowsOffset;
  private final long scanRowsLimit;
  private final DimFilter dimFilter;
  private final List<String> columns;
  private final Boolean legacy;
  private final Order order;
  private final Integer maxRowsQueuedForOrdering;
  private final Integer maxSegmentPartitionsOrderedInMemory;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("resultFormat") ResultFormat resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("offset") long scanRowsOffset,
      @JsonProperty("limit") long scanRowsLimit,
      @JsonProperty("order") Order order,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("legacy") Boolean legacy,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.resultFormat = (resultFormat == null) ? ResultFormat.RESULT_FORMAT_LIST : resultFormat;
    this.batchSize = (batchSize == 0) ? 4096 * 5 : batchSize;
    Preconditions.checkArgument(
        this.batchSize > 0,
        "batchSize must be greater than 0"
    );
    this.scanRowsOffset = scanRowsOffset;
    Preconditions.checkArgument(
        this.scanRowsOffset >= 0,
        "offset must be greater than or equal to 0"
    );
    this.scanRowsLimit = (scanRowsLimit == 0) ? Long.MAX_VALUE : scanRowsLimit;
    Preconditions.checkArgument(
        this.scanRowsLimit > 0,
        "limit must be greater than 0"
    );
    this.dimFilter = dimFilter;
    this.columns = columns;
    this.legacy = legacy;
    this.order = (order == null) ? Order.NONE : order;
    if (this.order != Order.NONE) {
      Preconditions.checkArgument(
          columns == null || columns.size() == 0 || columns.contains(ColumnHolder.TIME_COLUMN_NAME),
          "The __time column must be selected if the results are time-ordered."
      );
    }
    this.maxRowsQueuedForOrdering = validateAndGetMaxRowsQueuedForOrdering();
    this.maxSegmentPartitionsOrderedInMemory = validateAndGetMaxSegmentPartitionsOrderedInMemory();
  }

  private Integer validateAndGetMaxRowsQueuedForOrdering()
  {
    final Integer maxRowsQueuedForOrdering =
        getContextValue(ScanQueryConfig.CTX_KEY_MAX_ROWS_QUEUED_FOR_ORDERING, null);
    Preconditions.checkArgument(
        maxRowsQueuedForOrdering == null || maxRowsQueuedForOrdering > 0,
        "maxRowsQueuedForOrdering must be greater than 0"
    );
    return maxRowsQueuedForOrdering;
  }

  private Integer validateAndGetMaxSegmentPartitionsOrderedInMemory()
  {
    final Integer maxSegmentPartitionsOrderedInMemory =
        getContextValue(ScanQueryConfig.CTX_KEY_MAX_SEGMENT_PARTITIONS_FOR_ORDERING, null);
    Preconditions.checkArgument(
        maxSegmentPartitionsOrderedInMemory == null || maxSegmentPartitionsOrderedInMemory > 0,
        "maxRowsQueuedForOrdering must be greater than 0"
    );
    return maxSegmentPartitionsOrderedInMemory;
  }

  @JsonProperty
  @Override
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Offset for this query; behaves like SQL "OFFSET". Zero means no offset. Negative values are invalid.
   */
  @JsonProperty("offset")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getScanRowsOffset()
  {
    return scanRowsOffset;
  }

  /**
   * Limit for this query; behaves like SQL "LIMIT". Will always be positive. {@link Long#MAX_VALUE} is used in
   * situations where the user wants an effectively unlimited resultset.
   */
  @JsonProperty("limit")
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = ScanRowsLimitJsonIncludeFilter.class)
  public long getScanRowsLimit()
  {
    return scanRowsLimit;
  }

  /**
   * Returns whether this query is limited or not. Because {@link Long#MAX_VALUE} is used to signify unlimitedness,
   * this is equivalent to {@code getScanRowsLimit() != Long.Max_VALUE}.
   *
   * @see #getScanRowsLimit()
   */
  public boolean isLimited()
  {
    return scanRowsLimit != Long.MAX_VALUE;
  }

  @JsonProperty
  public Order getOrder()
  {
    return order;
  }

  @Nullable
  @JsonIgnore
  public Integer getMaxRowsQueuedForOrdering()
  {
    return maxRowsQueuedForOrdering;
  }

  @Nullable
  @JsonIgnore
  public Integer getMaxSegmentPartitionsOrderedInMemory()
  {
    return maxSegmentPartitionsOrderedInMemory;
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

  @Override
  public Ordering<ScanResultValue> getResultOrdering()
  {
    if (order == Order.NONE) {
      return Ordering.natural();
    }
    return Ordering.from(
        new ScanResultValueTimestampComparator(this).thenComparing(
            order == Order.ASCENDING
            ? Comparator.naturalOrder()
            : Comparator.<ScanResultValue>naturalOrder().reversed()
        )
    );
  }

  public ScanQuery withOffset(final long newOffset)
  {
    return Druids.ScanQueryBuilder.copy(this).offset(newOffset).build();
  }

  public ScanQuery withLimit(final long newLimit)
  {
    return Druids.ScanQueryBuilder.copy(this).limit(newLimit).build();
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
    final ScanQuery scanQuery = (ScanQuery) o;
    return batchSize == scanQuery.batchSize &&
           scanRowsOffset == scanQuery.scanRowsOffset &&
           scanRowsLimit == scanQuery.scanRowsLimit &&
           Objects.equals(legacy, scanQuery.legacy) &&
           Objects.equals(virtualColumns, scanQuery.virtualColumns) &&
           Objects.equals(resultFormat, scanQuery.resultFormat) &&
           Objects.equals(dimFilter, scanQuery.dimFilter) &&
           Objects.equals(columns, scanQuery.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        virtualColumns,
        resultFormat,
        batchSize,
        scanRowsOffset,
        scanRowsLimit,
        dimFilter,
        columns,
        legacy
    );
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
           ", offset=" + scanRowsOffset +
           ", limit=" + scanRowsLimit +
           ", dimFilter=" + dimFilter +
           ", columns=" + columns +
           ", legacy=" + legacy +
           '}';
  }

  /**
   * {@link JsonInclude} filter for {@link #getScanRowsLimit()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
   * exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode"})
  static class ScanRowsLimitJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }

      if (obj.getClass() == this.getClass()) {
        return true;
      }

      return obj instanceof Long && (long) obj == Long.MAX_VALUE;
    }
  }
}
