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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Queries;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Builder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

  public static class OrderBy
  {
    private final String columnName;
    private final Order order;

    @JsonCreator
    public OrderBy(
        @JsonProperty("columnName") final String columnName,
        @JsonProperty("order") final Order order
    )
    {
      this.columnName = Preconditions.checkNotNull(columnName, "columnName");
      this.order = Preconditions.checkNotNull(order, "order");

      if (order == Order.NONE) {
        throw new IAE("Order required for column [%s]", columnName);
      }
    }

    @JsonProperty
    public String getColumnName()
    {
      return columnName;
    }

    @JsonProperty
    public Order getOrder()
    {
      return order;
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
      OrderBy that = (OrderBy) o;
      return Objects.equals(columnName, that.columnName) && order == that.order;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(columnName, order);
    }

    @Override
    public String toString()
    {
      return StringUtils.format("%s %s", columnName, order == Order.ASCENDING ? "ASC" : "DESC");
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
  public static final int DEFAULT_BATCH_SIZE = 4096 * 5;

  private final VirtualColumns virtualColumns;
  private final ResultFormat resultFormat;
  private final int batchSize;
  private final long scanRowsOffset;
  private final long scanRowsLimit;
  private final DimFilter dimFilter;
  private final List<String> columns;
  private final Boolean legacy;
  private final Order timeOrder;
  private final List<OrderBy> orderBys;
  private final Integer maxRowsQueuedForOrdering;
  private final Integer maxSegmentPartitionsOrderedInMemory;
  private final List<ColumnType> columnTypes;

  @JsonCreator
  public ScanQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("resultFormat") ResultFormat resultFormat,
      @JsonProperty("batchSize") int batchSize,
      @JsonProperty("offset") long scanRowsOffset,
      @JsonProperty("limit") long scanRowsLimit,
      @JsonProperty("order") Order orderFromUser,
      @JsonProperty("orderBy") List<OrderBy> orderBysFromUser,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("legacy") Boolean legacy,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("columnTypes") List<ColumnType> columnTypes
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
    this.resultFormat = (resultFormat == null) ? ResultFormat.RESULT_FORMAT_LIST : resultFormat;
    this.batchSize = (batchSize == 0) ? DEFAULT_BATCH_SIZE : batchSize;
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
    this.columnTypes = columnTypes;

    if (columnTypes != null) {
      Preconditions.checkNotNull(columns, "columns may not be null if columnTypes are specified");
      if (columns.size() != columnTypes.size()) {
        throw new IAE(
            "Inconsistent number of columns[%d] and columnTypes[%d] specified!",
            columns.size(),
            columnTypes.size()
        );
      }
    }

    final Pair<List<OrderBy>, Order> ordering = verifyAndReconcileOrdering(orderBysFromUser, orderFromUser);
    this.orderBys = Preconditions.checkNotNull(ordering.lhs);
    this.timeOrder = ordering.rhs;

    if (this.columns != null && this.columns.size() > 0) {
      // Validate orderBy. (Cannot validate when signature is empty, since that means "discover at runtime".)

      for (final OrderBy orderByColumn : this.orderBys) {
        if (!this.columns.contains(orderByColumn.getColumnName())) {
          // Error message depends on how the user originally specified ordering.
          if (orderBysFromUser != null) {
            throw new IAE("Column [%s] from 'orderBy' must also appear in 'columns'.", orderByColumn.getColumnName());
          } else {
            throw new IllegalArgumentException("The __time column must be selected if the results are time-ordered.");
          }
        }
      }
    }

    this.maxRowsQueuedForOrdering = validateAndGetMaxRowsQueuedForOrdering();
    this.maxSegmentPartitionsOrderedInMemory = validateAndGetMaxSegmentPartitionsOrderedInMemory();
  }

  /**
   * Verifies that the ordering of a query is solely determined by {@link #getTimeOrder()}. Required to actually
   * execute queries, because {@link #getOrderBys()} is not yet understood by the query engines.
   *
   * @throws IllegalStateException if the ordering is not solely determined by {@link #getTimeOrder()}
   */
  public static void verifyOrderByForNativeExecution(final ScanQuery query)
  {
    if (query.getTimeOrder() == Order.NONE && !query.getOrderBys().isEmpty()) {
      throw new ISE("Cannot execute query with orderBy %s", query.getOrderBys());
    }
  }

  private Integer validateAndGetMaxRowsQueuedForOrdering()
  {
    final Integer maxRowsQueuedForOrdering =
        context().getInt(ScanQueryConfig.CTX_KEY_MAX_ROWS_QUEUED_FOR_ORDERING);
    Preconditions.checkArgument(
        maxRowsQueuedForOrdering == null || maxRowsQueuedForOrdering > 0,
        "maxRowsQueuedForOrdering must be greater than 0"
    );
    return maxRowsQueuedForOrdering;
  }

  private Integer validateAndGetMaxSegmentPartitionsOrderedInMemory()
  {
    final Integer maxSegmentPartitionsOrderedInMemory =
        context().getInt(ScanQueryConfig.CTX_KEY_MAX_SEGMENT_PARTITIONS_FOR_ORDERING);
    Preconditions.checkArgument(
        maxSegmentPartitionsOrderedInMemory == null || maxSegmentPartitionsOrderedInMemory > 0,
        "maxRowsQueuedForOrdering must be greater than 0"
    );
    return maxSegmentPartitionsOrderedInMemory;
  }

  @JsonProperty
  @Override
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = VirtualColumns.JsonIncludeFilter.class)
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
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = BatchSizeJsonIncludeFilter.class)
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
   * situations where the user wants an effectively unlimited result set.
   */
  @JsonProperty("limit")
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = ScanRowsLimitJsonIncludeFilter.class)
  public long getScanRowsLimit()
  {
    return scanRowsLimit;
  }

  public OffsetLimit getOffsetLimit()
  {
    return new OffsetLimit(scanRowsOffset, scanRowsLimit);
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

  /**
   * If this query is purely-time-ordered, returns a value of the enum {@link Order}. Otherwise, returns
   * {@link Order#NONE}. If the returned value is {@link Order#NONE} it may not agree with {@link #getOrderBys()}.
   */
  @JsonProperty("order")
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = ScanTimeOrderJsonIncludeFilter.class)
  public Order getTimeOrder()
  {
    return timeOrder;
  }

  public List<OrderBy> getOrderBys()
  {
    return orderBys;
  }

  @Nullable
  @JsonProperty("orderBy")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<OrderBy> getOrderBysForJson()
  {
    // Return "orderBy" if necessary (meaning: if it is nonempty and nontime). Prevents polluting JSONs with
    // redundant "orderBy" and "order" fields.

    if (orderBys.size() > 1
        || (orderBys.size() == 1
            && !Iterables.getOnlyElement(orderBys).getColumnName().equals(ColumnHolder.TIME_COLUMN_NAME))) {
      return orderBys;
    } else {
      return null;
    }
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
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DimFilter getFilter()
  {
    return dimFilter;
  }

  @Override
  public String getType()
  {
    return SCAN;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<ColumnType> getColumnTypes()
  {
    return columnTypes;
  }

  /**
   * Compatibility mode with the legacy scan-query extension.
   *
   * True, false, and null have different meanings: true/false mean "legacy" and "not legacy"; null means use the
   * default set by {@link ScanQueryConfig#isLegacy()}. The method {@link #withNonNullLegacy} is provided to help
   * with this.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean isLegacy()
  {
    return legacy;
  }

  @Override
  public Ordering<ScanResultValue> getResultOrdering()
  {
    // No support yet for actually executing queries with non-time orderBy.
    verifyOrderByForNativeExecution(this);

    if (timeOrder == Order.NONE) {
      return Ordering.natural();
    }
    return Ordering.from(
        new ScanResultValueTimestampComparator(this).thenComparing(
            timeOrder == Order.ASCENDING
            ? Comparator.naturalOrder()
            : Comparator.<ScanResultValue>naturalOrder().reversed()
        )
    );
  }

  @Nullable
  @Override
  public Set<String> getRequiredColumns()
  {
    if (columns == null || columns.isEmpty()) {
      // We don't know what columns we require. We'll find out when the segment shows up.
      return null;
    } else {
      return Queries.computeRequiredColumns(
          virtualColumns,
          dimFilter,
          Collections.emptyList(),
          Collections.emptyList(),
          columns
      );
    }
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
  public ScanQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return Druids.ScanQueryBuilder.copy(this).intervals(querySegmentSpec).build();
  }

  @Override
  public ScanQuery withDataSource(DataSource dataSource)
  {
    return Druids.ScanQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  @Override
  public ScanQuery withOverriddenContext(Map<String, Object> contextOverrides)
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
           Objects.equals(columns, scanQuery.columns) &&
           Objects.equals(orderBys, scanQuery.orderBys);
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
        orderBys,
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
           (orderBys.isEmpty() ? "" : ", orderBy=" + orderBys) +
           (legacy == null ? "" : ", legacy=" + legacy) +
           ", context=" + getContext() +
           '}';
  }

  /**
   * Verify and reconcile the two ways of specifying ordering: "orderBy", which can refer to any column, and
   * "order", which refers to the __time column.
   *
   * If only "order" is provided, it is returned as-is, along with an equivalent "orderBy".
   *
   * If only "orderBy" is provided, it is returned as-is. If it can be converted into an equivalent "order", then that
   * equivalent "order" is also returned. Otherwise, "orderBy" is returned as-is and "order" is returned as NONE.
   *
   * If both "orderBy" and "order" are provided, this returns them as-is if they are compatible, or throws an
   * exception if they are incompatible.
   *
   * @param orderByFromUser "orderBy" specified by the user (can refer to any column)
   * @param orderFromUser   "order" specified by the user (refers to time order)
   */
  private static Pair<List<OrderBy>, Order> verifyAndReconcileOrdering(
      @Nullable final List<OrderBy> orderByFromUser,
      @Nullable final Order orderFromUser
  )
  {
    final List<OrderBy> orderByRetVal;
    final Order orderRetVal;

    // Compute the returned orderBy.
    if (orderByFromUser != null) {
      orderByRetVal = orderByFromUser;
    } else if (orderFromUser == null || orderFromUser == Order.NONE) {
      orderByRetVal = Collections.emptyList();
    } else {
      orderByRetVal = Collections.singletonList(new OrderBy(ColumnHolder.TIME_COLUMN_NAME, orderFromUser));
    }

    // Compute the returned order.
    orderRetVal = computeTimeOrderFromOrderBys(orderByRetVal);

    // Verify compatibility, if the user specified both kinds of ordering.
    if (orderFromUser != null && orderFromUser != Order.NONE && orderRetVal != orderFromUser) {
      throw new IAE("Cannot provide 'order' incompatible with 'orderBy'");
    }

    return Pair.of(orderByRetVal, orderRetVal);
  }

  /**
   * Compute time ordering based on a list of orderBys.
   *
   * Returns {@link Order#ASCENDING} or {@link Order#DESCENDING} if the ordering is time-based; returns
   * {@link Order#NONE} otherwise. Importantly, this means that the returned order is not necessarily compatible
   * with the input orderBys.
   */
  @Nullable
  private static Order computeTimeOrderFromOrderBys(final List<OrderBy> orderBys)
  {
    if (orderBys.size() == 1) {
      final OrderBy orderByColumn = Iterables.getOnlyElement(orderBys);

      if (ColumnHolder.TIME_COLUMN_NAME.equals(orderByColumn.getColumnName())) {
        return orderByColumn.getOrder();
      }
    }

    return Order.NONE;
  }

  /**
   * {@link JsonInclude} filter for {@link #getTimeOrder()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
   * exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode"})
  static class ScanTimeOrderJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Order && Order.NONE.equals(obj);
    }
  }

  /**
   * {@link JsonInclude} filter for {@link #getScanRowsLimit()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
   * exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode"})
  static class ScanRowsLimitJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Long && (long) obj == Long.MAX_VALUE;
    }
  }

  /**
   * {@link JsonInclude} filter for {@link #getBatchSize()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
   * exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode"})
  static class BatchSizeJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Integer && (int) obj == DEFAULT_BATCH_SIZE;
    }
  }


  /**
   * Returns the RowSignature.
   *
   * If {@link ScanQuery#columnTypes} is not available it will do its best to fill in the types.
   */
  @Nullable
  public RowSignature getRowSignature()
  {
    return getRowSignature(false);
  }

  @Nullable
  public RowSignature getRowSignature(boolean defaultIsLegacy)
  {
    if (columns == null || columns.isEmpty()) {
      // Note: if no specific list of columns is provided, then since we can't predict what columns will come back, we
      // unfortunately can't do array-based results. In this case, there is a major difference between standard and
      // array-based results: the standard results will detect and return _all_ columns, whereas the array-based results
      // will include none of them.
      return RowSignature.empty();
    }
    if (columnTypes != null) {
      Builder builder = RowSignature.builder();
      for (int i = 0; i < columnTypes.size(); i++) {
        builder.add(columns.get(i), columnTypes.get(i));
      }
      return builder.build();
    }
    return guessRowSignature(defaultIsLegacy);
  }

  private RowSignature guessRowSignature(boolean defaultIsLegacy)
  {
    final RowSignature.Builder builder = RowSignature.builder();
    if (Boolean.TRUE.equals(legacy) || (legacy == null && defaultIsLegacy)) {
      builder.add(ScanQueryEngine.LEGACY_TIMESTAMP_KEY, null);
    }
    DataSource dataSource = getDataSource();
    for (String columnName : columns) {
      final ColumnType columnType = guessColumnType(columnName, virtualColumns, dataSource);
      builder.add(columnName, columnType);
    }
    return builder.build();
  }

  /**
   * Tries to guess the {@link ColumnType} from the {@link VirtualColumns} and the {@link DataSource}.
   *
   * We know the columnType for virtual columns and in some cases the columntypes of the datasource as well.
   */
  @Nullable
  private static ColumnType guessColumnType(String columnName, VirtualColumns virtualColumns, DataSource dataSource)
  {
    final VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(columnName);
    if (virtualColumn != null) {
      final ColumnCapabilities capabilities = virtualColumn.capabilities(c -> null, columnName);
      if (capabilities != null) {
        return capabilities.toColumnType();
      }
    } else {
      if (dataSource instanceof InlineDataSource) {
        InlineDataSource inlineDataSource = (InlineDataSource) dataSource;
        ColumnCapabilities caps = inlineDataSource.getRowSignature().getColumnCapabilities(columnName);
        if (caps != null) {
          return caps.toColumnType();
        }
      }
    }
    // Unknown type. In the future, it would be nice to have a way to fill these in.
    return null;
  }
}
