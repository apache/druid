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

package org.apache.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.TopNSequence;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x1;

  private final List<OrderByColumnSpec> columns;
  private final int offset;
  private final int limit;

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Check if a limitSpec has columns in the sorting order that are not part of the grouping fields represented
   * by `dimensions`.
   *
   * @param limitSpec  LimitSpec, assumed to be non-null
   * @param dimensions Grouping fields for a groupBy query
   *
   * @return True if limitSpec has sorting columns not contained in dimensions
   */
  public static boolean sortingOrderHasNonGroupingFields(DefaultLimitSpec limitSpec, List<DimensionSpec> dimensions)
  {
    for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
      int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
      if (dimIndex < 0) {
        return true;
      }
    }
    return false;
  }

  public static StringComparator getComparatorForDimName(DefaultLimitSpec limitSpec, String dimName)
  {
    final OrderByColumnSpec orderBy = OrderByColumnSpec.getOrderByForDimName(limitSpec.getColumns(), dimName);
    if (orderBy == null) {
      return null;
    }

    return orderBy.getDimensionComparator();
  }

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("offset") Integer offset,
      @JsonProperty("limit") Integer limit
  )
  {
    this.columns = (columns == null) ? ImmutableList.of() : columns;
    this.offset = (offset == null) ? 0 : offset;
    this.limit = (limit == null) ? Integer.MAX_VALUE : limit;

    Preconditions.checkArgument(this.offset >= 0, "offset[%s] must be >= 0", this.offset);
    Preconditions.checkArgument(this.limit > 0, "limit[%s] must be > 0", this.limit);
  }

  /**
   * Constructor that does not accept "offset". Useful for tests that only want to provide "columns" and "limit".
   */
  @VisibleForTesting
  public DefaultLimitSpec(
      final List<OrderByColumnSpec> columns,
      final Integer limit
  )
  {
    this(columns, 0, limit);
  }

  @JsonProperty
  public List<OrderByColumnSpec> getColumns()
  {
    return columns;
  }

  /**
   * Offset for this query; behaves like SQL "OFFSET". Zero means no offset. Negative values are invalid.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getOffset()
  {
    return offset;
  }

  /**
   * Limit for this query; behaves like SQL "LIMIT". Will always be positive. {@link Integer#MAX_VALUE} is used in
   * situations where the user wants an effectively unlimited result set.
   */
  @JsonProperty
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = LimitJsonIncludeFilter.class)
  public int getLimit()
  {
    return limit;
  }

  public boolean isOffset()
  {
    return offset > 0;
  }

  public boolean isLimited()
  {
    return limit < Integer.MAX_VALUE;
  }

  @Override
  public Function<Sequence<ResultRow>, Sequence<ResultRow>> build(final GroupByQuery query)
  {
    final List<DimensionSpec> dimensions = query.getDimensions();

    // Can avoid re-sorting if the natural ordering is good enough.
    boolean sortingNeeded = dimensions.size() < columns.size();

    final Set<String> aggAndPostAggNames = new HashSet<>();
    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
      aggAndPostAggNames.add(agg.getName());
    }
    for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
      aggAndPostAggNames.add(postAgg.getName());
    }

    if (!sortingNeeded) {
      for (int i = 0; i < columns.size(); i++) {
        final OrderByColumnSpec columnSpec = columns.get(i);

        if (aggAndPostAggNames.contains(columnSpec.getDimension())) {
          sortingNeeded = true;
          break;
        }

        final ColumnType columnType = getOrderByType(columnSpec, dimensions);
        final StringComparator naturalComparator;
        if (columnType.is(ValueType.STRING)) {
          naturalComparator = StringComparators.LEXICOGRAPHIC;
        } else if (columnType.isNumeric()) {
          naturalComparator = StringComparators.NUMERIC;
        } else if (columnType.isArray()) {
          if (columnType.getElementType().isNumeric()) {
            naturalComparator = StringComparators.NUMERIC;
          } else {
            naturalComparator = StringComparators.LEXICOGRAPHIC;
          }
        } else {
          sortingNeeded = true;
          break;
        }

        if (columnSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING
            || !columnSpec.getDimensionComparator().equals(naturalComparator)
            || !columnSpec.getDimension().equals(dimensions.get(i).getOutputName())) {
          sortingNeeded = true;
          break;
        }
      }
    }

    if (!sortingNeeded) {
      // If granularity is ALL, sortByDimsFirst doesn't change the sorting order.
      sortingNeeded = !query.getGranularity().equals(Granularities.ALL) && query.getContextSortByDimsFirst();
    }

    if (!sortingNeeded) {
      final QueryContext queryContext = query.context();
      String timestampField = queryContext.getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD);
      if (timestampField != null && !timestampField.isEmpty()) {
        // Will NPE if the key is not set
        int timestampResultFieldIndex = queryContext.getInt(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX);
        sortingNeeded = query.getContextSortByDimsFirst()
                        ? timestampResultFieldIndex != query.getDimensions().size() - 1
                        : timestampResultFieldIndex != 0;
      }
    }

    final Function<Sequence<ResultRow>, Sequence<ResultRow>> sortAndLimitFn;

    if (sortingNeeded) {
      // Materialize the Comparator first for fast-fail error checking.
      final Ordering<ResultRow> ordering = makeComparator(
          query.getResultRowSignature(),
          query.getResultRowHasTimestamp(),
          query.getDimensions(),
          query.getAggregatorSpecs(),
          query.getPostAggregatorSpecs(),
          query.getContextSortByDimsFirst()
      );

      // Both branches use a stable sort; important so consistent results are returned from query to query if the
      // underlying data isn't changing. (Useful for query reproducibility and offset-based pagination.)
      if (isLimited()) {
        sortAndLimitFn = results -> new TopNSequence<>(results, ordering, limit + offset);
      } else {
        sortAndLimitFn = results -> Sequences.sort(results, ordering).limit(limit + offset);
      }
    } else {
      if (isLimited()) {
        sortAndLimitFn = results -> results.limit(limit + offset);
      } else {
        sortAndLimitFn = Functions.identity();
      }
    }

    // Finally, apply offset after sorting and limiting.
    if (isOffset()) {
      return results -> sortAndLimitFn.apply(results).skip(offset);
    } else {
      return sortAndLimitFn;
    }
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return this;
  }

  private ColumnType getOrderByType(final OrderByColumnSpec columnSpec, final List<DimensionSpec> dimensions)
  {
    for (DimensionSpec dimSpec : dimensions) {
      if (columnSpec.getDimension().equals(dimSpec.getOutputName())) {
        return dimSpec.getOutputType();
      }
    }

    throw new ISE("Unknown column in order clause[%s]", columnSpec);
  }

  @Override
  public LimitSpec filterColumns(Set<String> names)
  {
    return new DefaultLimitSpec(
        columns.stream().filter(c -> names.contains(c.getDimension())).collect(Collectors.toList()),
        offset,
        limit
    );
  }

  /**
   * Returns a new DefaultLimitSpec identical to this one except for one difference: an offset parameter, if any, will
   * be removed and added to the limit. This is designed for passing down queries to lower levels of the stack. Only
   * the highest level should apply the offset parameter, and any pushed-down limits must be increased to accommodate
   * the offset.
   */
  public DefaultLimitSpec withOffsetToLimit()
  {
    if (isOffset()) {
      final int newLimit;

      if (limit == Integer.MAX_VALUE) {
        // Unlimited stays unlimited.
        newLimit = Integer.MAX_VALUE;
      } else if (limit > Integer.MAX_VALUE - offset) {
        // Handle overflow as best we can.
        throw new ISE("Cannot apply limit[%d] with offset[%d] due to overflow", limit, offset);
      } else {
        newLimit = limit + offset;
      }

      return new DefaultLimitSpec(columns, 0, newLimit);
    } else {
      return this;
    }
  }

  private Ordering<ResultRow> makeComparator(
      RowSignature rowSignature,
      boolean hasTimestamp,
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs,
      boolean sortByDimsFirst
  )
  {
    final Ordering<ResultRow> timeOrdering;

    if (hasTimestamp) {
      timeOrdering = new Ordering<ResultRow>()
      {
        @Override
        public int compare(ResultRow left, ResultRow right)
        {
          return Longs.compare(left.getLong(0), right.getLong(0));
        }
      };
    } else {
      timeOrdering = null;
    }

    Map<String, DimensionSpec> dimensionsMap = new HashMap<>();
    for (DimensionSpec spec : dimensions) {
      dimensionsMap.put(spec.getOutputName(), spec);
    }

    Map<String, AggregatorFactory> aggregatorsMap = new HashMap<>();
    for (final AggregatorFactory agg : aggs) {
      aggregatorsMap.put(agg.getName(), agg);
    }

    Map<String, PostAggregator> postAggregatorsMap = new HashMap<>();
    for (PostAggregator postAgg : postAggs) {
      postAggregatorsMap.put(postAgg.getName(), postAgg);
    }

    Ordering<ResultRow> ordering = null;
    for (OrderByColumnSpec columnSpec : columns) {
      String columnName = columnSpec.getDimension();
      Ordering<ResultRow> nextOrdering = null;

      final int columnIndex = rowSignature.indexOf(columnName);

      if (columnIndex >= 0) {
        if (postAggregatorsMap.containsKey(columnName)) {
          //noinspection unchecked
          nextOrdering = metricOrdering(columnIndex, postAggregatorsMap.get(columnName).getComparator());
        } else if (aggregatorsMap.containsKey(columnName)) {
          //noinspection unchecked
          nextOrdering = metricOrdering(columnIndex, aggregatorsMap.get(columnName).getComparator());
        } else if (dimensionsMap.containsKey(columnName)) {
          Optional<DimensionSpec> dimensionSpec = dimensions.stream()
                                                            .filter(ds -> ds.getOutputName().equals(columnName))
                                                            .findFirst();
          if (!dimensionSpec.isPresent()) {
            throw new ISE("Could not find the dimension spec for ordering column %s", columnName);
          }
          nextOrdering = dimensionOrdering(
              columnIndex,
              dimensionSpec.get().getOutputType(),
              columnSpec.getDimensionComparator()
          );
        }
      }

      if (nextOrdering == null) {
        throw new ISE("Unknown column in order clause[%s]", columnSpec);
      }

      if (columnSpec.getDirection() == OrderByColumnSpec.Direction.DESCENDING) {
        nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering == null ? nextOrdering : ordering.compound(nextOrdering);
    }

    if (ordering == null) {
      ordering = timeOrdering;
    } else if (timeOrdering != null) {
      ordering = sortByDimsFirst ? ordering.compound(timeOrdering) : timeOrdering.compound(ordering);
    }

    //noinspection unchecked
    return ordering != null ? ordering : (Ordering) Ordering.allEqual();
  }

  private <T> Ordering<ResultRow> metricOrdering(final int column, final Comparator<T> comparator)
  {
    // As per SQL standard we need to have same ordering for metrics as dimensions i.e nulls first
    // If SQL compatibility is not enabled we use nullsLast ordering for null metrics for backwards compatibility.
    final Comparator<T> nullFriendlyComparator = NullHandling.sqlCompatible()
                                                 ? Comparator.nullsFirst(comparator)
                                                 : Comparator.nullsLast(comparator);

    //noinspection unchecked
    return Ordering.from(Comparator.comparing(row -> (T) row.get(column), nullFriendlyComparator));
  }

  private Ordering<ResultRow> dimensionOrdering(
      final int column,
      final ColumnType columnType,
      final StringComparator comparator
  )
  {
    Comparator arrayComparator = null;
    if (columnType.isArray()) {
      final ValueType elementType = columnType.getElementType().getType();
      if (columnType.getElementType().isNumeric()) {
        arrayComparator = (Comparator<Object>) (o1, o2) -> ComparableList.compareWithComparator(
            comparator,
            DimensionHandlerUtils.convertToList(o1, elementType),
            DimensionHandlerUtils.convertToList(o2, elementType)
        );
      } else if (columnType.getElementType().equals(ColumnType.STRING)) {
        arrayComparator = (Comparator<Object>) (o1, o2) -> ComparableStringArray.compareWithComparator(
            comparator,
            DimensionHandlerUtils.convertToComparableStringArray(o1),
            DimensionHandlerUtils.convertToComparableStringArray(o2)
        );
      } else {
        throw new ISE("Cannot create comparator for array type %s.", columnType.toString());
      }
    }
    return Ordering.from(
        Comparator.comparing(
            (ResultRow row) -> {
              if (columnType.isArray()) {
                return row.get(column);
              } else {
                return getDimensionValue(row, column);
              }
            },
            Comparator.nullsFirst(arrayComparator == null ? comparator : arrayComparator)
        )
    );
  }

  @Nullable
  private static String getDimensionValue(ResultRow row, int column)
  {
    final List<String> values = Rows.objectToStrings(row.get(column));
    return values.isEmpty() ? null : Iterables.getOnlyElement(values);
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "columns='" + columns + '\'' +
           ", offset=" + offset +
           ", limit=" + limit +
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
    DefaultLimitSpec that = (DefaultLimitSpec) o;
    return offset == that.offset &&
           limit == that.limit &&
           Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, offset, limit);
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[][] columnBytes = new byte[columns.size()][];
    int columnsBytesSize = 0;
    int index = 0;
    for (OrderByColumnSpec column : columns) {
      columnBytes[index] = column.getCacheKey();
      columnsBytesSize += columnBytes[index].length;
      ++index;
    }

    ByteBuffer buffer = ByteBuffer.allocate(1 + columnsBytesSize + 2 * Integer.BYTES)
                                  .put(CACHE_KEY);
    for (byte[] columnByte : columnBytes) {
      buffer.put(columnByte);
    }

    buffer.putInt(limit);
    buffer.putInt(offset);

    return buffer.array();
  }

  public static class Builder
  {
    private List<OrderByColumnSpec> columns = Collections.emptyList();
    private Integer offset = null;
    private Integer limit = null;

    private Builder()
    {
    }

    public Builder orderBy(final String... columns)
    {
      return orderBy(
          Arrays.stream(columns)
                .map(s -> new OrderByColumnSpec(s, OrderByColumnSpec.Direction.ASCENDING))
                .toArray(OrderByColumnSpec[]::new)
      );
    }


    public Builder orderBy(final OrderByColumnSpec... columns)
    {
      this.columns = ImmutableList.copyOf(Arrays.asList(columns));
      return this;
    }

    public Builder offset(final int offset)
    {
      this.offset = offset;
      return this;
    }

    public Builder limit(final int limit)
    {
      this.limit = limit;
      return this;
    }

    public DefaultLimitSpec build()
    {
      return new DefaultLimitSpec(columns, offset, limit);
    }
  }

  /**
   * {@link JsonInclude} filter for {@link #getLimit()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed
   * and also requires spotbugs exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode"})
  public static class LimitJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Integer && (Integer) obj == Integer.MAX_VALUE;
    }
  }
}
