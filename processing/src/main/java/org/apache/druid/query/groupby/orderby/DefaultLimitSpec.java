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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x1;

  private final List<OrderByColumnSpec> columns;
  private final int limit;

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
      @JsonProperty("limit") Integer limit
  )
  {
    this.columns = (columns == null) ? ImmutableList.of() : columns;
    this.limit = (limit == null) ? Integer.MAX_VALUE : limit;

    Preconditions.checkArgument(this.limit > 0, "limit[%s] must be >0", limit);
  }

  @JsonProperty
  public List<OrderByColumnSpec> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
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

        final ValueType columnType = getOrderByType(columnSpec, dimensions);
        final StringComparator naturalComparator;
        if (columnType == ValueType.STRING) {
          naturalComparator = StringComparators.LEXICOGRAPHIC;
        } else if (ValueType.isNumeric(columnType)) {
          naturalComparator = StringComparators.NUMERIC;
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
      return isLimited() ? new LimitingFn(limit) : Functions.identity();
    }

    // Materialize the Comparator first for fast-fail error checking.
    final Ordering<ResultRow> ordering = makeComparator(
        query.getResultRowPositionLookup(),
        query.getResultRowHasTimestamp(),
        query.getDimensions(),
        query.getAggregatorSpecs(),
        query.getPostAggregatorSpecs(),
        query.getContextSortByDimsFirst()
    );

    if (isLimited()) {
      return new TopNFunction(ordering, limit);
    } else {
      return new SortingFn(ordering);
    }
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return this;
  }

  private ValueType getOrderByType(final OrderByColumnSpec columnSpec, final List<DimensionSpec> dimensions)
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
        limit
    );
  }

  private Ordering<ResultRow> makeComparator(
      Object2IntMap<String> rowOrderLookup,
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

      final int columnIndex = rowOrderLookup.applyAsInt(columnName);

      if (columnIndex >= 0) {
        if (postAggregatorsMap.containsKey(columnName)) {
          //noinspection unchecked
          nextOrdering = metricOrdering(columnIndex, postAggregatorsMap.get(columnName).getComparator());
        } else if (aggregatorsMap.containsKey(columnName)) {
          //noinspection unchecked
          nextOrdering = metricOrdering(columnIndex, aggregatorsMap.get(columnName).getComparator());
        } else if (dimensionsMap.containsKey(columnName)) {
          nextOrdering = dimensionOrdering(columnIndex, columnSpec.getDimensionComparator());
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

  private Ordering<ResultRow> dimensionOrdering(final int column, final StringComparator comparator)
  {
    return Ordering.from(
        Comparator.comparing((ResultRow row) -> getDimensionValue(row, column), Comparator.nullsFirst(comparator))
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
           ", limit=" + limit +
           '}';
  }

  private static class LimitingFn implements Function<Sequence<ResultRow>, Sequence<ResultRow>>
  {
    private final int limit;

    public LimitingFn(int limit)
    {
      this.limit = limit;
    }

    @Override
    public Sequence<ResultRow> apply(Sequence<ResultRow> input)
    {
      return input.limit(limit);
    }
  }

  private static class SortingFn implements Function<Sequence<ResultRow>, Sequence<ResultRow>>
  {
    private final Ordering<ResultRow> ordering;

    public SortingFn(Ordering<ResultRow> ordering)
    {
      this.ordering = ordering;
    }

    @Override
    public Sequence<ResultRow> apply(@Nullable Sequence<ResultRow> input)
    {
      return Sequences.sort(input, ordering);
    }
  }

  private static class TopNFunction implements Function<Sequence<ResultRow>, Sequence<ResultRow>>
  {
    private final Ordering<ResultRow> ordering;
    private final int limit;

    public TopNFunction(Ordering<ResultRow> ordering, int limit)
    {
      this.ordering = ordering;
      this.limit = limit;
    }

    @Override
    public Sequence<ResultRow> apply(final Sequence<ResultRow> input)
    {
      return new TopNSequence<>(input, ordering, limit);
    }
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

    if (limit != that.limit) {
      return false;
    }
    if (columns != null ? !columns.equals(that.columns) : that.columns != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columns != null ? columns.hashCode() : 0;
    result = 31 * result + limit;
    return result;
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

    ByteBuffer buffer = ByteBuffer.allocate(1 + columnsBytesSize + 4)
                                  .put(CACHE_KEY);
    for (byte[] columnByte : columnBytes) {
      buffer.put(columnByte);
    }
    buffer.put(Ints.toByteArray(limit));
    return buffer.array();
  }
}
