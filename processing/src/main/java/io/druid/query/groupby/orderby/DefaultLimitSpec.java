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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
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
   * @param limitSpec LimitSpec, assumed to be non-null
   * @param dimensions Grouping fields for a groupBy query
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
    this.columns = (columns == null) ? ImmutableList.<OrderByColumnSpec>of() : columns;
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

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs
  )
  {
    // Can avoid re-sorting if the natural ordering is good enough.

    boolean sortingNeeded = false;

    if (dimensions.size() < columns.size()) {
      sortingNeeded = true;
    }

    final Set<String> aggAndPostAggNames = Sets.newHashSet();
    for (AggregatorFactory agg : aggs) {
      aggAndPostAggNames.add(agg.getName());
    }
    for (PostAggregator postAgg : postAggs) {
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
      return limit == Integer.MAX_VALUE ? Functions.<Sequence<Row>>identity() : new LimitingFn(limit);
    }

    // Materialize the Comparator first for fast-fail error checking.
    final Ordering<Row> ordering = makeComparator(dimensions, aggs, postAggs);

    if (limit == Integer.MAX_VALUE) {
      return new SortingFn(ordering);
    } else {
      return new TopNFunction(ordering, limit);
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

  private Ordering<Row> makeComparator(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    Ordering<Row> ordering = new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    };

    Map<String, DimensionSpec> dimensionsMap = Maps.newHashMap();
    for (DimensionSpec spec : dimensions) {
      dimensionsMap.put(spec.getOutputName(), spec);
    }

    Map<String, AggregatorFactory> aggregatorsMap = Maps.newHashMap();
    for (final AggregatorFactory agg : aggs) {
      aggregatorsMap.put(agg.getName(), agg);
    }

    Map<String, PostAggregator> postAggregatorsMap = Maps.newHashMap();
    for (PostAggregator postAgg : postAggs) {
      postAggregatorsMap.put(postAgg.getName(), postAgg);
    }

    for (OrderByColumnSpec columnSpec : columns) {
      String columnName = columnSpec.getDimension();
      Ordering<Row> nextOrdering = null;

      if (postAggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, postAggregatorsMap.get(columnName).getComparator());
      } else if (aggregatorsMap.containsKey(columnName)) {
        nextOrdering = metricOrdering(columnName, aggregatorsMap.get(columnName).getComparator());
      } else if (dimensionsMap.containsKey(columnName)) {
        nextOrdering = dimensionOrdering(columnName, columnSpec.getDimensionComparator());
      }

      if (nextOrdering == null) {
        throw new ISE("Unknown column in order clause[%s]", columnSpec);
      }

      if (columnSpec.getDirection() == OrderByColumnSpec.Direction.DESCENDING) {
        nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering.compound(nextOrdering);
    }

    return ordering;
  }

  private Ordering<Row> metricOrdering(final String column, final Comparator comparator)
  {
    return new Ordering<Row>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public int compare(Row left, Row right)
      {
        return comparator.compare(left.getRaw(column), right.getRaw(column));
      }
    };
  }

  private Ordering<Row> dimensionOrdering(final String dimension, final StringComparator comparator)
  {
    return Ordering.from(comparator)
                   .nullsFirst()
                   .onResultOf(
                       new Function<Row, String>()
                       {
                         @Override
                         public String apply(Row input)
                         {
                           // Multi-value dimensions have all been flattened at this point;
                           final List<String> dimList = input.getDimension(dimension);
                           return dimList.isEmpty() ? null : dimList.get(0);
                         }
                       }
                   );
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "columns='" + columns + '\'' +
           ", limit=" + limit +
           '}';
  }

  private static class LimitingFn implements Function<Sequence<Row>, Sequence<Row>>
  {
    private int limit;

    public LimitingFn(int limit)
    {
      this.limit = limit;
    }

    @Override
    public Sequence<Row> apply(
        Sequence<Row> input
    )
    {
      return Sequences.limit(input, limit);
    }
  }

  private static class SortingFn implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final Ordering<Row> ordering;

    public SortingFn(Ordering<Row> ordering)
    {
      this.ordering = ordering;
    }

    @Override
    public Sequence<Row> apply(@Nullable Sequence<Row> input)
    {
      return Sequences.sort(input, ordering);
    }
  }

  private static class TopNFunction implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final Ordering<Row> ordering;
    private final int limit;

    public TopNFunction(Ordering<Row> ordering, int limit)
    {
      this.ordering = ordering;
      this.limit = limit;
    }

    @Override
    public Sequence<Row> apply(final Sequence<Row> input)
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
