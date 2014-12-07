/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x1;

  private final List<OrderByColumnSpec> columns;
  private final int limit;

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
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    if (columns.isEmpty()) {
      return new LimitingFn(limit);
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

    Map<String, Object> possibleOrderDims = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    for (DimensionSpec spec : dimensions) {
      possibleOrderDims.put(spec.getOutputName(), spec);
    }

    for (final AggregatorFactory agg : aggs) {
      possibleOrderDims.put(agg.getName(), agg);
    }

    for (PostAggregator postAgg : postAggs) {
      possibleOrderDims.put(postAgg.getName(), postAgg);
    }

    for (OrderByColumnSpec columnSpec : columns) {
      String dimension = columnSpec.getDimension();
      Object dimObj = possibleOrderDims.get(dimension);
      if (dimObj == null) {
        throw new ISE("Unknown column in order clause[%s]", columnSpec);
      }
      Ordering<Row> nextOrdering;
      if (dimObj instanceof DimensionSpec) {
        nextOrdering = dimensionOrdering(dimension, columnSpec.asNumber());
      } else if (dimObj instanceof AggregatorFactory) {
        nextOrdering = metricOrdering(dimension, ((AggregatorFactory) dimObj).getComparator());
      } else {
        nextOrdering = metricOrdering(dimension, ((PostAggregator) dimObj).getComparator());
      }

      switch (columnSpec.getDirection()) {
        case DESCENDING:
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

  private Ordering<Row> dimensionOrdering(final String dimension, final boolean asNumber)
  {
    if (asNumber) {
      // Order: null < string < number , and number only means "long" here.
      return new Ordering<Comparable>()
      {
        @Override
        public int compare(
            Comparable left, Comparable right
        )
        {
          if (left instanceof Long && right instanceof Long) {
            return ((Long) left).compareTo((Long) right);
          } else if (left instanceof String) {
            return -1;
          } else if (right instanceof String) {
            return 1;
          } else {
            return ((String) left).compareTo((String) right);
          }
        }
      }.nullsFirst().onResultOf(
          new Function<Row, Comparable>()
          {
            @Override
            public Comparable apply(Row input)
            {
              // Multi-value dimensions have all been flattened at this point;
              final List<String> dimList = input.getDimension(dimension.toLowerCase());
              if (dimList.isEmpty()) {
                return null;
              }

              String dimValue = dimList.get(0);
              // Make sure dimValue is number parsable.
              if (dimValue.isEmpty()) {
                return dimValue;
              }
              for (int i = 0; i < dimValue.length(); i++) {
                if (!Character.isDigit(dimValue.charAt(i))) {
                  return dimValue;
                }
              }
              try {
                return Long.parseLong(dimValue);
              }
              catch (NumberFormatException e) {
                return dimList.get(0);
              }
            }
          }
      );
    } else {
      return Ordering.natural().nullsFirst().onResultOf(
          new Function<Row, String>()
          {
            @Override
            public String apply(Row input)
            {
              // Multi-value dimensions have all been flattened at this point;
              final List<String> dimList = input.getDimension(dimension.toLowerCase());
              return dimList.isEmpty() ? null : dimList.get(0);
            }
          }
      );
    }
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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LimitingFn that = (LimitingFn) o;

      if (limit != that.limit) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return limit;
    }
  }

  private static class SortingFn implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final Ordering<Row> ordering;

    public SortingFn(Ordering<Row> ordering) {this.ordering = ordering;}

    @Override
    public Sequence<Row> apply(@Nullable Sequence<Row> input)
    {
      return Sequences.sort(input, ordering);
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

      SortingFn sortingFn = (SortingFn) o;

      if (ordering != null ? !ordering.equals(sortingFn.ordering) : sortingFn.ordering != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return ordering != null ? ordering.hashCode() : 0;
    }
  }

  private static class TopNFunction implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final TopNSorter<Row> sorter;
    private final int limit;

    public TopNFunction(Ordering<Row> ordering, int limit)
    {
      this.limit = limit;

      this.sorter = new TopNSorter<>(ordering);
    }

    @Override
    public Sequence<Row> apply(
        Sequence<Row> input
    )
    {
      final ArrayList<Row> materializedList = Sequences.toList(input, Lists.<Row>newArrayList());
      return Sequences.simple(sorter.toTopN(materializedList, limit));
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

      TopNFunction that = (TopNFunction) o;

      if (limit != that.limit) {
        return false;
      }
      if (sorter != null ? !sorter.equals(that.sorter) : that.sorter != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = sorter != null ? sorter.hashCode() : 0;
      result = 31 * result + limit;
      return result;
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
