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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.ordering.StringComparators.StringComparator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

  // regrouping granularity. should be bigger than query granularity
  private final QueryGranularity regroupingGranularity;

  // limit is applied to each group, possibly resulting up to <limit> * <number of groups> rows
  private final boolean applyLimitPerGroup;

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("columns") List<OrderByColumnSpec> columns,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("regroupingGranularity") QueryGranularity regroupingGranularity,
      @JsonProperty("applyLimitPerGroup") boolean applyLimitPerGroup
  )
  {
    this.columns = (columns == null) ? ImmutableList.<OrderByColumnSpec>of() : columns;
    this.limit = (limit == null) ? Integer.MAX_VALUE : limit;
    this.regroupingGranularity = regroupingGranularity;
    this.applyLimitPerGroup = applyLimitPerGroup;

    Preconditions.checkArgument(this.limit > 0, "limit[%s] must be >0", limit);
  }

  public DefaultLimitSpec(List<OrderByColumnSpec> columns, Integer limit)
  {
    this(columns, limit, null, false);
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

  @JsonProperty
  public QueryGranularity getRegroupingGranularity()
  {
    return regroupingGranularity;
  }

  @JsonProperty
  public boolean isApplyLimitPerGroup()
  {
    return applyLimitPerGroup;
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
    Ordering<Row> ordering = makeComparator(dimensions, aggs, postAggs);

    if (regroupingGranularity == null && !applyLimitPerGroup) {
      ordering = makeTimeComparator().compound(ordering);
      return limit == Integer.MAX_VALUE ? new SortingFn(ordering) : new TopNFunction(ordering, limit);
    }
    return new GroupTopNFunction(ordering, regroupingGranularity, applyLimitPerGroup, limit);
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return this;
  }

  private Ordering<Row> makeTimeComparator()
  {
    return new Ordering<Row>()
    {
      @Override
      public int compare(Row left, Row right)
      {
        return Longs.compare(left.getTimestampFromEpoch(), right.getTimestampFromEpoch());
      }
    };
  }

  private Ordering<Row> makeComparator(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    Map<String, DimensionSpec> dimensionsMap = Maps.newHashMapWithExpectedSize(dimensions.size());
    for (DimensionSpec spec : dimensions) {
      dimensionsMap.put(spec.getOutputName(), spec);
    }

    Map<String, AggregatorFactory> aggregatorsMap = Maps.newHashMapWithExpectedSize(aggs.size());
    for (AggregatorFactory agg : aggs) {
      aggregatorsMap.put(agg.getName(), agg);
    }

    Map<String, PostAggregator> postAggregatorsMap = Maps.newHashMapWithExpectedSize(postAggs.size());
    for (PostAggregator postAgg : postAggs) {
      postAggregatorsMap.put(postAgg.getName(), postAgg);
    }

    Ordering<Row> ordering = null;
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

      switch (columnSpec.getDirection()) {
        case DESCENDING:
          nextOrdering = nextOrdering.reverse();
      }

      ordering = ordering == null ? nextOrdering : ordering.compound(nextOrdering);
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
           ", limit=" + limit + (applyLimitPerGroup ? " per group" : "") +
           (regroupingGranularity != null ? ", granularity=" + regroupingGranularity : "") +
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

  // regroup with <granularity> and select top <limit> results
  public class GroupTopNFunction implements Function<Sequence<Row>, Sequence<Row>>
  {
    private final Ordering<Row> ordering;
    private final int limit;
    private final QueryGranularity granularity;
    private final boolean limitPerGroup;

    public GroupTopNFunction(Ordering<Row> ordering, QueryGranularity granularity, boolean limitPerGroup, int limit)
    {
      this.ordering = ordering;
      this.granularity = granularity == null ? QueryGranularities.NONE : granularity;
      this.limitPerGroup = limitPerGroup;
      this.limit = limit;
    }

    @Override
    public Sequence<Row> apply(
        Sequence<Row> input
    )
    {
      Map<Long, MinMaxPriorityQueue<Row>> grouped = Maps.<Long, MinMaxPriorityQueue<Row>>newTreeMap();
      ArrayList<Row> rows = Sequences.toList(input, Lists.<Row>newArrayList());
      for (Row row : rows) {
        long time = granularity.truncate(row.getTimestampFromEpoch());
        MinMaxPriorityQueue<Row> queue = grouped.get(time);
        if (queue == null) {
          grouped.put(time, queue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(limit).create());
        }
        queue.add(row);
      }

      Iterable<Row> iterable = Collections.emptyList();
      for (Collection<Row> value : grouped.values()) {
        Iterable<Row> ordered = new OrderedPriorityQueueItems<Row>((MinMaxPriorityQueue<Row>) value);
        if (limitPerGroup) {
          ordered = Iterables.limit(ordered, limit);
        }
        iterable = Iterables.concat(iterable, ordered);
      }
      return Sequences.simple(limitPerGroup || rows.size() < limit ? iterable : Iterables.limit(iterable, limit));
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

      GroupTopNFunction that = (GroupTopNFunction) o;

      if (limit != that.limit || limitPerGroup != that.limitPerGroup) {
        return false;
      }
      // todo: it's not comparable
      if (granularity != that.granularity) {
        return false;
      }
      if (!ordering.equals(that.ordering)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = ordering.hashCode();
      result = 31 * result + granularity.hashCode();
      result = 31 * result + limit;
      if (limitPerGroup) {
        result = (result << 1) + 1;
      }
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

    if (limit != that.limit || applyLimitPerGroup != that.applyLimitPerGroup) {
      return false;
    }
    if (columns != null ? !columns.equals(that.columns) : that.columns != null) {
      return false;
    }

    if (regroupingGranularity != null ? regroupingGranularity != that.regroupingGranularity
                                      : that.regroupingGranularity != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columns != null ? columns.hashCode() : 0;
    result = 31 * result + (regroupingGranularity != null ? regroupingGranularity.hashCode() : 0);
    result = 31 * result + limit;
    if (applyLimitPerGroup) {
      result = (result << 1) + 1;
    }
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

    byte[] granKey = regroupingGranularity == null ? new byte[0] : regroupingGranularity.cacheKey();

    ByteBuffer buffer = ByteBuffer.allocate(1 + columnsBytesSize + granKey.length + 1 + 4)
                                  .put(CACHE_KEY);
    for (byte[] columnByte : columnBytes) {
      buffer.put(columnByte);
    }
    buffer.put(Ints.toByteArray(limit));
    buffer.put(granKey);
    buffer.put(isApplyLimitPerGroup() ? (byte)1 : 0);
    return buffer.array();
  }
}
