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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.constant.LongConstantAggregator;
import org.apache.druid.query.aggregation.constant.LongConstantBufferAggregator;
import org.apache.druid.query.aggregation.constant.LongConstantVectorAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class implements {@code grouping} function to determine the grouping that a row is part of. Different result rows
 * for a query could have different grouping columns when subtotals are used.
 *
 * This aggregator factory takes following arguments
 *  - {@code name} - Name of aggregators
 *  - {@code groupings} - List of dimensions that the user is interested in tracking
 *  - {@code keyDimensions} - The list of grouping dimensions being included in the result row. This list is a subset of
 *                             {@code groupings}. This argument cannot be passed by the user. It is set by druid engine
 *                             when a particular subtotal spec is being processed. Whenever druid engine processes a new
 *                             subtotal spec, engine sets that subtotal spec as new {@code keyDimensions}.
 *
 *  When key dimensions are updated, {@code value} is updated as well. How the value is determined is captured
 *  at {@link #groupingId(List, Set)}.
 *
 *  since grouping has to be calculated only once, it could have been implemented as a virtual function or
 *  post-aggregator etc. We modelled it as an aggregation operator so that its output can be used in a post-aggregator.
 *  Calcite too models grouping function as an aggregation operator.
 *  Since it is a non-trivial special aggregation, implementing it required changes in core druid engine to work. There
 *  were few approaches. We chose the approach that required least changes in core druid.
 *  Refer to https://github.com/apache/druid/pull/10518#discussion_r532941216 for more details.
 *
 *  Currently, it works in following way
 *    - On data servers (no change),
 *      - this factory generates {@link LongConstantAggregator} / {@link LongConstantBufferAggregator} / {@link LongConstantVectorAggregator}
 *         with keyDimensions as null
 *      - The aggregators don't actually aggregate anything and their result is not actually used. We could have removed
 *      these aggregators on data servers but that would result in a signature mismatch on broker and data nodes. That requires
 *      extra handling and is error-prone.
 *    - On brokers
 *      - Results from data node is already being re-processed for each subtotal spec. We made modifications in this path to update the
 *      grouping id for each row.
 *
 */
@EverythingIsNonnullByDefault
public class GroupingAggregatorFactory extends AggregatorFactory
{
  private static final Comparator<Long> VALUE_COMPARATOR = Long::compare;
  private final String name;
  private final List<String> groupings;
  private final long value;
  @Nullable
  private final Set<String> keyDimensions;

  @JsonCreator
  public GroupingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("groupings") List<String> groupings
  )
  {
    this(name, groupings, null);
  }

  @VisibleForTesting
  GroupingAggregatorFactory(
      String name,
      List<String> groupings,
      @Nullable Set<String> keyDimensions
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.name = name;
    this.groupings = groupings;
    this.keyDimensions = keyDimensions;
    value = groupingId(groupings, keyDimensions);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new LongConstantAggregator(value);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new LongConstantBufferAggregator(value);
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    return new LongConstantVectorAggregator(value);
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  /**
   * Replace the param {@code keyDimensions} with the new set of key dimensions
   */
  public GroupingAggregatorFactory withKeyDimensions(Set<String> newKeyDimensions)
  {
    return new GroupingAggregatorFactory(name, groupings, newKeyDimensions);
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @JsonProperty
  public List<String> getGroupings()
  {
    return groupings;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  public long getValue()
  {
    return value;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (null == lhs) {
      return rhs;
    }
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GroupingAggregatorFactory(name, groupings, keyDimensions);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new GroupingAggregatorFactory(name, groupings, keyDimensions));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    // The aggregator doesn't need to read any fields.
    return Collections.emptyList();
  }

  @Override
  public ValueType getType()
  {
    return ValueType.LONG;
  }

  @Override
  public ValueType getFinalizedType()
  {
    return ValueType.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder keyBuilder = new CacheKeyBuilder(AggregatorUtil.GROUPING_CACHE_TYPE_ID)
        .appendStrings(groupings);
    if (null != keyDimensions) {
      keyBuilder.appendStrings(keyDimensions);
    }
    return keyBuilder.build();
  }

  /**
   * Given the list of grouping dimensions, returns a long value where each bit at position X in the returned value
   * corresponds to the dimension in groupings at same position X. X is the position relative to the right end. if
   * keyDimensions contain the grouping dimension at position X, the bit is set to 0 at position X, otherwise it is
   * set to 1.
   *
   *  groupings           keyDimensions           value (3 least significant bits)         value (long)
   *    a,b,c                    [a]                       011                                       3
   *    a,b,c                    [b]                       101                                       5
   *    a,b,c                    [c]                       110                                       6
   *    a,b,c                  [a,b]                       001                                       1
   *    a,b,c                  [a,c]                       010                                       2
   *    a,b,c                  [b,c]                       100                                       4
   *    a,b,c                [a,b,c]                       000                                       0
   *    a,b,c                     []                       111                                       7    // None included
   *    a,b,c                 <null>                       000                                       0    // All included
   */
  private long groupingId(List<String> groupings, @Nullable Set<String> keyDimensions)
  {
    Preconditions.checkArgument(!CollectionUtils.isNullOrEmpty(groupings), "Must have a non-empty grouping dimensions");
    // (Long.SIZE - 1) is just a sanity check. In practice, it will be just few dimensions. This limit
    // also makes sure that values are always positive.
    Preconditions.checkArgument(
        groupings.size() < Long.SIZE,
        "Number of dimensions %s is more than supported %s",
        groupings.size(),
        Long.SIZE - 1
    );
    long temp = 0L;
    for (String groupingDimension : groupings) {
      temp = temp << 1;
      if (!isDimensionIncluded(groupingDimension, keyDimensions)) {
        temp = temp | 1L;
      }
    }
    return temp;
  }

  private boolean isDimensionIncluded(String dimToCheck, @Nullable Set<String> keyDimensions)
  {
    if (null == keyDimensions) {
      // All dimensions are included
      return true;
    } else {
      return keyDimensions.contains(dimToCheck);
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
    GroupingAggregatorFactory factory = (GroupingAggregatorFactory) o;
    return name.equals(factory.name) &&
           groupings.equals(factory.groupings) &&
           Objects.equals(keyDimensions, factory.keyDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, groupings, keyDimensions);
  }

  @Override
  public String toString()
  {
    return "GroupingAggregatorFactory{" +
           "name='" + name + '\'' +
           ", groupings=" + groupings +
           ", keyDimensions=" + keyDimensions +
           '}';
  }
}
