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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
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
    return Integer.BYTES;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.GROUPING_CACHE_TYPE_ID)
        .appendStrings(groupings)
        .build();
  }

  private long groupingId(List<String> groupings, @Nullable Set<String> keyDimensions)
  {
    Preconditions.checkArgument(!CollectionUtils.isNullOrEmpty(groupings), "Must have a non-empty grouping dimensions");
    // Integer.size is just a sanity check. In practice, it will be just few dimensions.
    Preconditions.checkArgument(
        groupings.size() < Integer.SIZE,
        "Number of dimensions %s is more than supported %s",
        groupings.size(),
        Integer.SIZE - 1
    );
    long temp = 0L;
    for (String groupingDimension : groupings) {
      if (isDimensionIncluded(groupingDimension, keyDimensions)) {
        temp = temp | 1L;
      }
      temp = temp << 1;
    }
    return temp >> 1;
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
