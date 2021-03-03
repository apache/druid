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

package org.apache.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.query.aggregation.SerializablePairLongLongSerde;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstLastUtils;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("longLast")
public class LongLastAggregatorFactory extends AggregatorFactory
{
  private static final Aggregator NIL_AGGREGATOR = new LongLastAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      false
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new LongLastBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      false
  )
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };

  private final String fieldName;
  private final String name;

  @JsonCreator
  public LongLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    }

    return new LongLastAggregator(
        metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        valueSelector,
        StringFirstLastUtils.selectorNeedsFoldCheck(
            valueSelector,
            metricFactory.getColumnCapabilities(fieldName),
            SerializablePairLongLong.class
        )
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    }

    return new LongLastBufferAggregator(
        metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        selector,
        StringFirstLastUtils.selectorNeedsFoldCheck(
            selector,
            metricFactory.getColumnCapabilities(fieldName),
            SerializablePairLongLong.class
        )
    );
  }

  @Override
  public Comparator getComparator()
  {
    return LongFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    Long leftTime = ((SerializablePair<Long, Long>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Long>) rhs).lhs;
    if (leftTime >= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new GenericLastAggregateCombiner(SerializablePairLongLong.class);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongLastAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new LongLastAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (map.get("rhs") == null) {
      return new SerializablePair<>(((Number) map.get("lhs")).longValue(), null);
    }
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).longValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePair<Long, Long>) object).rhs;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length)
                     .put(AggregatorUtil.LONG_LAST_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getComplexTypeName()
  {
    return SerializablePairLongLongSerde.TYPE_NAME;
  }

  /**
   * actual type is {@link SerializablePair<Long, Float>}
   */
  @Override
  public ValueType getType()
  {
    return ValueType.COMPLEX;
  }

  @Override
  public ValueType getFinalizedType()
  {
    return ValueType.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Long.BYTES;
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

    LongLastAggregatorFactory that = (LongLastAggregatorFactory) o;

    return name.equals(that.name) && fieldName.equals(that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return "LongLastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
