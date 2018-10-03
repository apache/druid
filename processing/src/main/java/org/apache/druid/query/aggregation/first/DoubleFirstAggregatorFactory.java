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

package org.apache.druid.query.aggregation.first;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NullableAggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DoubleFirstAggregatorFactory extends NullableAggregatorFactory<ColumnValueSelector>
{
  public static final Comparator<SerializablePair<Long, Double>> VALUE_COMPARATOR =
      Comparator.comparingDouble(o -> o.rhs);

  private final String fieldName;
  private final String name;
  private final boolean storeDoubleAsFloat;

  @JsonCreator
  public DoubleFirstAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.storeDoubleAsFloat = ColumnHolder.storeDoubleAsFloat();
  }

  @Override
  protected ColumnValueSelector selector(ColumnSelectorFactory metricFactory)
  {
    return metricFactory.makeColumnValueSelector(fieldName);
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    return new DoubleFirstAggregator(metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME), selector);
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    return new DoubleFirstBufferAggregator(
        metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        selector
    );
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
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
    Long leftTime = ((SerializablePair<Long, Double>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Double>) rhs).lhs;
    if (leftTime <= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("DoubleFirstAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleFirstAggregatorFactory(name, name)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
      {
        return new DoubleFirstAggregator(null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.getObject();
            if (pair.lhs < firstTime) {
              firstTime = pair.lhs;
              firstValue = pair.rhs;
            }
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
      {
        return new DoubleFirstBufferAggregator(null, null)
        {
          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.getObject();
            long firstTime = buf.getLong(position);
            if (pair.lhs < firstTime) {
              buf.putLong(position, pair.lhs);
              buf.putDouble(position + Long.BYTES, pair.rhs);
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("selector", selector);
          }
        };
      }
    };
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new DoubleFirstAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).doubleValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePair<Long, Double>) object).rhs;
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
                     .put(AggregatorUtil.DOUBLE_FIRST_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    if (storeDoubleAsFloat) {
      return "float";
    }
    return "double";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + Double.BYTES;
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

    DoubleFirstAggregatorFactory that = (DoubleFirstAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name);
  }

  @Override
  public String toString()
  {
    return "DoubleFirstAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
