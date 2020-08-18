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
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
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

public class DoubleLastAggregatorFactory extends AggregatorFactory
{
  private static final Aggregator NIL_AGGREGATOR = new DoubleLastAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance()
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new DoubleLastBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance()
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
  private final boolean storeDoubleAsFloat;

  @JsonCreator
  public DoubleLastAggregatorFactory(
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
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final BaseDoubleColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new DoubleLastAggregator(
          metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
          valueSelector
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BaseDoubleColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new DoubleLastBufferAggregator(
          metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
          valueSelector
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleFirstAggregatorFactory.VALUE_COMPARATOR;
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
    if (leftTime >= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("DoubleLastAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleLastAggregatorFactory(name, name)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final ColumnValueSelector<SerializablePair<Long, Double>> selector =
            metricFactory.makeColumnValueSelector(name);
        return new DoubleLastAggregator(null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Double> pair = selector.getObject();
            if (pair.lhs >= lastTime) {
              lastTime = pair.lhs;
              if (pair.rhs != null) {
                lastValue = pair.rhs;
                rhsNull = false;
              } else {
                rhsNull = true;
              }
            }
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ColumnValueSelector<SerializablePair<Long, Double>> selector =
            metricFactory.makeColumnValueSelector(name);
        return new DoubleLastBufferAggregator(null, null)
        {
          @Override
          public void putValue(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Double> pair = selector.getObject();
            buf.putDouble(position, pair.rhs);
          }

          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Double> pair = selector.getObject();
            long lastTime = buf.getLong(position);
            if (pair.lhs >= lastTime) {
              if (pair.rhs != null) {
                updateTimeWithValue(buf, position, pair.lhs);
              } else {
                updateTimeWithNull(buf, position, pair.lhs);
              }
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
    return Collections.singletonList(new LongFirstAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (map.get("rhs") == null) {
      return new SerializablePair<>(((Number) map.get("lhs")).longValue(), null);
    }
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
                     .put(AggregatorUtil.DOUBLE_LAST_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public ValueType getType()
  {
    // if we don't pretend to be a primitive, group by v1 gets sad and doesn't work because no complex type serde
    return storeDoubleAsFloat ? ValueType.FLOAT : ValueType.DOUBLE;
  }

  @Override
  public ValueType getFinalizedType()
  {
    // this is a copy of getComplexTypeName in the hopes that someday groupby v1 is no more and it will report it's actual
    // type
    return storeDoubleAsFloat ? ValueType.FLOAT : ValueType.DOUBLE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Double.BYTES;
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

    DoubleLastAggregatorFactory that = (DoubleLastAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName);
  }

  @Override
  public String toString()
  {
    return "DoubleLastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
