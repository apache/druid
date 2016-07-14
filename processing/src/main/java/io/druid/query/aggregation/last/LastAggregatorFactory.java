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

package io.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class LastAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x11;
  private static final String TYPE_LONG = "long";
  private static final String TYPE_DOUBLE = "double";

  private final String fieldName;
  private final String name;
  private final String value;

  @JsonCreator
  public LastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("value") String value
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    Preconditions.checkArgument(
        value.equals(TYPE_LONG) || value.equals(TYPE_DOUBLE),
        "Must have a valid, non-null type"
    );

    this.name = name;
    this.fieldName = fieldName;
    this.value = value;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (value.equals(TYPE_LONG)) {
      return new LongLastAggregator(
          name, metricFactory.makeLongColumnSelector(fieldName),
          metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)
      );
    } else if (value.equals(TYPE_DOUBLE)) {
      return new DoubleLastAggregator(
          name, metricFactory.makeFloatColumnSelector(fieldName),
          metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME)
      );
    }
    throw new IAE("undefined type");
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (value.equals(TYPE_LONG)) {
      return new LongLastBufferAggregator(
          metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
          metricFactory.makeLongColumnSelector(fieldName)
      );
    } else if (value.equals(TYPE_DOUBLE)) {
      return new DoubleLastBufferAggregator(
          metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    }
    throw new IAE("undefined type");
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return Longs.compare(((SerializablePair<Long, Object>) o1).lhs, ((SerializablePair<Long, Object>) o2).lhs);
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return (((SerializablePair<Long, Object>) lhs).lhs > ((SerializablePair<Long, Object>) rhs).lhs) ? lhs : rhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LastAggregatorFactory(name, name, value)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        if (value.equals(TYPE_LONG)) {
          return new LongLastAggregator(name, null, null)
          {
            @Override
            public void aggregate()
            {
              SerializablePair<Long, Long> pair = (SerializablePair<Long, Long>) selector.get();
              if (pair.rhs >= lastTime) {
                lastTime = pair.lhs;
                lastValue = pair.rhs;
              }
            }
          };
        } else if (value.equals(TYPE_DOUBLE)) {
          return new DoubleLastAggregator(name, null, null)
          {
            @Override
            public void aggregate()
            {
              SerializablePair<Long, Long> pair = (SerializablePair<Long, Long>) selector.get();
              if (pair.rhs >= lastTime) {
                lastTime = pair.lhs;
                lastValue = pair.rhs;
              }
            }
          };
        }
        throw new IAE("undefined type");
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        if (value.equals(TYPE_LONG)) {
          return new LongLastBufferAggregator(null, null)
          {
            @Override
            public void aggregate(ByteBuffer buf, int position)
            {
              SerializablePair<Long, Long> pair = (SerializablePair<Long, Long>) selector.get();
              long lastTime = buf.getLong(position);
              if (pair.lhs >= lastTime) {
                buf.putLong(position, pair.lhs);
                buf.putLong(position + Longs.BYTES, pair.rhs);
              }
            }
          };
        } else if (value.equals(TYPE_DOUBLE)) {
          return new DoubleLastBufferAggregator(null, null)
          {
            @Override
            public void aggregate(ByteBuffer buf, int position)
            {
              SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.get();
              long lastTime = buf.getLong(position);
              if (pair.lhs >= lastTime) {
                buf.putLong(position, pair.lhs);
                buf.putDouble(position + Longs.BYTES, pair.rhs);
              }
            }
          };
        }
        throw new IAE("undefined type");
      }
    };
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass() &&
        other.getTypeName().equals(this.getTypeName())) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new LastAggregatorFactory(fieldName, fieldName, value));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (value.equals(TYPE_LONG)) {
      return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).longValue());
    } else if (value.equals(TYPE_DOUBLE)) {
      return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).doubleValue());
    }
    throw new IAE("undefined type");
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((SerializablePair<Long, Object>) object).rhs;
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

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(Column.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return value.equals(TYPE_DOUBLE) ? "float" : value;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    if (value.equals(TYPE_LONG)) {
      return Longs.BYTES * 2;
    } else if (value.equals(TYPE_DOUBLE)) {
      return Longs.BYTES + Doubles.BYTES;
    }
    throw new IAE("undefined type");
  }

  @Override
  public Object getAggregatorStartValue()
  {
    if (value.equals(TYPE_LONG)) {
      return new SerializablePair<>(Long.MIN_VALUE, 0L);
    } else if (value.equals(TYPE_DOUBLE)) {
      return new SerializablePair<>(Long.MIN_VALUE, 0D);
    }
    throw new IAE("undefined type");
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

    LastAggregatorFactory that = (LastAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return value.equals(that.value);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "LastAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", value='" + value + '\'' +
           '}';
  }
}
