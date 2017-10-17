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
import com.google.common.primitives.Longs;
import com.metamx.common.StringUtils;
import io.druid.collections.SerializablePair;
import io.druid.java.util.common.UOE;
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import io.druid.query.aggregation.first.LongFirstAggregatorFactory;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DoubleLastAggregatorFactory extends AggregatorFactory
{

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
    this.storeDoubleAsFloat = Column.storeDoubleAsFloat();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleLastAggregator(
        name,
        metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeDoubleColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleLastBufferAggregator(
        metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeDoubleColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleFirstAggregatorFactory.TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
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
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new DoubleLastAggregator(name, null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.getObject();
            if (pair.lhs >= lastTime) {
              lastTime = pair.lhs;
              lastValue = pair.rhs;
            }
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new DoubleLastBufferAggregator(null, null)
        {
          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.getObject();
            long lastTime = buf.getLong(position);
            if (pair.lhs >= lastTime) {
              buf.putLong(position, pair.lhs);
              buf.putDouble(position + Longs.BYTES, pair.rhs);
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
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new LongFirstAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).doubleValue());
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((SerializablePair<Long, Double>) object).rhs;
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
    return Arrays.asList(Column.TIME_COLUMN_NAME, fieldName);
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
