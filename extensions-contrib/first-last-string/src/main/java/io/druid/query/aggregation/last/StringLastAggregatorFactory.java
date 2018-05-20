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
import io.druid.collections.SerializablePair;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseObjectColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StringLastAggregatorFactory extends AggregatorFactory
{
  public static final Integer MAX_SIZE_STRING = 1024;
  public static final Comparator VALUE_COMPARATOR = (o1, o2) -> ((SerializablePair<Long, String>) o1).rhs.equals(((SerializablePair<Long, String>) o2).rhs)
                                                                ? 1
                                                                : 0;

  public static final Comparator TIME_COMPARATOR = (o1, o2) -> Longs.compare(
      ((SerializablePair<Long, Object>) o1).lhs,
      ((SerializablePair<Long, Object>) o2).lhs
  );

  private final String fieldName;
  private final String name;
  private final Integer maxStringBytes;

  @JsonCreator
  public StringLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxStringBytes") Integer maxStringBytes
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    this.name = name;
    this.fieldName = fieldName;
    this.maxStringBytes = maxStringBytes == null ? MAX_SIZE_STRING : maxStringBytes;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new StringLastAggregator(
        metricFactory.makeColumnValueSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeColumnValueSelector(fieldName),
        maxStringBytes
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new StringLastBufferAggregator(
        metricFactory.makeColumnValueSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeColumnValueSelector(fieldName),
        maxStringBytes
    );
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleFirstAggregatorFactory.TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new StringLastAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringLastAggregatorFactory(name, name, maxStringBytes)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(name);
        return new StringLastAggregator(null, null, maxStringBytes)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, String> pair = (SerializablePair<Long, String>) selector.getObject();
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
        final BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(name);
        return new StringLastBufferAggregator(null, null, maxStringBytes)
        {
          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            ByteBuffer mutationBuffer = buf.duplicate();
            mutationBuffer.position(position);

            SerializablePair<Long, String> pair = (SerializablePair<Long, String>) selector.getObject();
            long lastTime = mutationBuffer.getLong(position);
            if (pair.lhs >= lastTime) {
              mutationBuffer.putLong(position, pair.lhs);
              byte[] valueBytes = pair.rhs.getBytes(StandardCharsets.UTF_8);

              mutationBuffer.putInt(position + Long.BYTES, valueBytes.length);
              mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
              mutationBuffer.put(valueBytes);
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
    return Arrays.<AggregatorFactory>asList(new StringLastAggregatorFactory(fieldName, fieldName, maxStringBytes));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((String) map.get("rhs")));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((SerializablePair<Long, String>) object).rhs;
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
                     .put(AggregatorUtil.STRING_LAST_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "serializablePairLongString";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + Integer.BYTES + maxStringBytes;
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

    StringLastAggregatorFactory that = (StringLastAggregatorFactory) o;

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
    return "StringFirstAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", maxStringBytes=" + maxStringBytes + '\'' +
           '}';
  }
}
