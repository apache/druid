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

package io.druid.query.aggregation.first;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.collections.SerializablePair;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("stringFirst")
public class StringFirstAggregatorFactory extends AggregatorFactory
{
  public static final Integer MAX_SIZE_STRING = 1024;
  public static final Comparator VALUE_COMPARATOR = (o1, o2) -> ((SerializablePair<Long, String>) o1).rhs.equals(((SerializablePair<Long, String>) o2).rhs)
                                                                ? 1
                                                                : 0;

  public final String fieldName;
  public final String name;
  public final Integer maxStringBytes;

  @JsonCreator
  public StringFirstAggregatorFactory(
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
    return new StringFirstAggregator(
        metricFactory.makeColumnValueSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeColumnValueSelector(fieldName),
        maxStringBytes
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new StringFirstBufferAggregator(
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
    return new StringFirstAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringFirstFoldingAggregatorFactory(name, name, maxStringBytes);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new StringFirstAggregatorFactory(fieldName, fieldName, maxStringBytes));
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
                     .put(AggregatorUtil.STRING_FIRST_CACHE_TYPE_ID)
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

    StringFirstAggregatorFactory that = (StringFirstAggregatorFactory) o;

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
