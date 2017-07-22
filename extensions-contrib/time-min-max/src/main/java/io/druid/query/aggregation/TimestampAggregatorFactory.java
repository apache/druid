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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.segment.ColumnSelectorFactory;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TimestampAggregatorFactory extends AggregatorFactory
{
  final String name;
  final String fieldName;
  final String timeFormat;
  private final Comparator<Long> comparator;
  private final Long initValue;

  private TimestampSpec timestampSpec;

  TimestampAggregatorFactory(
      String name,
      String fieldName,
      String timeFormat,
      Comparator<Long> comparator,
      Long initValue
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.timeFormat = timeFormat;
    this.comparator = comparator;
    this.initValue = initValue;

    this.timestampSpec = new TimestampSpec(fieldName, timeFormat, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new TimestampAggregator(name, metricFactory.makeObjectColumnSelector(fieldName), timestampSpec, comparator, initValue);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new TimestampBufferAggregator(metricFactory.makeObjectColumnSelector(fieldName), timestampSpec, comparator, initValue);
  }

  @Override
  public Comparator getComparator()
  {
    return TimestampAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return TimestampAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new TimestampAggregatorFactory(name, name, timeFormat, comparator, initValue);
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
    return Arrays.<AggregatorFactory>asList(new TimestampAggregatorFactory(fieldName, fieldName, timeFormat, comparator, initValue));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return new DateTime((long)object);
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
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length)
                     .put(AggregatorUtil.TIMESTAMP_CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "long";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
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

    TimestampAggregatorFactory that = (TimestampAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(comparator, that.comparator)) {
      return false;
    }
    if (!Objects.equals(initValue, that.initValue)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (comparator != null ? comparator.hashCode() : 0);
    result = 31 * result + (initValue != null ? initValue.hashCode() : 0);
    return result;
  }

  static Long convertLong(TimestampSpec timestampSpec, Object input)
  {
    if (input instanceof Number) {
      return ((Number)input).longValue();
    } else if (input instanceof DateTime) {
      return ((DateTime)input).getMillis();
    } else if (input instanceof Timestamp) {
      return ((Timestamp)input).getTime();
    } else if (input instanceof String) {
      return timestampSpec.parseDateTime(input).getMillis();
    }

    return null;
  }
}
