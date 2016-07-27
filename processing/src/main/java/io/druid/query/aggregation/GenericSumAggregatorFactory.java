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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.metamx.common.StringUtils;
import io.druid.data.ValueType;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class GenericSumAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x17;

  private final String fieldName;
  private final String name;
  private final ValueType inputType;

  @JsonCreator
  public GenericSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("inputType") final ValueType inputType
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.inputType = inputType == null ? ValueType.DOUBLE : inputType;
    Preconditions.checkArgument(ValueType.isNumeric(inputType));
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    switch (inputType) {
      case FLOAT:
        return new DoubleSumAggregator.FloatInput(name, metricFactory.makeFloatColumnSelector(fieldName));
      case DOUBLE:
        return new DoubleSumAggregator.DoubleInput(name, metricFactory.makeDoubleColumnSelector(fieldName));
      case LONG:
        return new LongSumAggregator(name, metricFactory.makeLongColumnSelector(fieldName));
    }
    throw new IllegalStateException();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    switch (inputType) {
      case FLOAT:
        return new DoubleSumBufferAggregator.FloatInput(metricFactory.makeFloatColumnSelector(fieldName));
      case DOUBLE:
        return new DoubleSumBufferAggregator.DoubleInput(metricFactory.makeDoubleColumnSelector(fieldName));
      case LONG:
        return new LongSumBufferAggregator(metricFactory.makeLongColumnSelector(fieldName));
    }
    throw new IllegalStateException();
  }

  @Override
  public Comparator getComparator()
  {
    return inputType.comparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    switch (inputType) {
      case FLOAT:
      case DOUBLE:
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
      case LONG:
        return ((Number) lhs).longValue() + ((Number) rhs).longValue();
    }
    throw new IllegalStateException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    switch (inputType) {
      case FLOAT:
      case DOUBLE:
        return new GenericSumAggregatorFactory(name, name, ValueType.DOUBLE);
      case LONG:
        return new GenericSumAggregatorFactory(name, name, ValueType.LONG);
    }
    throw new IllegalStateException();
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
    return Arrays.<AggregatorFactory>asList(new GenericSumAggregatorFactory(fieldName, fieldName, inputType));
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getInputType()
  {
    return inputType.toString();
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] inputTypeBytes = StringUtils.toUtf8(inputType.name());

    return ByteBuffer.allocate(1 + fieldNameBytes.length + inputTypeBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return inputType.name().toLowerCase();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0D;
  }

  @Override
  public String toString()
  {
    return "GenericSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           "inputType='" + inputType + '\'' +
           ", name='" + name + '\'' +
           '}';
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

    GenericSumAggregatorFactory that = (GenericSumAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(inputType, that.inputType)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, inputType, name);
  }
}
