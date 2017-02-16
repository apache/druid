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

package io.druid.query.aggregation.avg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("avg")
public class AvgAggregatorFactory extends AggregatorFactory
{
  public static FloatColumnSelector asFloatColumnSelector(final ObjectColumnSelector selector)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        return (float) ((AvgAggregatorCollector) selector.get()).compute();
      }
    };
  }

  public static LongColumnSelector asLongColumnSelector(final ObjectColumnSelector selector)
  {
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        return (long) ((AvgAggregatorCollector) selector.get()).compute();
      }
    };
  }

  protected static final byte CACHE_TYPE_ID = 22;

  private final String name;
  private final String fieldName;
  private final String inputType;

  @JsonCreator
  public AvgAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("inputType") String inputType
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.inputType = inputType == null ? "float" : inputType;
  }

  public AvgAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null);
  }

  @Override
  public String getTypeName()
  {
    return "avg";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return AvgAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return Aggregators.noopAggregator();
    }
    if ("float".equalsIgnoreCase(inputType)) {
      return new AvgAggregator.FloatAvgAggregator(
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    } else if ("long".equalsIgnoreCase(inputType)) {
      return new AvgAggregator.LongAvgAggregator(
          metricFactory.makeLongColumnSelector(fieldName)
      );
    } else if ("avg".equalsIgnoreCase(inputType)) {
      return new AvgAggregator.ObjectAvgAggregator(selector);
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a float, long or avg, got a %s", fieldName, inputType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return Aggregators.noopBufferAggregator();
    }

    if ("float".equalsIgnoreCase(inputType)) {
      return new AvgBufferAggregator.FloatAvgBufferAggregator(
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    } else if ("long".equalsIgnoreCase(inputType)) {
      return new AvgBufferAggregator.LongAvgBufferAggregator(
          metricFactory.makeLongColumnSelector(fieldName)
      );
    } else if ("avg".equalsIgnoreCase(inputType)) {
      return new AvgBufferAggregator.ObjectAvgBufferAggregator(selector);
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a float, long or avg, got a %s", fieldName, inputType
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new AvgAggregatorFactory(name, name, getTypeName());
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new AvgAggregatorFactory(fieldName, fieldName, inputType));
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (Objects.equals(getName(), other.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Comparator getComparator()
  {
    return AvgAggregatorCollector.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return AvgAggregatorCollector.combineValues(lhs, rhs);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((AvgAggregatorCollector) object).compute();
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return AvgAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return AvgAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return AvgAggregatorCollector.from(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
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
    return inputType;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = com.metamx.common.StringUtils.toUtf8(fieldName);
    byte[] inputTypeBytes = com.metamx.common.StringUtils.toUtf8(inputType);

    return ByteBuffer.allocate(1 + fieldNameBytes.length + inputTypeBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(inputTypeBytes)
                     .array();
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

    AvgAggregatorFactory that = (AvgAggregatorFactory) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }
    return inputType != null ? inputType.equals(that.inputType) : that.inputType == null;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + (inputType != null ? inputType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "AvgAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", inputType='" + inputType + '\'' +
           '}';
  }
}
