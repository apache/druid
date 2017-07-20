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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.NoopAggregator;
import io.druid.query.aggregation.NoopBufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("variance")
public class VarianceAggregatorFactory extends AggregatorFactory
{
  protected final String fieldName;
  protected final String name;
  protected final String estimator;
  private final String inputType;

  protected final boolean isVariancePop;

  @JsonCreator
  public VarianceAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("estimator") String estimator,
      @JsonProperty("inputType") String inputType
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
    this.inputType = inputType == null ? "float" : inputType;
  }

  public VarianceAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  public String getTypeName()
  {
    return "variance";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return VarianceAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return NoopAggregator.instance();
    }

    if ("float".equalsIgnoreCase(inputType)) {
      return new VarianceAggregator.FloatVarianceAggregator(metricFactory.makeFloatColumnSelector(fieldName));
    } else if ("long".equalsIgnoreCase(inputType)) {
      return new VarianceAggregator.LongVarianceAggregator(metricFactory.makeLongColumnSelector(fieldName));
    } else if ("variance".equalsIgnoreCase(inputType)) {
      return new VarianceAggregator.ObjectVarianceAggregator(selector);
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (selector == null) {
      return NoopBufferAggregator.instance();
    }
    if ("float".equalsIgnoreCase(inputType)) {
      return new VarianceBufferAggregator.FloatVarianceAggregator(
          name,
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    } else if ("long".equalsIgnoreCase(inputType)) {
      return new VarianceBufferAggregator.LongVarianceAggregator(
          name,
          metricFactory.makeLongColumnSelector(fieldName)
      );
    } else if ("variance".equalsIgnoreCase(inputType)) {
      return new VarianceBufferAggregator.ObjectVarianceAggregator(name, selector);
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new VarianceFoldingAggregatorFactory(name, name, estimator);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new VarianceAggregatorFactory(fieldName, fieldName, estimator, inputType));
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
    return VarianceAggregatorCollector.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return VarianceAggregatorCollector.combineValues(lhs, rhs);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((VarianceAggregatorCollector) object).getVariance(isVariancePop);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return VarianceAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return VarianceAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return VarianceAggregatorCollector.from(
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
  public String getEstimator()
  {
    return estimator;
  }

  @JsonProperty
  public String getInputType()
  {
    return inputType;
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
    byte[] inputTypeBytes = StringUtils.toUtf8(inputType);
    return ByteBuffer.allocate(2 + fieldNameBytes.length + 1 + inputTypeBytes.length)
                     .put(AggregatorUtil.VARIANCE_CACHE_TYPE_ID)
                     .put(isVariancePop ? (byte) 1 : 0)
                     .put(fieldNameBytes)
                     .put((byte) 0xFF)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", isVariancePop='" + isVariancePop + '\'' +
           ", inputType='" + inputType + '\'' +
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

    VarianceAggregatorFactory that = (VarianceAggregatorFactory) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(isVariancePop, that.isVariancePop)) {
      return false;
    }
    if (!Objects.equals(inputType, that.inputType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName.hashCode();
    result = 31 * result + Objects.hashCode(name);
    result = 31 * result + Objects.hashCode(isVariancePop);
    result = 31 * result + Objects.hashCode(inputType);
    return result;
  }
}
