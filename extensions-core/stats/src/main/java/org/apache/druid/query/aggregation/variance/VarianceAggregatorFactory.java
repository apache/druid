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

package org.apache.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("variance")
public class VarianceAggregatorFactory extends AggregatorFactory
{
  private static final String VARIANCE_TYPE_NAME = "variance";
  protected final String fieldName;
  protected final String name;
  @Nullable
  protected final String estimator;
  @Nullable
  private final String inputType;

  protected final boolean isVariancePop;

  @JsonCreator
  public VarianceAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("estimator") @Nullable String estimator,
      @JsonProperty("inputType") @Nullable String inputType
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
    this.inputType = inputType;
  }

  public VarianceAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  public String getTypeName()
  {
    return VARIANCE_TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return VarianceAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector<?> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopAggregator.instance();
    }

    final String type = getTypeString(metricFactory);

    if (ValueType.FLOAT.name().equalsIgnoreCase(type)) {
      return new VarianceAggregator.FloatVarianceAggregator(selector);
    } else if (ValueType.DOUBLE.name().equalsIgnoreCase(type)) {
      return new VarianceAggregator.DoubleVarianceAggregator(selector);
    } else if (ValueType.LONG.name().equalsIgnoreCase(type)) {
      return new VarianceAggregator.LongVarianceAggregator(selector);
    } else if (VARIANCE_TYPE_NAME.equalsIgnoreCase(type)) {
      return new VarianceAggregator.ObjectVarianceAggregator(selector);
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long, or variance, but got a %s",
        fieldName,
        inputType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector<?> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopBufferAggregator.instance();
    }
    final String type = getTypeString(metricFactory);

    if (ValueType.FLOAT.name().equalsIgnoreCase(type)) {
      return new VarianceBufferAggregator.FloatVarianceAggregator(selector);
    } else if (ValueType.DOUBLE.name().equalsIgnoreCase(type)) {
      return new VarianceBufferAggregator.DoubleVarianceAggregator(selector);
    } else if (ValueType.LONG.name().equalsIgnoreCase(type)) {
      return new VarianceBufferAggregator.LongVarianceAggregator(selector);
    } else if (VARIANCE_TYPE_NAME.equalsIgnoreCase(type)) {
      return new VarianceBufferAggregator.ObjectVarianceAggregator(selector);
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long, or variance, but got a %s",
        fieldName,
        inputType
    );
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return VarianceAggregatorCollector.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    // VarianceAggregatorFactory.combine() delegates to VarianceAggregatorCollector.combineValues() and it doesn't check
    // for nulls, so this AggregateCombiner neither.
    return new ObjectAggregateCombiner<VarianceAggregatorCollector>()
    {
      private final VarianceAggregatorCollector combined = new VarianceAggregatorCollector();

      @Override
      public void reset(ColumnValueSelector selector)
      {
        VarianceAggregatorCollector first = (VarianceAggregatorCollector) selector.getObject();
        combined.copyFrom(first);
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        VarianceAggregatorCollector other = (VarianceAggregatorCollector) selector.getObject();
        combined.fold(other);
      }

      @Override
      public Class<VarianceAggregatorCollector> classOfObject()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector getObject()
      {
        return combined;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new VarianceFoldingAggregatorFactory(name, name, estimator);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new VarianceAggregatorFactory(fieldName, fieldName, estimator, inputType));
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (Objects.equals(getName(), other.getName()) && other instanceof VarianceAggregatorFactory) {
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

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((VarianceAggregatorCollector) object).getVariance(isVariancePop);
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
          ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)))
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

  @Nullable
  @JsonProperty
  public String getEstimator()
  {
    return estimator;
  }

  @JsonProperty
  public String getInputType()
  {
    return inputType == null ? StringUtils.toLowerCase(ValueType.FLOAT.name()) : inputType;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.VARIANCE_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(inputType)
        .appendBoolean(isVariancePop)
        .appendString(estimator)
        .build();
  }

  @Override
  public String toString()
  {
    return "VarianceAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", estimator='" + estimator + '\'' +
           ", inputType='" + inputType + '\'' +
           ", isVariancePop=" + isVariancePop +
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
    return isVariancePop == that.isVariancePop &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(name, that.name) &&
           Objects.equals(estimator, that.estimator) &&
           Objects.equals(inputType, that.inputType);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(fieldName, name, estimator, inputType, isVariancePop);
  }

  private String getTypeString(ColumnSelectorFactory metricFactory)
  {
    String type = inputType;
    if (type == null) {
      ColumnCapabilities capabilities = metricFactory.getColumnCapabilities(fieldName);
      if (capabilities != null) {
        type = StringUtils.toLowerCase(capabilities.getType().name());
      } else {
        type = StringUtils.toLowerCase(ValueType.FLOAT.name());
      }
    }
    return type;
  }

}
