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

package org.apache.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.NoopVectorAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.cardinality.HyperLogLogCollectorAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class HyperUniquesAggregatorFactory extends AggregatorFactory
{
  public static Object estimateCardinality(@Nullable Object object, boolean round)
  {
    final HyperLogLogCollector collector = (HyperLogLogCollector) object;

    // Avoid ternary for round check as it causes estimateCardinalityRound to be cast to double.
    if (round) {
      return collector == null ? 0L : collector.estimateCardinalityRound();
    } else {
      return collector == null ? 0d : collector.estimateCardinality();
    }
  }

  private final String name;
  private final String fieldName;
  private final boolean isInputHyperUnique;
  private final boolean round;

  @JsonCreator
  public HyperUniquesAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("isInputHyperUnique") boolean isInputHyperUnique,
      @JsonProperty("round") boolean round
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.isInputHyperUnique = isInputHyperUnique;
    this.round = round;
  }

  public HyperUniquesAggregatorFactory(
      String name,
      String fieldName
  )
  {
    this(name, fieldName, false, false);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopAggregator.instance();
    }
    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || HyperLogLogCollector.class.isAssignableFrom(classOfObject)) {
      return new HyperUniquesAggregator(selector);
    }

    throw new IAE("Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, classOfObject);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopBufferAggregator.instance();
    }
    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || HyperLogLogCollector.class.isAssignableFrom(classOfObject)) {
      return new HyperUniquesBufferAggregator(selector);
    }

    throw new IAE("Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, classOfObject);
  }

  @Override
  public VectorAggregator factorizeVector(final VectorColumnSelectorFactory selectorFactory)
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    if (capabilities == null || capabilities.getType() != ValueType.COMPLEX) {
      return NoopVectorAggregator.instance();
    } else {
      return new HyperUniquesVectorAggregator(selectorFactory.makeObjectSelector(fieldName));
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.naturalNullsFirst();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return ((HyperLogLogCollector) lhs).fold((HyperLogLogCollector) rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new HyperLogLogCollectorAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name, false, round);
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
    return Collections.singletonList(new HyperUniquesAggregatorFactory(
        fieldName,
        fieldName,
        isInputHyperUnique,
        round
    ));
  }

  @Override
  public Object deserialize(Object object)
  {
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }

    return HyperLogLogCollector.makeCollector(buffer);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return estimateCardinality(object, round);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public boolean getIsInputHyperUnique()
  {
    return isInputHyperUnique;
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.HYPER_UNIQUE_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendBoolean(round)
        .build();
  }

  @Override
  public String getTypeName()
  {
    if (isInputHyperUnique) {
      return "preComputedHyperUnique";
    } else {
      return "hyperUnique";
    }
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.getLatestNumBytesForDenseStorage();
  }

  @Override
  public String toString()
  {
    return "HyperUniquesAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", isInputHyperUnique=" + isInputHyperUnique +
           ", round=" + round +
           '}';
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HyperUniquesAggregatorFactory that = (HyperUniquesAggregatorFactory) o;
    return isInputHyperUnique == that.isInputHyperUnique &&
           round == that.round &&
           Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, isInputHyperUnique, round);
  }
}
