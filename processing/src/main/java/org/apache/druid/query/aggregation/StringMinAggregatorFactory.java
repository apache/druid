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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class StringMinAggregatorFactory extends AggregatorFactory
{
  private static final Comparator<String> VALUE_COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());

  public static final int DEFAULT_MAX_STRING_SIZE = 1024;
  private final int maxStringBytes;
  private final String name;
  private final String fieldName;
  private final boolean aggregateMultipleValues;

  @JsonCreator
  public StringMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("maxStringBytes") @Nullable Integer maxStringBytes,
      @JsonProperty("aggregateMultipleValues") @Nullable final Boolean aggregateMultipleValues
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name.");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null field name.");

    this.name = name;
    this.fieldName = fieldName;

    if (maxStringBytes == null) {
      this.maxStringBytes = DEFAULT_MAX_STRING_SIZE;
    } else {
      if (maxStringBytes < 0) {
        throw new IAE("maxStringBytes must be greater than 0");
      }

      this.maxStringBytes = maxStringBytes;
    }

    this.aggregateMultipleValues = aggregateMultipleValues == null || aggregateMultipleValues;
  }

  public StringMinAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    @SuppressWarnings("unchecked")
    BaseObjectColumnValueSelector<String> selector = metricFactory.makeColumnValueSelector(fieldName);
    return new StringMinAggregator(selector, maxStringBytes);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    @SuppressWarnings("unchecked")
    BaseObjectColumnValueSelector<String> selector = metricFactory.makeColumnValueSelector(fieldName);
    return new StringMinBufferAggregator(selector, maxStringBytes);
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    // null capabilities mean the column doesn't exist, so in vector engines the selector will never be multi-value
    // canVectorize ensures that nonnull capabilities => dict-encoded string
    if (capabilities != null && capabilities.hasMultipleValues().isMaybeTrue()) {
      return new StringMinVectorAggregator(
          null,
          selectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(fieldName)),
          maxStringBytes,
          aggregateMultipleValues
      );
    } else {
      return new StringMinVectorAggregator(
          selectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(fieldName)),
          null,
          maxStringBytes,
          aggregateMultipleValues
      );
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
    return capabilities == null
           || (capabilities.is(ValueType.STRING) && capabilities.isDictionaryEncoded().isTrue());
  }

  @Override
  public Comparator<String> getComparator()
  {
    return StringMinAggregatorFactory.VALUE_COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    return StringMinAggregator.combineValues((String) lhs, (String) rhs);
  }

  @JsonIgnore
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringMinAggregatorFactory(name, name, maxStringBytes, aggregateMultipleValues);
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
  public Object deserialize(Object object)
  {
    return object.toString(); // Assuming the object can be converted to a String
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.STRING;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.STRING;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES + maxStringBytes;
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return getMaxIntermediateSize();
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new StringMinAggregatorFactory(newName, fieldName, maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.STRING_MIN_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(maxStringBytes)
        .build();
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

    StringMinAggregatorFactory that = (StringMinAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(maxStringBytes, that.maxStringBytes)) {
      return false;
    }
    return Objects.equals(aggregateMultipleValues, that.aggregateMultipleValues);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public String toString()
  {
    return "StringMinAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           ", aggregateMultipleValues=" + aggregateMultipleValues +
           '}';
  }
}
