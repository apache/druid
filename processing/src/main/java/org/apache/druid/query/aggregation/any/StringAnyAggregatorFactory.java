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

package org.apache.druid.query.aggregation.any;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class StringAnyAggregatorFactory extends AggregatorFactory
{
  private static final Comparator<String> VALUE_COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

  private final String fieldName;
  private final String name;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;

  @JsonCreator
  public StringAnyAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxStringBytes") Integer maxStringBytes,
      @JsonProperty("aggregateMultipleValues") @Nullable final Boolean aggregateMultipleValues
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    if (maxStringBytes != null && maxStringBytes < 0) {
      throw new IAE("maxStringBytes must be greater than 0");
    }
    this.name = name;
    this.fieldName = fieldName;
    this.maxStringBytes = maxStringBytes == null
                          ? StringFirstAggregatorFactory.DEFAULT_MAX_STRING_SIZE
                          : maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues == null ? true : aggregateMultipleValues;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new StringAnyAggregator(metricFactory.makeColumnValueSelector(fieldName), maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new StringAnyBufferAggregator(metricFactory.makeColumnValueSelector(fieldName), maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public StringAnyVectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    // null capabilities mean the column doesn't exist, so in vector engines the selector will never be multi-value
    if (capabilities != null && capabilities.hasMultipleValues().isMaybeTrue()) {
      return new StringAnyVectorAggregator(
          null,
          selectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(fieldName)),
          maxStringBytes,
          aggregateMultipleValues
      );
    } else {
      return new StringAnyVectorAggregator(
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
    return true;
  }

  @Override
  public Comparator getComparator()
  {
    return StringAnyAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringAnyAggregatorFactory(name, name, maxStringBytes, aggregateMultipleValues);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
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
  public Integer getMaxStringBytes()
  {
    return maxStringBytes;
  }
  @JsonProperty
  public boolean getAggregateMultipleValues()
  {
    return aggregateMultipleValues;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.STRING_ANY_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(maxStringBytes)
        .build();
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
  public AggregatorFactory withName(String newName)
  {
    return new StringAnyAggregatorFactory(newName, getFieldName(), getMaxStringBytes(), getAggregateMultipleValues());
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
    StringAnyAggregatorFactory that = (StringAnyAggregatorFactory) o;
    return maxStringBytes == that.maxStringBytes &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes);
  }

  @Override
  public String toString()
  {
    return "StringAnyAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           '}';
  }
}
