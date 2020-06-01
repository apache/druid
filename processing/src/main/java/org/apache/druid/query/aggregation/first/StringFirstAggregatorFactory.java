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

package org.apache.druid.query.aggregation.first;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("stringFirst")
public class StringFirstAggregatorFactory extends AggregatorFactory
{
  private static final Aggregator NIL_AGGREGATOR = new StringFirstAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      0,
      false
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new StringFirstBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      0,
      false
  )
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };

  public static final int DEFAULT_MAX_STRING_SIZE = 1024;

  public static final Comparator TIME_COMPARATOR = (o1, o2) -> Longs.compare(
      ((SerializablePairLongString) o1).lhs,
      ((SerializablePairLongString) o2).lhs
  );

  public static final Comparator<SerializablePairLongString> VALUE_COMPARATOR = (o1, o2) -> {
    int comparation;

    // First we check if the objects are null
    if (o1 == null && o2 == null) {
      comparation = 0;
    } else if (o1 == null) {
      comparation = -1;
    } else if (o2 == null) {
      comparation = 1;
    } else {

      // If the objects are not null, we will try to compare using timestamp
      comparation = o1.lhs.compareTo(o2.lhs);

      // If both timestamp are the same, we try to compare the Strings
      if (comparation == 0) {

        // First we check if the strings are null
        if (o1.rhs == null && o2.rhs == null) {
          comparation = 0;
        } else if (o1.rhs == null) {
          comparation = -1;
        } else if (o2.rhs == null) {
          comparation = 1;
        } else {

          // If the strings are not null, we will compare them
          // Note: This comparation maybe doesn't make sense to first/last aggregators
          comparation = o1.rhs.compareTo(o2.rhs);
        }
      }
    }

    return comparation;
  };

  private final String fieldName;
  private final String name;
  protected final int maxStringBytes;

  @JsonCreator
  public StringFirstAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxStringBytes") Integer maxStringBytes
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
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final BaseObjectColumnValueSelector<?> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new StringFirstAggregator(
          metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
          valueSelector,
          maxStringBytes,
          StringFirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName))
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BaseObjectColumnValueSelector<?> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new StringFirstBufferAggregator(
          metricFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
          valueSelector,
          maxStringBytes,
          StringFirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName))
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new StringFirstAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringFirstAggregatorFactory(name, name, maxStringBytes);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new StringFirstAggregatorFactory(fieldName, fieldName, maxStringBytes));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePairLongString(((Number) map.get("lhs")).longValue(), ((String) map.get("rhs")));
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePairLongString) object).rhs;
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

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.STRING_FIRST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(maxStringBytes)
        .build();
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
    return "StringFirstAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           '}';
  }
}
