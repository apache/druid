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

package org.apache.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.first.FirstLastUtils;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVectorSelectors;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("stringLast")
public class StringLastAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex("serializablePairLongString");

  private static final Aggregator NIL_AGGREGATOR = new StringLastAggregator(
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

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new StringLastBufferAggregator(
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

  private final String fieldName;
  private final String timeColumn;
  private final String name;
  protected final int maxStringBytes;

  @JsonCreator
  public StringLastAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("timeColumn") @Nullable final String timeColumn,
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
    this.timeColumn = timeColumn == null ? ColumnHolder.TIME_COLUMN_NAME : timeColumn;
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
      return new StringLastAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          maxStringBytes,
          FirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName), SerializablePairLongString.class)
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
      return new StringLastBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          maxStringBytes,
          FirstLastUtils.selectorNeedsFoldCheck(valueSelector, metricFactory.getColumnCapabilities(fieldName), SerializablePairLongString.class)
      );
    }
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {

    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    VectorValueSelector timeSelector = selectorFactory.makeValueSelector(timeColumn);

    if (Types.isNumeric(capabilities)) {
      VectorValueSelector valueSelector = selectorFactory.makeValueSelector(fieldName);
      VectorObjectSelector objectSelector = ExpressionVectorSelectors.castValueSelectorToObject(
          selectorFactory.getReadableVectorInspector(),
          fieldName,
          valueSelector,
          capabilities.toColumnType(),
          ColumnType.STRING
      );
      return new StringLastVectorAggregator(timeSelector, objectSelector, maxStringBytes);
    }

    if (capabilities != null) {
      if (capabilities.is(ValueType.STRING) && capabilities.isDictionaryEncoded().isTrue()) {
        if (!capabilities.hasMultipleValues().isTrue()) {
          SingleValueDimensionVectorSelector sSelector = selectorFactory.makeSingleValueDimensionSelector(
              DefaultDimensionSpec.of(
                  fieldName));
          return new SingleStringLastDimensionVectorAggregator(timeSelector, sSelector, maxStringBytes);
        }
      }
    }

    VectorObjectSelector vSelector = selectorFactory.makeObjectSelector(fieldName);
    if (capabilities != null) {
      return new StringLastVectorAggregator(timeSelector, vSelector, maxStringBytes);
    } else {
      return new StringLastVectorAggregator(null, vSelector, maxStringBytes);
    }
  }

  @Override
  public Comparator getComparator()
  {
    return StringFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return StringFirstAggregatorFactory.TIME_COMPARATOR.compare(lhs, rhs) > 0 ? lhs : rhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new GenericLastAggregateCombiner(SerializablePairLongString.class);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringLastAggregatorFactory(name, name, timeColumn, maxStringBytes);
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
  public String getTimeColumn()
  {
    return timeColumn;
  }

  @JsonProperty
  public Integer getMaxStringBytes()
  {
    return maxStringBytes;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(timeColumn, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.STRING_LAST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(timeColumn)
        .appendInt(maxStringBytes)
        .build();
  }

  /**
   * actual type is {@link SerializablePairLongString}
   */
  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.STRING;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + Integer.BYTES + maxStringBytes;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new StringLastAggregatorFactory(newName, getFieldName(), getTimeColumn(), getMaxStringBytes());
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
    StringLastAggregatorFactory that = (StringLastAggregatorFactory) o;
    return maxStringBytes == that.maxStringBytes &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(timeColumn, that.timeColumn) &&
           Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes, timeColumn);
  }

  @Override
  public String toString()
  {
    return "StringLastAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           ", timeColumn=" + timeColumn +
           '}';
  }
}
