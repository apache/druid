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
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.query.aggregation.SerializablePairLongLongComplexMetricSerde;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@JsonTypeName("longFirst")
public class LongFirstAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(SerializablePairLongLongComplexMetricSerde.TYPE_NAME);

  private static final Aggregator NIL_AGGREGATOR = new LongFirstAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      false
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new LongFirstBufferAggregator(
      NilColumnValueSelector.instance(),
      NilColumnValueSelector.instance(),
      false
  )
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };

  public static final Comparator<SerializablePair<Long, Long>> VALUE_COMPARATOR =
      SerializablePair.createNullHandlingComparator(Long::compare, true);

  private final String fieldName;
  private final String timeColumn;
  private final String name;

  @JsonCreator
  public LongFirstAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("timeColumn") @Nullable final String timeColumn
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.timeColumn = timeColumn == null ? ColumnHolder.TIME_COLUMN_NAME : timeColumn;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new LongFirstAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          FirstLastUtils.selectorNeedsFoldCheck(
              valueSelector,
              metricFactory.getColumnCapabilities(fieldName),
              SerializablePairLongLong.class
          )
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new LongFirstBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          FirstLastUtils.selectorNeedsFoldCheck(
              valueSelector,
              metricFactory.getColumnCapabilities(fieldName),
              SerializablePairLongLong.class
          )
      );
    }
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    VectorValueSelector timeSelector = columnSelectorFactory.makeValueSelector(timeColumn);
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
    if (Types.isNumeric(capabilities)) {
      VectorValueSelector valueSelector = columnSelectorFactory.makeValueSelector(fieldName);
      return new LongFirstVectorAggregator(timeSelector, valueSelector);
    }
    VectorObjectSelector objectSelector = columnSelectorFactory.makeObjectSelector(fieldName);
    return new LongFirstVectorAggregator(timeSelector, objectSelector);
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    Long leftTime = ((SerializablePair<Long, Long>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Long>) rhs).lhs;
    if (leftTime <= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new GenericFirstAggregateCombiner(SerializablePairLongLong.class);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongFirstAggregatorFactory(name, name, timeColumn);
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (map.get("rhs") == null) {
      return new SerializablePairLongLong(((Number) map.get("lhs")).longValue(), null);
    }
    return new SerializablePairLongLong(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).longValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePairLongLong) object).rhs;
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

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(timeColumn, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.LONG_FIRST_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(timeColumn)
        .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Long.BYTES;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new LongFirstAggregatorFactory(newName, getFieldName(), getTimeColumn());
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

    LongFirstAggregatorFactory that = (LongFirstAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && timeColumn.equals(that.timeColumn) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "LongFirstAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", timeColumn='" + timeColumn + '\'' +
           '}';
  }
}
