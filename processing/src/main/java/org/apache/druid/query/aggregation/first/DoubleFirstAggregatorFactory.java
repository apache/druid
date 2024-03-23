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
import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.SerializablePairLongDoubleComplexMetricSerde;
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
import java.util.Objects;

@JsonTypeName("doubleFirst")
public class DoubleFirstAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(SerializablePairLongDoubleComplexMetricSerde.TYPE_NAME);

  private static final Aggregator NIL_AGGREGATOR = new DoubleFirstAggregator(
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

  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new DoubleFirstBufferAggregator(
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

  public static final Comparator<SerializablePair<Long, Double>> VALUE_COMPARATOR =
      SerializablePair.createNullHandlingComparator(Double::compare, true);

  private final String fieldName;
  private final String timeColumn;
  private final String name;
  private final boolean storeDoubleAsFloat;

  @JsonCreator
  public DoubleFirstAggregatorFactory(
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
    this.storeDoubleAsFloat = ColumnHolder.storeDoubleAsFloat();
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new DoubleFirstAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          FirstLastUtils.selectorNeedsFoldCheck(
              valueSelector,
              metricFactory.getColumnCapabilities(fieldName),
              SerializablePairLongDouble.class
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
      return new DoubleFirstBufferAggregator(
          metricFactory.makeColumnValueSelector(timeColumn),
          valueSelector,
          FirstLastUtils.selectorNeedsFoldCheck(
              valueSelector,
              metricFactory.getColumnCapabilities(fieldName),
              SerializablePairLongDouble.class
          )
      );
    }
  }

  @Override
  public VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory
  )
  {
    VectorValueSelector timeSelector = columnSelectorFactory.makeValueSelector(timeColumn);
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
    if (Types.isNumeric(capabilities)) {
      VectorValueSelector valueSelector = columnSelectorFactory.makeValueSelector(fieldName);
      return new DoubleFirstVectorAggregator(timeSelector, valueSelector);
    }
    VectorObjectSelector objectSelector = columnSelectorFactory.makeObjectSelector(fieldName);
    return new DoubleFirstVectorAggregator(timeSelector, objectSelector);

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
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    Long leftTime = ((SerializablePair<Long, Double>) lhs).lhs;
    Long rightTime = ((SerializablePair<Long, Double>) rhs).lhs;
    if (leftTime <= rightTime) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new GenericFirstAggregateCombiner(SerializablePairLongDouble.class);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleFirstAggregatorFactory(name, name, timeColumn);
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    if (map.get("rhs") == null) {
      return new SerializablePairLongDouble(((Number) map.get("lhs")).longValue(), null);
    }
    return new SerializablePairLongDouble(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).doubleValue());
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePairLongDouble) object).rhs;
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
    return new CacheKeyBuilder(AggregatorUtil.DOUBLE_FIRST_CACHE_TYPE_ID)
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
    // this is a copy of getComplexTypeName in the hopes that someday groupby v1 is no more and it will report it's actual
    // type of COMPLEX
    return storeDoubleAsFloat ? ColumnType.FLOAT : ColumnType.DOUBLE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // timestamp, is null, value
    return Long.BYTES + Byte.BYTES + Double.BYTES;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DoubleFirstAggregatorFactory(newName, getFieldName(), getTimeColumn());
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

    DoubleFirstAggregatorFactory that = (DoubleFirstAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && timeColumn.equals(that.timeColumn) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, timeColumn);
  }

  @Override
  public String toString()
  {
    return "DoubleFirstAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", timeColumn='" + timeColumn + '\'' +
           '}';
  }
}
