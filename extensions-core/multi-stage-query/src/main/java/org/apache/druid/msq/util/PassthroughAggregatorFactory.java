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

package org.apache.druid.msq.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Hack that allows "passing through" arbitrary complex types into
 * {@link org.apache.druid.segment.incremental.IncrementalIndex}.
 *
 * Used by {@link org.apache.druid.msq.exec.ControllerImpl#makeDimensionsAndAggregatorsForIngestion}.
 *
 * To move away from this, it would need to be possible to create complex columns in segments only knowing the complex
 * type; in particular, without knowing the type of an aggregator factory or dimension schema that corresponds to
 * the complex type.
 */
@JsonTypeName(PassthroughAggregatorFactory.TYPE)
@EverythingIsNonnullByDefault
public class PassthroughAggregatorFactory extends AggregatorFactory
{
  private static final int ESTIMATED_HEAP_FOOTPRINT = 800;

  static final String TYPE = "passthrough";

  private final String columnName;
  private final String complexTypeName;

  @JsonCreator
  public PassthroughAggregatorFactory(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("complexTypeName") String complexTypeName
  )
  {
    this.columnName = columnName;
    this.complexTypeName = complexTypeName;
  }

  @JsonProperty("columnName")
  public String getColumnName()
  {
    return columnName;
  }

  /**
   * The getter for this variable is named differently because the 'getComplexTypeName' is a deprecated method present
   * in the super class {@link AggregatorFactory}, and can be removed discriminately here, leading to incorrect serde of
   * the aggregator factory over the wire
   */
  @JsonProperty("complexTypeName")
  public String getComplexName()
  {
    return complexTypeName;
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new PassthroughAggregator(metricFactory.makeColumnValueSelector(columnName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return this;
  }

  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    return columnName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(columnName);
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return ESTIMATED_HEAP_FOOTPRINT;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new PassthroughAggregatorFactory(newName, complexTypeName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex(complexTypeName);
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.ofComplex(complexTypeName);
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
    PassthroughAggregatorFactory that = (PassthroughAggregatorFactory) o;
    return columnName.equals(that.columnName) && complexTypeName.equals(that.complexTypeName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, complexTypeName);
  }

  @Override
  public String toString()
  {
    return "PassthroughAggregatorFactory{" +
           "columnName='" + columnName + '\'' +
           ", complexTypeName='" + complexTypeName + '\'' +
           '}';
  }
}
