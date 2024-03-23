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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This AggregatorFactory is meant to wrap the subquery used as an expression into a single value
 * and is expected to throw an exception when the subquery results in more than one row
 *
 * <p>
 * This consumes columnType as well along with name and fieldName to pass it on to underlying
 * {@link SingleValueBufferAggregator} to work with different ColumnTypes
 */
@JsonTypeName("singleValue")
public class SingleValueAggregatorFactory extends AggregatorFactory
{
  @JsonProperty
  private final String name;
  @JsonProperty
  private final String fieldName;
  @JsonProperty
  private final ColumnType columnType;
  public static final int DEFAULT_MAX_VALUE_SIZE = 1024;

  @JsonCreator
  public SingleValueAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("columnType") final ColumnType columnType
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    this.columnType = Preconditions.checkNotNull(columnType, "columnType");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    return new SingleValueAggregator(selector);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    ColumnCapabilities columnCapabilities = metricFactory.getColumnCapabilities(fieldName);
    if (columnCapabilities == null) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.DEFENSIVE)
                          .build("Unable to get the capabilities of field [%s]", fieldName);
    }
    ColumnType columnType = new ColumnType(columnCapabilities.getType(), null, null);
    return new SingleValueBufferAggregator(selector, columnType);
  }

  @Override
  public Comparator getComparator()
  {
    throw DruidException.defensive("Single Value Aggregator would not have more than one row to compare");
  }

  /**
   * Combine method would never be invoked as the broker sends the subquery to multiple segments
   * and gather the results to a single value on which the single value aggregator is applied.
   * Though getCombiningFactory would be invoked for understanding the fieldname.
   */
  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    throw DruidException.defensive("Single Value Aggregator would not have more than one row to combine");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SingleValueAggregatorFactory(name, name, columnType);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return columnType;
  }

  @Override
  public ColumnType getResultType()
  {
    return columnType;
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

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // keeping 8 bytes for all numerics to make code look simple. This would store only a single value.
    return Byte.BYTES + (columnType.isNumeric() ? Double.BYTES : DEFAULT_MAX_VALUE_SIZE);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{AggregatorUtil.SINGLE_VALUE_CACHE_TYPE_ID};
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
    SingleValueAggregatorFactory that = (SingleValueAggregatorFactory) o;
    return Objects.equals(name, that.name)
           && Objects.equals(fieldName, that.fieldName)
           && Objects.equals(columnType, that.columnType);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, columnType);
  }

  @Override
  public String toString()
  {
    return "SingleValueAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", columnType=" + columnType +
           '}';
  }
}
