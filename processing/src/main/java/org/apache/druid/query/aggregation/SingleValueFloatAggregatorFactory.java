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
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName("singleValueFloat")
public class SingleValueFloatAggregatorFactory extends SingleValueAggregatorFactory
{

  @JsonCreator
  public SingleValueFloatAggregatorFactory(
          @JsonProperty("name") String name,
          @JsonProperty("fieldName") final String fieldName
  )
  {
      super(name, fieldName);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    final BaseLongColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(getFieldName());
    return new SingleValueFloatAggregator(
            valueSelector
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    final BaseFloatColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(getFieldName());
    return new SingleValueFloatBufferAggregator(valueSelector);
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return new SingleValueFloatAggregatorFactory(getName(), getName());
  }

  @Override
  public Object deserialize(Object object)
  {
    return object == null ? null : (Float) object;
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : (Float) object;
  }

  @Override
  public int getMaxIntermediateSize() {
    return Float.BYTES;
  }

  @Override
  public byte[] getCacheKey() {
    return new byte[]{AggregatorUtil.SINGLE_VALUE_CACHE_TYPE_ID};
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.FLOAT;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.FLOAT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SingleValueFloatAggregatorFactory that = (SingleValueFloatAggregatorFactory) o;
    return Objects.equals(getName(), that.getName()) && Objects.equals(getFieldName(), that.getFieldName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getFieldName());
  }

  @Override
  public String toString() {
    return "SingleValueFloatAggregatorFactory{" +
            "name='" + getName() + '\'' +
            ", fieldName='" + getFieldName() + '\'' +
            '}';
  }
}
