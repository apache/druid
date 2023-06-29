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

package org.apache.druid.compressedbigdecimal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.Collections;
import java.util.List;

public class CompressedBigDecimalMaxAggregatorFactory extends CompressedBigDecimalAggregatorFactoryBase
{
  private static final byte CACHE_TYPE_ID = 0x37;

  /**
   * Constructor.
   *
   * @param name                metric field name
   * @param fieldName           fieldName metric field name
   * @param size                size of the int array used for calculations
   * @param scale               scale of the number
   * @param strictNumberParsing if true, failure to parse strings to numbers throws an exception. otherwise 0 is
   *                            returned
   */
  @JsonCreator
  public CompressedBigDecimalMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty(value = "size", required = false) Integer size,
      @JsonProperty(value = "scale", required = false) Integer scale,
      @JsonProperty(value = "strictNumberParsing", required = false) Boolean strictNumberParsing
  )
  {
    super(name, fieldName, size, scale, strictNumberParsing, CACHE_TYPE_ID);
  }

  @Override
  protected Aggregator factorize(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector<CompressedBigDecimal> selector
  )
  {
    return new CompressedBigDecimalMaxAggregator(size, scale, selector, strictNumberParsing);
  }

  @Override
  protected BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector<CompressedBigDecimal> selector
  )
  {
    return new CompressedBigDecimalMaxBufferAggregator(size, scale, selector, strictNumberParsing);
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null && rhs == null) {
      return null;
    } else if (lhs == null) {
      return rhs;
    } else if (rhs == null) {
      return lhs;
    } else {
      // Allocate a new result and accumlate both left and right into it.
      // This ensures that the result has the correct scale, avoiding possible IllegalArgumentExceptions
      // due to truncation when the deserialized objects aren't big enough to hold the accumlated result.
      // The most common case this avoids is deserializing 0E-9 into a CompressedBigDecimal with array
      // size 1 and then accumulating a larger value into it.
      CompressedBigDecimal retVal = ArrayCompressedBigDecimal.allocateMin(size, scale);
      CompressedBigDecimal left = (CompressedBigDecimal) lhs;
      CompressedBigDecimal right = (CompressedBigDecimal) rhs;

      retVal.accumulateMax(left);
      retVal.accumulateMax(right);

      return retVal;
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CompressedBigDecimalMaxAggregatorFactory(name, name, size, scale, strictNumberParsing);
  }

  @Override
  public AggregateCombiner<CompressedBigDecimal> makeAggregateCombiner()
  {
    return new CompressedBigDecimalMaxAggregateCombiner();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new CompressedBigDecimalMaxAggregatorFactory(
        fieldName,
        fieldName,
        size,
        scale,
        strictNumberParsing
    ));
  }

  @Override
  public String toString()
  {
    return "CompressedBigDecimalMaxAggregatorFactory{" +
           "name='" + getName() + '\'' +
           ", type='" + getIntermediateType().asTypeString() + '\'' +
           ", fieldName='" + getFieldName() + '\'' +
           ", requiredFields='" + requiredFields() + '\'' +
           ", size='" + getSize() + '\'' +
           ", scale='" + getScale() + '\'' +
           ", strictNumberParsing='" + getStrictNumberParsing() + '\'' +
           '}';
  }
}
