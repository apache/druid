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
import com.google.common.base.Objects;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * An aggregator factory to generate longSum aggregator object.
 */
public class CompressedBigDecimalSumAggregatorFactory
    extends NullableNumericAggregatorFactory<ColumnValueSelector<CompressedBigDecimal>>
{

  public static final int DEFAULT_SCALE = 9;
  public static final int DEFAULT_SIZE = 6;
  public static final boolean DEFAULT_STRICT_NUMBER_PARSING = false;

  private static final byte CACHE_TYPE_ID = 0x37;

  public static final Comparator<CompressedBigDecimal> COMPARATOR = CompressedBigDecimal::compareTo;

  private final String name;
  private final String fieldName;
  private final int size;
  private final int scale;
  private final boolean strictNumberParsing;
  private final byte[] cacheKey;

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
  public CompressedBigDecimalSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty(value = "size", required = false) Integer size,
      @JsonProperty(value = "scale", required = false) Integer scale,
      @JsonProperty(value = "strictNumberParsing", required = false) Boolean strictNumberParsing
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.size = size == null ? DEFAULT_SIZE : size;
    this.scale = scale == null ? DEFAULT_SCALE : scale;
    this.strictNumberParsing = strictNumberParsing == null ? DEFAULT_STRICT_NUMBER_PARSING : strictNumberParsing;

    cacheKey = ByteBuffer.allocate(1 + StringUtils.toUtf8(fieldName).length + 2 * Integer.BYTES + 1)
                         .put(CACHE_TYPE_ID)
                         .put(StringUtils.toUtf8(fieldName))
                         .putInt(this.size)
                         .putInt(this.scale)
                         .put((byte) (this.strictNumberParsing ? 1 : 0))
                         .array();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected ColumnValueSelector<CompressedBigDecimal> selector(ColumnSelectorFactory metricFactory)
  {
    return (ColumnValueSelector<CompressedBigDecimal>) metricFactory.makeColumnValueSelector(fieldName);
  }

  @Override
  protected Aggregator factorize(
      ColumnSelectorFactory metricFactory,
      @Nonnull ColumnValueSelector<CompressedBigDecimal> selector
  )
  {
    return new CompressedBigDecimalSumAggregator(size, scale, selector, strictNumberParsing);
  }

  @Override
  protected BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      @Nonnull ColumnValueSelector<CompressedBigDecimal> selector
  )
  {
    return new CompressedBigDecimalSumBufferAggregator(size, scale, selector, strictNumberParsing);
  }

  @Override
  public Comparator<CompressedBigDecimal> getComparator()
  {
    return COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null && rhs == null) {
      return ArrayCompressedBigDecimal.allocateZero(size, scale);
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
      CompressedBigDecimal retVal = ArrayCompressedBigDecimal.allocateZero(size, scale);
      CompressedBigDecimal left = (CompressedBigDecimal) lhs;
      CompressedBigDecimal right = (CompressedBigDecimal) rhs;
      if (!left.isZero()) {
        retVal.accumulateSum(left);
      }
      if (!right.isZero()) {
        retVal.accumulateSum(right);
      }
      return retVal;
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CompressedBigDecimalSumAggregatorFactory(name, name, size, scale, strictNumberParsing);
  }

  @Override
  public AggregateCombiner<CompressedBigDecimal> makeAggregateCombiner()
  {
    return new CompressedBigDecimalSumAggregateCombiner();
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new CompressedBigDecimalSumAggregatorFactory(
        name,
        fieldName,
        size,
        scale,
        strictNumberParsing
    ));
  }

  @Nullable
  @Override
  public Object deserialize(Object object)
  {
    return Utils.objToCompressedBigDecimal(object);
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex(CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL);
  }

  @Override
  public byte[] getCacheKey()
  {
    return cacheKey;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  /**
   * Get the filed name.
   *
   * @return dimension/metric field name
   */
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getScale()
  {
    return scale;
  }

  @JsonProperty
  public int getSize()
  {
    return size;
  }

  @JsonProperty
  public boolean getStrictNumberParsing()
  {
    return strictNumberParsing;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES * size;
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
    CompressedBigDecimalSumAggregatorFactory that = (CompressedBigDecimalSumAggregatorFactory) o;
    return size == that.size
           && scale == that.scale
           && Objects.equal(name, that.name)
           && Objects.equal(fieldName, that.fieldName)
           && Objects.equal(strictNumberParsing, that.strictNumberParsing);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(name, fieldName, size, scale, strictNumberParsing);
  }

  @Override
  public String toString()
  {
    return "CompressedBigDecimalSumAggregatorFactory{" +
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
