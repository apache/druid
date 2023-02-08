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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public abstract class CompressedBigDecimalAggregatorFactoryBase
    extends NullableNumericAggregatorFactory<ColumnValueSelector<CompressedBigDecimal>>
{
  public static final int DEFAULT_SCALE = 9;
  public static final int DEFAULT_SIZE = 6;
  public static final boolean DEFAULT_STRICT_NUMBER_PARSING = false;
  public static final int BUFFER_AGGREGATOR_HEADER_SIZE_BYTES = 1;


  public static final Comparator<CompressedBigDecimal> COMPARATOR = CompressedBigDecimal::compareTo;

  protected final String name;
  protected final String fieldName;
  protected final int size;
  protected final int scale;
  protected final boolean strictNumberParsing;

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
   * @param cacheTypeId
   */
  protected CompressedBigDecimalAggregatorFactoryBase(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty(value = "size", required = false) Integer size,
      @JsonProperty(value = "scale", required = false) Integer scale,
      @JsonProperty(value = "strictNumberParsing", required = false) Boolean strictNumberParsing,
      byte cacheTypeId
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.size = size == null ? DEFAULT_SIZE : size;
    this.scale = scale == null ? DEFAULT_SCALE : scale;
    this.strictNumberParsing = strictNumberParsing == null ? DEFAULT_STRICT_NUMBER_PARSING : strictNumberParsing;

    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    cacheKey = ByteBuffer.allocate(1 + fieldNameBytes.length + 2 * Integer.BYTES + 1)
                         .put(cacheTypeId)
                         .put(fieldNameBytes)
                         .putInt(this.size)
                         .putInt(this.scale)
                         .put((byte) (this.strictNumberParsing ? 1 : 0))
                         .array();
  }

  @SuppressWarnings("unchecked")
  @Override
  public ColumnValueSelector<CompressedBigDecimal> selector(ColumnSelectorFactory metricFactory)
  {
    return (ColumnValueSelector<CompressedBigDecimal>) metricFactory.makeColumnValueSelector(fieldName);
  }

  @Override
  protected abstract Aggregator factorize(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector<CompressedBigDecimal> selector
  );
  @Override
  protected abstract BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector<CompressedBigDecimal> selector
  );

  @Override
  public Comparator<CompressedBigDecimal> getComparator()
  {
    return COMPARATOR;
  }

  @Nullable
  @Override
  public abstract Object combine(Object lhs, Object rhs);

  @Override
  public abstract AggregatorFactory getCombiningFactory();

  @Override
  public abstract AggregateCombiner<CompressedBigDecimal> makeAggregateCombiner();

  @Override
  public abstract List<AggregatorFactory> getRequiredColumns();

  @Override
  public abstract String toString();

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
    return BUFFER_AGGREGATOR_HEADER_SIZE_BYTES + Integer.BYTES * size;
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
    CompressedBigDecimalAggregatorFactoryBase that = (CompressedBigDecimalAggregatorFactoryBase) o;
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
}
