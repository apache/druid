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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * An aggregator factory to generate longSum aggregator object.
 */
public class CompressedBigDecimalAggregatorFactory
    extends NullableNumericAggregatorFactory<ColumnValueSelector<CompressedBigDecimal<?>>>
{

  public static final int DEFAULT_SCALE = 9;
  public static final int DEFAULT_SIZE = 3;
  private static final byte CACHE_TYPE_ID = 0x37;

  public static final Comparator<CompressedBigDecimal<?>> COMPARATOR = new Comparator<CompressedBigDecimal<?>>()
  {
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int compare(CompressedBigDecimal lhs, CompressedBigDecimal rhs)
    {
      return lhs.compareTo(rhs);
    }
  };

  private final String name;
  private final String fieldName;
  private final int size;
  private final int scale;

  /**
   * Constructor.
   *
   * @param name      metric field name
   * @param fieldName fieldName metric field name
   * @param size      size of the int array used for calculations
   * @param scale     scale of the number
   */
  @JsonCreator
  public CompressedBigDecimalAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty(value = "size", required = false) Integer size,
      @JsonProperty(value = "scale", required = false) Integer scale
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.size = size == null ? DEFAULT_SIZE : size;
    this.scale = scale == null ? DEFAULT_SCALE : scale;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected ColumnValueSelector<CompressedBigDecimal<?>> selector(ColumnSelectorFactory metricFactory)
  {
    return (ColumnValueSelector<CompressedBigDecimal<?>>) metricFactory.makeColumnValueSelector(fieldName);
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory,
                                 @Nonnull ColumnValueSelector<CompressedBigDecimal<?>> selector)
  {
    return new CompressedBigDecimalAggregator(size, scale, selector);
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory,
                                               @Nonnull ColumnValueSelector<CompressedBigDecimal<?>> selector)
  {
    return new CompressedBigDecimalBufferAggregator(size, scale, selector);
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getComparator()
   */
  @Override
  public Comparator<CompressedBigDecimal<?>> getComparator()
  {
    return COMPARATOR;
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#combine(java.lang.Object, java.lang.Object)
   */
  @Nullable
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null && rhs == null) {
      return ArrayCompressedBigDecimal.allocate(size, scale);
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
      CompressedBigDecimal<?> retVal = ArrayCompressedBigDecimal.allocate(size, scale);
      CompressedBigDecimal<?> left = (CompressedBigDecimal<?>) lhs;
      CompressedBigDecimal<?> right = (CompressedBigDecimal<?>) rhs;
      if (left.signum() != 0) {
        retVal.accumulate(left);
      }
      if (right.signum() != 0) {
        retVal.accumulate(right);
      }
      return retVal;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getCombiningFactory()
   */
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CompressedBigDecimalAggregatorFactory(name, name, size, scale);
  }

  @Override
  public AggregateCombiner<CompressedBigDecimal<?>> makeAggregateCombiner()
  {
    return new CompressedBigDecimalAggregateCombiner();
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getRequiredColumns()
   */
  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new CompressedBigDecimalAggregatorFactory(
        fieldName,
        fieldName,
        size,
        scale
    ));
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#deserialize(java.lang.Object)
   */
  @Nullable
  @Override
  public Object deserialize(Object object)
  {
    if (object == null) {
      return null;
    } else if (object instanceof BigDecimal) {
      return new ArrayCompressedBigDecimal((BigDecimal) object);
    } else if (object instanceof Double) {
      return new ArrayCompressedBigDecimal(new BigDecimal((Double) object));
    } else if (object instanceof String) {
      return new ArrayCompressedBigDecimal(new BigDecimal((String) object));
    } else {
      throw new RuntimeException("unknown type in deserialize: " + object.getClass().getSimpleName());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#requiredFields()
   */
  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  /* (non-Javadoc) Get Type */
  @Override
  public ValueType getType()
  {
    return ValueType.COMPLEX;
  }
  
  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getTypeName()
   */
  @Override
  public String getComplexTypeName()
  {
    return CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL;
  }
 
  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getCacheKey()
   */
  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#finalizeComputation(java.lang.Object)
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    CompressedBigDecimal<?> compressedBigDecimal = (CompressedBigDecimal<?>) object;
    BigDecimal bigDecimal = compressedBigDecimal.toBigDecimal();
    return bigDecimal.compareTo(BigDecimal.ZERO) == 0 ? 0 : bigDecimal;
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getName()
   */
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

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getMaxIntermediateSize()
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES * size;
  }

  @Override
  public String toString()
  {
    return "CompressedBigDecimalAggregatorFactory{" +
        "name='" + getName() + '\'' +
        ", type='" + getComplexTypeName() + '\'' +
        ", fieldName='" + getFieldName() + '\'' +
        ", requiredFields='" + requiredFields() + '\'' +
        ", size='" + getSize() + '\'' +
        ", scale='" + getScale() + '\'' +
        '}';
  }
}
