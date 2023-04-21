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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * A vector aggregator that returns a constant value.
 */
public class ConstantVectorAggregator implements VectorAggregator
{
  private static final ConstantVectorAggregator DOUBLE_NIL_VECTOR_AGGREGATOR = new ConstantVectorAggregator(
      NullHandling.defaultDoubleValue()
  );

  private static final ConstantVectorAggregator FLOAT_NIL_VECTOR_AGGREGATOR = new ConstantVectorAggregator(
      NullHandling.defaultFloatValue()
  );

  private static final ConstantVectorAggregator LONG_NIL_VECTOR_AGGREGATOR = new ConstantVectorAggregator(
      NullHandling.defaultLongValue()
  );

  /**
   * @return A vectorized aggregator that returns the default double value.
   */
  public static ConstantVectorAggregator nilDouble()
  {
    return DOUBLE_NIL_VECTOR_AGGREGATOR;
  }

  /**
   * @return A vectorized aggregator that returns the default float value.
   */
  public static ConstantVectorAggregator nilFloat()
  {
    return FLOAT_NIL_VECTOR_AGGREGATOR;
  }

  /**
   * @return A vectorized aggregator that returns the default long value.
   */
  public static ConstantVectorAggregator nilLong()
  {
    return LONG_NIL_VECTOR_AGGREGATOR;
  }

  /**
   * @return A vectorized aggregator that returns the default long value.
   */
  public static ConstantVectorAggregator of(final Object value)
  {
    return new ConstantVectorAggregator(value);
  }

  @Nullable
  private final Object returnValue;

  private ConstantVectorAggregator(@Nullable Object returnValue)
  {
    this.returnValue = returnValue;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    // Do nothing
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    // Do nothing.
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    // Do nothing.
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return returnValue;
  }

  @Override
  public void close()
  {
    // Do nothing.
  }
}
