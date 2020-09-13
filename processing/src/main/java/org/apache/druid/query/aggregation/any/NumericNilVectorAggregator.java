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
 * A vector aggregator that returns the default numeric value.
 */
public abstract class NumericNilVectorAggregator implements VectorAggregator
{
  /**
   * @return A vectorized aggregator that returns the default double value.
   */
  public static DoubleNilVectorAggregator doubleNilVectorAggregator()
  {
    return DoubleNilVectorAggregator.INSTANCE;
  }

  /**
   * @return A vectorized aggregator that returns the default float value.
   */
  public static FloatNilVectorAggregator floatNilVectorAggregator()
  {
    return FloatNilVectorAggregator.INSTANCE;
  }

  /**
   * @return A vectorized aggregator that returns the default long value.
   */
  public static LongNilVectorAggregator longNilVectorAggregator()
  {
    return LongNilVectorAggregator.INSTANCE;
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
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {
    // Do nothing.
  }

  @Override
  public void close()
  {
    // Do nothing.
  }

  public static class DoubleNilVectorAggregator extends NumericNilVectorAggregator
  {
    private static final DoubleNilVectorAggregator INSTANCE = new DoubleNilVectorAggregator();

    @Nullable
    private final Double returnValue;

    private DoubleNilVectorAggregator()
    {
      this.returnValue = NullHandling.defaultDoubleValue();
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return returnValue;
    }
  }

  public static class LongNilVectorAggregator extends NumericNilVectorAggregator
  {
    private static final LongNilVectorAggregator INSTANCE = new LongNilVectorAggregator();

    @Nullable
    private final Long returnValue;

    private LongNilVectorAggregator()
    {
      this.returnValue = NullHandling.defaultLongValue();
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return returnValue;
    }
  }

  public static class FloatNilVectorAggregator extends NumericNilVectorAggregator
  {
    private static final FloatNilVectorAggregator INSTANCE = new FloatNilVectorAggregator();

    @Nullable
    private final Float returnValue;

    private FloatNilVectorAggregator()
    {
      this.returnValue = NullHandling.defaultFloatValue();
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return returnValue;
    }
  }
}
