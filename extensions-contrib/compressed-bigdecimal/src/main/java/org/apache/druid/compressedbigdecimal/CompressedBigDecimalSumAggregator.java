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

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * An Aggregator to aggregate big decimal values.
 */
public class CompressedBigDecimalSumAggregator implements Aggregator
{

  private final ColumnValueSelector<CompressedBigDecimal> selector;
  private final boolean strictNumberParsing;
  private final CompressedBigDecimal sum;

  /**
   * Constructor.
   *
   * @param size                the size to allocate
   * @param scale               the scale
   * @param selector            that has the metric value
   * @param strictNumberParsing true => NumberFormatExceptions thrown; false => NumberFormatException returns 0
   */
  public CompressedBigDecimalSumAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal> selector,
      boolean strictNumberParsing
  )
  {
    this.selector = selector;
    this.strictNumberParsing = strictNumberParsing;
    this.sum = ArrayCompressedBigDecimal.allocateZero(size, scale);
  }

  @Override
  public void aggregate()
  {
    CompressedBigDecimal selectedObject = Utils.objToCompressedBigDecimalWithScale(
        selector.getObject(),
        sum.getScale(),
        strictNumberParsing
    );

    if (selectedObject != null) {
      sum.accumulateSum(selectedObject);
    }
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getLong()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
