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
public class CompressedBigDecimalAggregator implements Aggregator
{

  private final ColumnValueSelector<CompressedBigDecimal<?>> selector;
  private final CompressedBigDecimal<?> sum;

  /**
   * Constructor.
   *
   * @param size     the size to allocate
   * @param scale    the scale
   * @param selector that has the metric value
   */
  public CompressedBigDecimalAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal<?>> selector
  )
  {
    this.selector = selector;
    this.sum = ArrayCompressedBigDecimal.allocate(size, scale);
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.Aggregator#aggregate()
   */
  @Override
  public void aggregate()
  {
    CompressedBigDecimal<?> selectedObject = selector.getObject();
    if (selectedObject != null) {
      if (selectedObject.getScale() != sum.getScale()) {
        selectedObject = Utils.scaleUp(selectedObject);
      }
      sum.accumulate(selectedObject);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.Aggregator#get()
   */
  @Override
  public Object get()
  {
    return sum;
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.Aggregator#getFloat()
   */
  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("CompressedBigDecimalAggregator does not support getFloat()");
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.Aggregator#getLong()
   */
  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("CompressedBigDecimalAggregator does not support getLong()");
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.Aggregator#close()
   */
  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
