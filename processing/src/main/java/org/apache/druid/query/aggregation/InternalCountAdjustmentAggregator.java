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

/**
 */
public class InternalCountAdjustmentAggregator implements Aggregator
{
  private long count = 0;
  private int startIndex = 0;
  private final int[] appendingBytesOnRollupRows;
  private final int[] rollupRows;

  public InternalCountAdjustmentAggregator(CountAdjustmentHolder countAdjustmentHolder)
  {
    this.appendingBytesOnRollupRows = countAdjustmentHolder.getAppendingBytes();
    this.rollupRows = countAdjustmentHolder.getRollupRows();
  }

  @Override
  public void aggregate()
  {
    ++count;
  }

  @Override
  public Object get()
  {
    return count;
  }

  @Override
  public float getFloat()
  {
    return (float) count;
  }

  @Override
  public long getLong()
  {
    if (startIndex == rollupRows.length) {
      return 0;
    }
    // rollupRows need sort by asc
    return count == rollupRows[startIndex] ? appendingBytesOnRollupRows[startIndex++] : 0;
  }

  @Override
  public double getDouble()
  {
    return (double) count;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
