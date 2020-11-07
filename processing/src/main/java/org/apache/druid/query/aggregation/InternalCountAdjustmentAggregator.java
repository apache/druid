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
  private final int[] appendingBytesOnRollupRows;
  private final int[] rollupRows;
  private int beforeIndex = -1;
  private int needAppendingBytes = 0;

  public InternalCountAdjustmentAggregator(CountAdjustmentHolder countAdjustmentHolder)
  {
    this.appendingBytesOnRollupRows = countAdjustmentHolder.getAppendingBytes();
    this.rollupRows = countAdjustmentHolder.getRollupRows();
  }

  @Override
  public void aggregate()
  {
    ++count;
    final int appendingIndex = findRollupRows();
    if (appendingIndex == -1) {
      needAppendingBytes = 0;
      return;
    }
    needAppendingBytes = appendingBytesOnRollupRows[appendingIndex];
  }

  private int findRollupRows()
  {
    if (beforeIndex == rollupRows.length - 1) {
      return -1;
    }
    for (int i = beforeIndex + 1; i < rollupRows.length; i++) {
      // rollupRows need sort by asc
      if (count < rollupRows[i]) {
        break;
      }
      if (count == rollupRows[i]) {
        beforeIndex = i;
        return i;
      }
    }
    return -1;
  }

  @Override
  public Object get()
  {
    return count;
  }

  @Override
  public float getFloat()
  {
    return (float) needAppendingBytes;
  }

  @Override
  public long getLong()
  {
    return needAppendingBytes;
  }

  @Override
  public double getDouble()
  {
    return (double) needAppendingBytes;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
