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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CountAdjustmentHolder
{
  private static final Logger log = new Logger(CountAdjustmentHolder.class);
  private final HashMap<String, OnheapIncrementalIndex.MetricAdjustmentHolder> metricTypeAndHolderMap;
  @Nullable
  private int[] appendBytesAll;
  @Nullable
  private int[] rollupRowsAll;

  public CountAdjustmentHolder(HashMap<String, OnheapIncrementalIndex.MetricAdjustmentHolder> metricTypeAndHolderMap)
  {
    this.metricTypeAndHolderMap = metricTypeAndHolderMap;
    initAdjustment();
    if (log.isDebugEnabled()) {
      log.debug("Adjustment info:rollupRows:%s,appendBytes:%s", Arrays.toString(rollupRowsAll), Arrays.toString(appendBytesAll));
    }
  }

  private void initAdjustment()
  {
    final Iterator<Map.Entry<String, OnheapIncrementalIndex.MetricAdjustmentHolder>> iterator = metricTypeAndHolderMap.entrySet().iterator();
    while (iterator.hasNext()) {
      final OnheapIncrementalIndex.MetricAdjustmentHolder metricAdjustmentHolder = iterator.next().getValue();
      // merge sort and when rollupRow equals then sum appendbytes
      mergeSortAndEqualSum(metricAdjustmentHolder);
    }

  }

  private void mergeSortAndEqualSum(OnheapIncrementalIndex.MetricAdjustmentHolder metricAdjustmentHolder)
  {
    final int[] appendBytes = metricAdjustmentHolder.getMetricTypeAppendingBytes();
    final int[] rollupRows = metricAdjustmentHolder.getMetricTypeRollupRows();
    if (appendBytesAll == null) {
      appendBytesAll = appendBytes;
      rollupRowsAll = rollupRows;
    } else {
      int i = 0, j = 0, k = 0;
      int[] tempAppendBytesAll = new int[rollupRowsAll.length + rollupRows.length];
      int[] tempRollupRowsAll = new int[rollupRowsAll.length + rollupRows.length];
      while (i < rollupRowsAll.length && j < rollupRows.length) {
        if (rollupRowsAll[i] == rollupRows[j]) {
          tempRollupRowsAll[k] = rollupRows[j];
          tempAppendBytesAll[k] = appendBytesAll[i++] + appendBytes[j++];
        } else if (rollupRowsAll[i] > rollupRows[j]) {
          tempRollupRowsAll[k] = rollupRows[j];
          tempAppendBytesAll[k] = appendBytes[j++];
        } else {
          tempRollupRowsAll[k] = rollupRowsAll[i];
          tempAppendBytesAll[k] = appendBytesAll[i++];
        }
        k++;
      }
      while (i < rollupRowsAll.length) {
        tempRollupRowsAll[k] = rollupRowsAll[i];
        tempAppendBytesAll[k++] = appendBytesAll[i++];
      }
      while (j < rollupRows.length) {
        tempRollupRowsAll[k] = rollupRows[j];
        tempAppendBytesAll[k++] = appendBytes[j++];
      }

      if (k < tempAppendBytesAll.length) {
        appendBytesAll = new int[k];
        rollupRowsAll = new int[k];
        System.arraycopy(tempAppendBytesAll, 0, appendBytesAll, 0, k);
        System.arraycopy(tempRollupRowsAll, 0, rollupRowsAll, 0, k);
      }
    }
  }

  public int[] getAppendingBytes()
  {
    return appendBytesAll;
  }

  public int[] getRollupRows()
  {
    return rollupRowsAll;
  }
}
