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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricAdjustmentHolder
{
  private MaxIntermediateSizeAdjustStrategy metricTypeAdjustmentStrategy;
  private int metricCount = 0;
  private int[] appendBytesAll;
  private int[] rollupRowsAll;
  private HashMap<Object, MaxIntermediateSizeAdjustStrategy> inputAndStrategyMap = new HashMap<>();
  private HashMap<Object, AtomicInteger> inputAndCountMap = new HashMap<>();

  public MetricAdjustmentHolder(MaxIntermediateSizeAdjustStrategy metricTypeAdjustmentStrategy)
  {
    this.metricTypeAdjustmentStrategy = metricTypeAdjustmentStrategy;
  }

  public void selectStrategyByType(MaxIntermediateSizeAdjustStrategy metricTypeAdjustmentStrategy)
  {
    switch (this.metricTypeAdjustmentStrategy.getAdjustmentType()) {
      case MAX:
        if (this.metricTypeAdjustmentStrategy.compareTo(metricTypeAdjustmentStrategy) < 0) {
          this.metricTypeAdjustmentStrategy = metricTypeAdjustmentStrategy;
        }
        ++metricCount;
        break;
      case MERGE:
        inputAndStrategyMap.put(metricTypeAdjustmentStrategy.getInputVal(), metricTypeAdjustmentStrategy);
        final AtomicInteger atomicInteger = inputAndCountMap.computeIfAbsent(
            metricTypeAdjustmentStrategy.getInputVal(),
            k -> new AtomicInteger(0)
        );
        atomicInteger.incrementAndGet();
        break;
      default:
    }
  }

  public void computeAppendingInfo()
  {
    switch (this.metricTypeAdjustmentStrategy.getAdjustmentType()) {
      case MAX:
        appendBytesAll = Arrays.stream(metricTypeAdjustmentStrategy.appendBytesOnRollupNum())
            .map(appendingByte -> appendingByte * metricCount)
            .toArray();
        break;
      case MERGE:
        final Iterator<Map.Entry<Object, MaxIntermediateSizeAdjustStrategy>> iterator = inputAndStrategyMap.entrySet()
            .iterator();
        while (iterator.hasNext()) {
          final Map.Entry<Object, MaxIntermediateSizeAdjustStrategy> next = iterator.next();
          final Object inputVal = next.getKey();
          final MaxIntermediateSizeAdjustStrategy strategy = next.getValue();
          mergeSortAndEqualSum(strategy, inputAndCountMap.get(inputVal).get());
        }
        break;
      default:
    }
  }

  public int[] getMetricTypeAppendingBytes()
  {
    if (this.metricTypeAdjustmentStrategy.getAdjustmentType() == MaxIntermediateSizeAdjustStrategy.AdjustmentType.MAX) {
      appendBytesAll = Arrays.stream(metricTypeAdjustmentStrategy.appendBytesOnRollupNum())
          .map(appendingByte -> appendingByte * metricCount)
          .toArray();
    }
    return appendBytesAll;
  }

  public int[] getMetricTypeRollupRows()
  {
    if (this.metricTypeAdjustmentStrategy.getAdjustmentType() == MaxIntermediateSizeAdjustStrategy.AdjustmentType.MAX) {
      rollupRowsAll = metricTypeAdjustmentStrategy.adjustWithRollupNum();
    }
    return rollupRowsAll;
  }

  private void mergeSortAndEqualSum(MaxIntermediateSizeAdjustStrategy metricTypeAdjustmentStrategy, int count)
  {
    int[] appendBytes = Arrays.stream(metricTypeAdjustmentStrategy.appendBytesOnRollupNum())
        .map(appendBt -> appendBt * count)
        .toArray();
    int[] rollupRows = metricTypeAdjustmentStrategy.adjustWithRollupNum();
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

      if (k <= tempAppendBytesAll.length) {
        appendBytesAll = new int[k];
        rollupRowsAll = new int[k];
        System.arraycopy(tempAppendBytesAll, 0, appendBytesAll, 0, k);
        System.arraycopy(tempRollupRowsAll, 0, rollupRowsAll, 0, k);
      }
    }
  }
}
