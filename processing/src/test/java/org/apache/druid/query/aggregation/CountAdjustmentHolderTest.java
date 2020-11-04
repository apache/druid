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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class CountAdjustmentHolderTest
{
  @Test
  public void testMergeSort()
  {
    HashMap<String, MetricAdjustmentHolder> metricTypeAndHolderMap = new HashMap<>();
    final MaxIntermediateSizeAdjustStrategy strategyMax1 = new MaxIntermediateSizeAdjustStrategy()
    {
      @Override
      public AdjustmentType getAdjustmentType()
      {
        return AdjustmentType.MAX;
      }

      @Override
      public String getAdjustmentMetricType()
      {
        return "custom01";
      }

      @Override
      public int[] adjustWithRollupNum()
      {
        return new int[]{2, 4, 8, 32};
      }

      @Override
      public int[] appendBytesOnRollupNum()
      {
        return new int[]{100, 200, 400, 800};
      }

      @Override
      public int initAppendBytes()
      {
        return 0;
      }

      @Override
      public Object getInputVal()
      {
        return null;
      }

      @Override
      public int compareTo(Object o)
      {
        return 0;
      }
    };

    final MaxIntermediateSizeAdjustStrategy strategyMax2 = new MaxIntermediateSizeAdjustStrategy()
    {
      @Override
      public AdjustmentType getAdjustmentType()
      {
        return AdjustmentType.MAX;
      }

      @Override
      public String getAdjustmentMetricType()
      {
        return "custom01";
      }

      @Override
      public int[] adjustWithRollupNum()
      {
        return new int[]{2, 4, 8, 32};
      }

      @Override
      public int[] appendBytesOnRollupNum()
      {
        return new int[]{100, 200, 400, 800};
      }

      @Override
      public int initAppendBytes()
      {
        return 0;
      }

      @Override
      public Object getInputVal()
      {
        return null;
      }

      @Override
      public int compareTo(Object o)
      {
        return 0;
      }
    };

    final MaxIntermediateSizeAdjustStrategy strategyMerge1 = new MaxIntermediateSizeAdjustStrategy()
    {
      @Override
      public AdjustmentType getAdjustmentType()
      {
        return AdjustmentType.MERGE;
      }

      @Override
      public String getAdjustmentMetricType()
      {
        return "custom02";
      }

      @Override
      public int[] adjustWithRollupNum()
      {
        return new int[]{4, 8, 16};
      }

      @Override
      public int[] appendBytesOnRollupNum()
      {
        return new int[]{100, 200, 700};
      }

      @Override
      public int initAppendBytes()
      {
        return 0;
      }

      @Override
      public Object getInputVal()
      {
        return null;
      }

      @Override
      public int compareTo(Object o)
      {
        return 0;
      }
    };

    final MaxIntermediateSizeAdjustStrategy strategyMerge2 = new MaxIntermediateSizeAdjustStrategy()
    {
      @Override
      public AdjustmentType getAdjustmentType()
      {
        return AdjustmentType.MERGE;
      }

      @Override
      public String getAdjustmentMetricType()
      {
        return "custom02";
      }

      @Override
      public int[] adjustWithRollupNum()
      {
        return new int[]{4, 8, 16};
      }

      @Override
      public int[] appendBytesOnRollupNum()
      {
        return new int[]{100, 200, 700};
      }

      @Override
      public int initAppendBytes()
      {
        return 0;
      }

      @Override
      public Object getInputVal()
      {
        return null;
      }

      @Override
      public int compareTo(Object o)
      {
        return 0;
      }
    };

    final MetricAdjustmentHolder metricAdjustmentHolder1 = metricTypeAndHolderMap.computeIfAbsent(
        strategyMax1.getAdjustmentMetricType(),
        k -> new MetricAdjustmentHolder(strategyMax1)
    );
    metricAdjustmentHolder1.selectStrategyByType(strategyMax1);
    final MetricAdjustmentHolder metricAdjustmentHolder2 = metricTypeAndHolderMap.computeIfAbsent(
        strategyMax2.getAdjustmentMetricType(),
        k -> new MetricAdjustmentHolder(strategyMax2)
    );
    metricAdjustmentHolder2.selectStrategyByType(strategyMax2);

    final MetricAdjustmentHolder metricAdjustmentHolder3 = metricTypeAndHolderMap.computeIfAbsent(
        strategyMerge1.getAdjustmentMetricType(),
        k -> new MetricAdjustmentHolder(strategyMerge1)
    );
    metricAdjustmentHolder3.selectStrategyByType(strategyMerge1);
    final MetricAdjustmentHolder metricAdjustmentHolder4 = metricTypeAndHolderMap.computeIfAbsent(
        strategyMerge2.getAdjustmentMetricType(),
        k -> new MetricAdjustmentHolder(strategyMerge2)
    );
    metricAdjustmentHolder4.selectStrategyByType(strategyMerge2);

    int[] expectedRollupNums = {2, 4, 8, 16, 32};
    int[] expectedAppendBytes = {100 * 2, 300 * 2, 600 * 2, 700 * 2, 800 * 2};

    CountAdjustmentHolder countAdjustmentHolder = new CountAdjustmentHolder(metricTypeAndHolderMap);
    final int[] rollupRows = countAdjustmentHolder.getRollupRows();
    final int[] appendingBytes = countAdjustmentHolder.getAppendingBytes();
    Assert.assertArrayEquals(expectedRollupNums, rollupRows);
    Assert.assertArrayEquals(expectedAppendBytes, appendingBytes);

  }
}
