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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;

import java.util.Arrays;

public class DoublesSketchSizeAdjustStrategy extends MaxIntermediateSizeAdjustStrategy<Integer>
{
  private static final int N_NUM = 28;
  private static final int K_NUM = 12;
  private static final int XN = 2;
  private static final int START_N = 8;
  private static final int START_K = 16;
  private static final int INIT_SIZE = 96;
  private static int[][] knSize = new int[K_NUM][N_NUM];
  private final int k;

  private final int[] rollupNums = new int[N_NUM];
  // when rollup cardinal num is equals rollupNums[i] appending bytes
  private final int[] adjustBytes = new int[N_NUM];
  private final int initAggAppendBytes;

  public DoublesSketchSizeAdjustStrategy(int k)
  {
    this.k = k;

    boolean copyFlag = false;
    for (int ki = START_K, i = 0; i < K_NUM; ki *= 2, i++) {
      for (int nj = START_N, j = 0; j < N_NUM; nj *= XN, j++) {
        knSize[i][j] = DoublesSketch.getUpdatableStorageBytes(ki, nj - 1);
        if (copyFlag == false) {
          rollupNums[j] = nj - 1;
        }
      }
      copyFlag = true;
    }

    int kIndex = this.k / START_K == 0 ? 0 : Integer.numberOfTrailingZeros(this.k / START_K);
    initAggAppendBytes = -DoublesSketch.getUpdatableStorageBytes(k, DoublesSketchAggregatorFactory.MAX_STREAM_LENGTH) + INIT_SIZE;

    int[] tempBytes = new int[adjustBytes.length];
    System.arraycopy(knSize[kIndex], 0, tempBytes, 0, N_NUM);
    // compute appending bytes
    for (int i = 0; i < tempBytes.length; i++) {
      if (i == tempBytes.length - 1) {
        adjustBytes[i] = adjustBytes[i - 1];
      } else {
        adjustBytes[i] = tempBytes[i + 1] - tempBytes[i];
      }
    }
  }

  @Override
  public AdjustmentType getAdjustmentType()
  {
    return AdjustmentType.MAX;
  }

  @Override
  public String getAdjustmentMetricType()
  {
    return DoublesSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public int[] adjustWithRollupNum()
  {
    return rollupNums;
  }

  @Override
  public int[] appendBytesOnRollupNum()
  {
    return adjustBytes;
  }

  @Override
  public int initAppendBytes()
  {
    return initAggAppendBytes;
  }

  @Override
  public Integer getInputVal()
  {
    return k;
  }

  @Override
  public int compareTo(MaxIntermediateSizeAdjustStrategy<Integer> other)
  {
    if (other.getInputVal() == 0) {
      return -1;
    }
    return Integer.compare(k, other.getInputVal());
  }
  @Override
  public String toString()
  {
    return "DoublesSketchSizeAdjustStrategy{" +
        "k=" + k +
        ", rollupNums=" + Arrays.toString(rollupNums) +
        ", adjustBytes=" + Arrays.toString(adjustBytes) +
        ", initAggAppendBytes=" + initAggAppendBytes +
        '}';
  }
}
