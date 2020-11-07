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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.Util;
import org.apache.datasketches.theta.SetOperation;
import org.apache.druid.query.aggregation.MaxIntermediateSizeAdjustStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ThetaSketch adjust strategy is appending bytes depending on cardinality number.
 * for example when size=1024
 * <p>
 * current cardinality
 * (need exec adjust)    actual occupy bytes  -   adjust before bytes  =  append bytes
 * 1                   256                      16416                -16160
 * 16                  2052                       256                1796
 * 128                 16416                      2052               14364
 */
public class ThetaSketchSizeAdjustStrategy extends MaxIntermediateSizeAdjustStrategy<Integer>
{
  // example size=2048,X8： rollupCardinalNums 0-32 -->856byte (JOL)  33-256->4440byte (JOL)  257...->33112byte(JOL)
  private final int size;
  private final int[] rollupNums;
  // when rollup cardinal num is equals rollupNums[i] appending bytes
  private final int[] adjustBytes;
  private final int initAggAppendBytes;

  public ThetaSketchSizeAdjustStrategy(final int tempSize)
  {
    this.size = Util.ceilingPowerOf2(tempSize);
    List<Integer> list = new ArrayList<>();
    int lgSizeNums = Integer.numberOfTrailingZeros(this.size);
    int lgNomLongs = Integer.numberOfTrailingZeros(this.size);
    final int arrlongs = Util.startingSubMultiple(lgNomLongs + 1, ResizeFactor.X8, Util.MIN_LG_ARR_LONGS);
    int rollupNum = (int) Math.pow(2, arrlongs - 1);
    while (true) {
      lgSizeNums -= 3;
      if (lgSizeNums < Util.MIN_LG_NOM_LONGS) {
        break;
      }
      list.add(rollupNum);
      rollupNum *= 8;
    }

    int maxBytes = SetOperation.getMaxUnionBytes(this.size);
    initAggAppendBytes = maxBytes / (int) Math.pow(8, list.size()) - maxBytes;
    rollupNums = new int[list.size()];
    adjustBytes = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      rollupNums[i] = list.get(i);
      adjustBytes[i] = SetOperation.getMaxUnionBytes(this.size) / (int) Math.pow(8, list.size() - i);
      if (i > 0) {
        int deltaBytes;
        deltaBytes = adjustBytes[i] - adjustBytes[i - 1];
        adjustBytes[i - 1] = deltaBytes;
      }
    }
    if (list.size() > 0) {
      adjustBytes[list.size() - 1] = SetOperation.getMaxUnionBytes(this.size) - adjustBytes[list.size() - 1];
    }
  }

  @Override
  public AdjustmentType getAdjustmentType()
  {
    return AdjustmentType.MERGE;
  }

  @Override
  public String getAdjustmentMetricType()
  {
    return SketchModule.THETA_SKETCH;
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
    return size;
  }

  @Override
  public int compareTo(MaxIntermediateSizeAdjustStrategy<Integer> other)
  {
    if (other.getInputVal() == 0) {
      return -1;
    }
    return Integer.compare(size, other.getInputVal());
  }

  @Override
  public String toString()
  {
    return "ThetaSketchSizeAdjustStrategy{" +
        "size=" + size +
        ", rollupNums=" + Arrays.toString(rollupNums) +
        ", adjustBytes=" + Arrays.toString(adjustBytes) +
        ", initAggAppendBytes=" + initAggAppendBytes +
        '}';
  }
}
