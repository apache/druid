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

import org.apache.druid.guice.annotations.ExtensionPoint;

@ExtensionPoint
public abstract class MaxIntermediateSizeAdjustStrategy<Input> implements Comparable<MaxIntermediateSizeAdjustStrategy<Input>>
{
  public enum AdjustmentType
  {
    MAX,
    MERGE
  }

  public abstract AdjustmentType getAdjustmentType();

  public abstract String getAdjustmentMetricType();

  /**
   * Specify the number of aggregates in a list that need to be sorted from small to large
   *
   * @return
   */
  public abstract int[] adjustWithRollupNum();

  /**
   * When the specified number of aggregate bars is reached, AppenatorImpl#bytesCurrentlyInMemory
   * The size to be appended: bytesCurrentlyInMemory + appendBytesOnRollupNum.
   *
   * @return
   */
  public abstract int[] appendBytesOnRollupNum();

  /**
   * GetMaxIntermediateSize + initAppendBytes: The number of bytes required to create an Agg object (usually negative)
   *
   * @return
   */
  public abstract int initAppendBytes();

  /**
   * if input value is different in the same metric type,then select the adjustment strategy with the maximum input value
   *
   * @return
   */
  public abstract Input getInputVal();
}
