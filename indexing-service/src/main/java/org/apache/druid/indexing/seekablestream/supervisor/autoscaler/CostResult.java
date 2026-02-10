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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

/**
 * Holds the result of a cost computation from {@link WeightedCostFunction#computeCost}.
 * All costs are measured in seconds.
 */
public class CostResult
{

  private final double totalCost;
  private final double lagCost;
  private final double idleCost;

  public CostResult()
  {
    this(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
  }

  /**
   * @param totalCost the weighted sum of lagCost and idleCost
   * @param lagCost   the weighted cost representing expected time (seconds) to recover current lag
   * @param idleCost  the weighted cost representing total compute time (seconds) wasted being idle per task duration
   */
  public CostResult(double totalCost, double lagCost, double idleCost)
  {
    this.totalCost = totalCost;
    this.lagCost = lagCost;
    this.idleCost = idleCost;
  }

  public double totalCost()
  {
    return totalCost;
  }

  public double lagCost()
  {
    return lagCost;
  }

  public double idleCost()
  {
    return idleCost;
  }
}
