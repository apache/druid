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
 * Lag cost is based on recovery time; idle cost is based on the weighted idle-ratio penalty.
 */
public class CostResult
{
  static final CostResult INFINITE_COST = new CostResult(
      Double.POSITIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.POSITIVE_INFINITY
  );

  private final double totalCost;
  private final double lagCost;
  private final double idleCost;

  /**
   * @param totalCost the weighted sum of lagCost and idleCost
   * @param lagCost   the weighted cost representing expected time (seconds) to recover current lag
   * @param idleCost  the weighted cost representing the predicted idle-ratio penalty
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
