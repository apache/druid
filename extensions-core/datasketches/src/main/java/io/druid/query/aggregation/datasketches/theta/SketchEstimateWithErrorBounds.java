/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Container class used to return estimates in conjunction with 
 * estimated error bounds.
 */
public class SketchEstimateWithErrorBounds
{
  final private double estimate;
  final private double highBound;
  final private double lowBound;
  final private int numStdDev;
  
  @JsonCreator
  public SketchEstimateWithErrorBounds(
      @JsonProperty("estimate") double estimate,
      @JsonProperty("highBound") double highBound,
      @JsonProperty("lowBound") double lowBound,
      @JsonProperty("numStdDev") int numStdDev
  )
  {
    this.estimate = estimate;
    this.highBound = highBound;
    this.lowBound = lowBound;
    this.numStdDev = numStdDev;
  }
  
  @JsonProperty
  public double getEstimate()
  {
    return estimate;
  }

  @JsonProperty
  public double getHighBound()
  {
    return highBound;
  }

  @JsonProperty
  public double getLowBound()
  {
    return lowBound;
  }

  @JsonProperty
  public int getNumStdDev()
  {
    return numStdDev;
  }

  @Override
  public String toString()
  {
    return "SketchEstimateWithErrorBounds{" +
        "estimate=" + Double.toString(estimate) +
        ", highBound=" + Double.toString(highBound) +
        ", lowBound="+ Double.toString(lowBound) +
        ", numStdDev=" + Integer.toString(numStdDev) +
        "}";
  }

  @Override
  public int hashCode()
  {
    int result = Double.valueOf(estimate).hashCode();
    result = 31 * result + Double.valueOf(highBound).hashCode();
    result = 31 * result + Double.valueOf(lowBound).hashCode();
    result = 31 * result + Integer.valueOf(numStdDev).hashCode();

    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    } 
    
    SketchEstimateWithErrorBounds that = (SketchEstimateWithErrorBounds) obj;
    if (estimate != that.estimate ||
        highBound != that.highBound ||
        lowBound != that.lowBound ||
        numStdDev != that.numStdDev) {
      return false;
    }
    return true;
  }
}
