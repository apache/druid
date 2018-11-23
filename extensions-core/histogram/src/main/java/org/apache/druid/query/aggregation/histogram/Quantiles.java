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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Arrays;

@JsonTypeName("quantiles")
public class Quantiles
{
  float[] probabilities;
  float[] quantiles;
  float min;
  float max;

  @JsonCreator
  public Quantiles(
      @JsonProperty("probabilities") float[] probabilities,
      @JsonProperty("quantiles") float[] quantiles,
      @JsonProperty("min") float min,
      @JsonProperty("max") float max
  )
  {
    this.probabilities = probabilities;
    this.quantiles = quantiles;
    this.min = min;
    this.max = max;
  }

  @JsonProperty
  public float[] getProbabilities()
  {
    return probabilities;
  }

  @JsonProperty
  public float[] getQuantiles()
  {
    return quantiles;
  }

  @JsonProperty
  public float getMin()
  {
    return min;
  }

  @JsonProperty
  public float getMax()
  {
    return max;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Quantiles quantiles1 = (Quantiles) o;

    if (Float.compare(quantiles1.max, max) != 0) {
      return false;
    }
    if (Float.compare(quantiles1.min, min) != 0) {
      return false;
    }
    if (!Arrays.equals(probabilities, quantiles1.probabilities)) {
      return false;
    }
    if (!Arrays.equals(quantiles, quantiles1.quantiles)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = probabilities != null ? Arrays.hashCode(probabilities) : 0;
    result = 31 * result + (quantiles != null ? Arrays.hashCode(quantiles) : 0);
    result = 31 * result + (min != +0.0f ? Float.floatToIntBits(min) : 0);
    result = 31 * result + (max != +0.0f ? Float.floatToIntBits(max) : 0);
    return result;
  }
}
