/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.histogram;

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
