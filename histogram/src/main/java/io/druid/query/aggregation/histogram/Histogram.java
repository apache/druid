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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class Histogram
{
  double[] breaks;
  double[] counts;

  public Histogram(float[] breaks, double[] counts)
  {
    double[] retVal = new double[breaks.length];
    for (int i = 0; i < breaks.length; ++i) {
      retVal[i] = (double) breaks[i];
    }

    this.breaks = retVal;
    this.counts = counts;
  }

  @JsonProperty
  public double[] getBreaks()
  {
    return breaks;
  }

  @JsonProperty
  public double[] getCounts()
  {
    return counts;
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

    Histogram that = (Histogram) o;

    if (!Arrays.equals(this.getBreaks(), that.getBreaks())) {
      return false;
    }
    if (!Arrays.equals(this.getCounts(), that.getCounts())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (this.getBreaks() != null ? ArrayUtils.hashCode(this.getBreaks(), 0, this.getBreaks().length) : 0);
    result = 31 * result + (this.getCounts() != null ? ArrayUtils.hashCode(
        this.getCounts(),
        0,
        this.getCounts().length
    ) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Histogram{" +
           "breaks=" + Arrays.toString(breaks) +
           ", counts=" + Arrays.toString(counts) +
           '}';
  }
}
