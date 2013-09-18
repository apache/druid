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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Arrays;

public class HistogramVisual
{
  @JsonProperty final public double[] breaks;
  @JsonProperty
  final public double[] counts;
  // an array of the quantiles including the min. and max.
  @JsonProperty final public double[] quantiles;

  @JsonCreator
  public HistogramVisual(
      @JsonProperty double[] breaks,
      @JsonProperty double[] counts,
      @JsonProperty double[] quantiles
  )
  {
    Preconditions.checkArgument(breaks != null, "breaks must not be null");
    Preconditions.checkArgument(counts != null, "counts must not be null");
    Preconditions.checkArgument(breaks.length == counts.length + 1, "breaks.length must be counts.length + 1");

    this.breaks = breaks;
    this.counts = counts;
    this.quantiles = quantiles;
  }

  public HistogramVisual(
        float[] breaks,
        float[] counts,
        float[] quantiles
  )
  {
    Preconditions.checkArgument(breaks != null, "breaks must not be null");
    Preconditions.checkArgument(counts != null, "counts must not be null");
    Preconditions.checkArgument(breaks.length == counts.length + 1, "breaks.length must be counts.length + 1");

    this.breaks = new double[breaks.length];
    this.counts = new double[counts.length];
    this.quantiles = new double[quantiles.length];
    for(int i = 0; i < breaks.length; ++i) this.breaks[i] = breaks[i];
    for(int i = 0; i < counts.length; ++i) this.counts[i] = counts[i];
    for(int i = 0; i < quantiles.length; ++i) this.quantiles[i] = quantiles[i];
  }

  @Override
  public String toString()
  {
    return "HistogramVisual{" +
           "counts=" + Arrays.toString(counts) +
           ", breaks=" + Arrays.toString(breaks) +
           ", quantiles=" + Arrays.toString(quantiles) +
           '}';
  }
}
