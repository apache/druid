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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Arrays;

public class HistogramVisual
{
  @JsonProperty public final double[] breaks;
  @JsonProperty
  public final double[] counts;
  // an array of the quantiles including the min. and max.
  @JsonProperty public final double[] quantiles;

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
    for (int i = 0; i < breaks.length; ++i) {
      this.breaks[i] = breaks[i];
    }
    for (int i = 0; i < counts.length; ++i) {
      this.counts[i] = counts[i];
    }
    for (int i = 0; i < quantiles.length; ++i) {
      this.quantiles[i] = quantiles[i];
    }
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
