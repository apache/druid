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

package org.apache.druid.emitter.prometheus.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.SortedSet;

@Deprecated
@JsonTypeName(MetricType.DimensionMapNames.TIMER)
public class Timer extends Histogram
{
  private static final double[] BUCKETS = {.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300};
  private final double conversionFactor;

  public Timer(
      @JsonProperty("dimensions") SortedSet<String> dimensions,
      @JsonProperty("type") MetricType type,
      @JsonProperty("help") String help,
      @JsonProperty("conversionFactor") double conversionFactor
  )
  {
    super(dimensions, type, help, BUCKETS);
    this.conversionFactor = conversionFactor;
  }

  public double getConversionFactor()
  {
    return conversionFactor;
  }

  @Override
  public void record(String[] labelValues, double value)
  {
    this.getCollector().labels(labelValues).observe(value / this.conversionFactor);
  }
}
