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

public enum MetricType
{
  count(DimensionMapNames.COUNTER),
  gauge(DimensionMapNames.GAUGE),
  timer(DimensionMapNames.TIMER),
  histogram(DimensionMapNames.HISTOGRAM),
  summary(DimensionMapNames.SUMMARY);

  private final String dimensionMapName;

  MetricType(String dimensionMapName)
  {
    this.dimensionMapName = dimensionMapName;
  }

  public String getDimensionMapName()
  {
    return dimensionMapName;
  }

  // TODO: get rid of this inner class after upgrading jackson in parent
  //       ref: https://github.com/FasterXML/jackson-databind/issues/2739
  static class DimensionMapNames
  {
    static final String GAUGE = "gauge";
    static final String COUNTER = "count";
    static final String TIMER = "timer";
    static final String HISTOGRAM = "histogram";
    static final String SUMMARY = "summary";
  }
}
