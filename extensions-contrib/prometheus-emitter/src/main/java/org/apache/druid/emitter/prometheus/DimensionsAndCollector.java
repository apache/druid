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

package org.apache.druid.emitter.prometheus;

import io.prometheus.client.SimpleCollector;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DimensionsAndCollector
{
  private static final Logger log = new Logger(DimensionsAndCollector.class);
  private final String[] dimensions;
  private final SimpleCollector collector;
  private final double conversionFactor;
  private final double[] histogramBuckets;
  private final ConcurrentHashMap<List<String>, Stopwatch> labelValuesToStopwatch;
  private final Duration ttlSeconds;

  DimensionsAndCollector(String[] dimensions, SimpleCollector collector, double conversionFactor, double[] histogramBuckets, @Nullable Integer ttlSeconds)
  {
    this.dimensions = dimensions;
    this.collector = collector;
    this.conversionFactor = conversionFactor;
    this.histogramBuckets = histogramBuckets;
    this.labelValuesToStopwatch = new ConcurrentHashMap<>();
    this.ttlSeconds = ttlSeconds != null ? Duration.standardSeconds(ttlSeconds) : null;
  }

  public String[] getDimensions()
  {
    return dimensions;
  }

  public SimpleCollector getCollector()
  {
    return collector;
  }

  public double getConversionFactor()
  {
    return conversionFactor;
  }

  public double[] getHistogramBuckets()
  {
    return histogramBuckets;
  }

  public void resetLastUpdateTime(List<String> labelValues)
  {
    labelValuesToStopwatch.compute(labelValues, (k, v) -> {
      if (v != null) {
        v.restart();
        return v;
      } else {
        return Stopwatch.createStarted();
      }
    });
  }

  public ConcurrentMap<List<String>, Stopwatch> getLabelValuesToStopwatch()
  {
    return labelValuesToStopwatch;
  }

  public boolean removeIfExpired(List<String> labelValues)
  {
    if (ttlSeconds == null) {
      throw DruidException.defensive("Invalid usage of isExpired, flushPeriod has not been set");
    }
    return labelValuesToStopwatch.computeIfPresent(labelValues, (k, v) -> {
      if (v.hasElapsed(ttlSeconds)) {
        return null;
      }
      return v;
    }) == null;
  }
}
