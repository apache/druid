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
import java.util.concurrent.atomic.AtomicLong;

public class DimensionsAndCollector
{
  private final String[] dimensions;
  private final SimpleCollector collector;
  private final double conversionFactor;
  private final double[] histogramBuckets;
  private final AtomicLong lastUpdateTime;

  DimensionsAndCollector(String[] dimensions, SimpleCollector collector, double conversionFactor, double[] histogramBuckets)
  {
    this.dimensions = dimensions;
    this.collector = collector;
    this.conversionFactor = conversionFactor;
    this.histogramBuckets = histogramBuckets;
    this.lastUpdateTime = new AtomicLong(System.currentTimeMillis());
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

  public void updateLastUpdateTime()
  {
    lastUpdateTime.set(System.currentTimeMillis());
  }

  public long getLastUpdateTime()
  {
    return lastUpdateTime.get();
  }

  public boolean isExpired(long ttlMillis)
  {
    long currentTime = System.currentTimeMillis();
    return (currentTime - lastUpdateTime.get()) > ttlMillis;
  }
}
