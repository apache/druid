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

package io.druid.indexing.common.stats;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;

import java.util.Map;

public class RowIngestionMeters
{
  public static final String PROCESSED = "processed";
  public static final String PROCESSED_WITH_ERROR = "processedWithError";
  public static final String UNPARSEABLE = "unparseable";
  public static final String THROWN_AWAY = "thrownAway";

  private final MetricRegistry metricRegistry;
  private final Meter processed;
  private final Meter processedWithError;
  private final Meter unparseable;
  private final Meter thrownAway;

  public RowIngestionMeters()
  {
    this.metricRegistry = new MetricRegistry();
    this.processed = metricRegistry.meter(PROCESSED);
    this.processedWithError = metricRegistry.meter(PROCESSED_WITH_ERROR);
    this.unparseable = metricRegistry.meter(UNPARSEABLE);
    this.thrownAway = metricRegistry.meter(THROWN_AWAY);
  }

  public Meter getProcessed()
  {
    return processed;
  }

  public Meter getProcessedWithError()
  {
    return processedWithError;
  }

  public Meter getUnparseable()
  {
    return unparseable;
  }

  public Meter getThrownAway()
  {
    return thrownAway;
  }

  public Map<String, Object> getTotals()
  {
    Map<String, Object> totals = Maps.newHashMap();
    totals.put(PROCESSED, processed.getCount());
    totals.put(PROCESSED_WITH_ERROR, processedWithError.getCount());
    totals.put(UNPARSEABLE, unparseable.getCount());
    totals.put(THROWN_AWAY, thrownAway.getCount());
    return totals;
  }

  public Map<String, Object> getMovingAverages()
  {
    Map<String, Object> movingAverages = Maps.newHashMap();

    Map<String, Object> oneMinute = Maps.newHashMap();
    oneMinute.put(PROCESSED, processed.getOneMinuteRate());
    oneMinute.put(PROCESSED_WITH_ERROR, processedWithError.getOneMinuteRate());
    oneMinute.put(UNPARSEABLE, unparseable.getOneMinuteRate());
    oneMinute.put(THROWN_AWAY, thrownAway.getOneMinuteRate());

    Map<String, Object> fiveMinute = Maps.newHashMap();
    fiveMinute.put(PROCESSED, processed.getFiveMinuteRate());
    fiveMinute.put(PROCESSED_WITH_ERROR, processedWithError.getFiveMinuteRate());
    fiveMinute.put(UNPARSEABLE, unparseable.getFiveMinuteRate());
    fiveMinute.put(THROWN_AWAY, thrownAway.getFiveMinuteRate());

    Map<String, Object> fifteenMinute = Maps.newHashMap();
    fifteenMinute.put(PROCESSED, processed.getFifteenMinuteRate());
    fifteenMinute.put(PROCESSED_WITH_ERROR, processedWithError.getFifteenMinuteRate());
    fifteenMinute.put(UNPARSEABLE, unparseable.getFifteenMinuteRate());
    fifteenMinute.put(THROWN_AWAY, thrownAway.getFifteenMinuteRate());

    movingAverages.put("1m", oneMinute);
    movingAverages.put("5m", fiveMinute);
    movingAverages.put("15m", fifteenMinute);

    return movingAverages;
  }
}
