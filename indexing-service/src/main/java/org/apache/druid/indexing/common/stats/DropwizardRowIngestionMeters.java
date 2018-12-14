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

package org.apache.druid.indexing.common.stats;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.HashMap;
import java.util.Map;

public class DropwizardRowIngestionMeters implements RowIngestionMeters
{
  public static final String ONE_MINUTE_NAME = "1m";
  public static final String FIVE_MINUTE_NAME = "5m";
  public static final String FIFTEEN_MINUTE_NAME = "15m";

  private final MetricRegistry metricRegistry;
  private final Meter processed;
  private final Meter processedWithError;
  private final Meter unparseable;
  private final Meter thrownAway;

  public DropwizardRowIngestionMeters()
  {
    this.metricRegistry = new MetricRegistry();
    this.processed = metricRegistry.meter(PROCESSED);
    this.processedWithError = metricRegistry.meter(PROCESSED_WITH_ERROR);
    this.unparseable = metricRegistry.meter(UNPARSEABLE);
    this.thrownAway = metricRegistry.meter(THROWN_AWAY);
  }

  @Override
  public long getProcessed()
  {
    return processed.getCount();
  }

  @Override
  public void incrementProcessed()
  {
    processed.mark();
  }

  @Override
  public long getProcessedWithError()
  {
    return processedWithError.getCount();
  }

  @Override
  public void incrementProcessedWithError()
  {
    processedWithError.mark();
  }

  @Override
  public long getUnparseable()
  {
    return unparseable.getCount();
  }

  @Override
  public void incrementUnparseable()
  {
    unparseable.mark();
  }

  @Override
  public long getThrownAway()
  {
    return thrownAway.getCount();
  }

  @Override
  public void incrementThrownAway()
  {
    thrownAway.mark();
  }

  @Override
  public RowIngestionMetersTotals getTotals()
  {
    return new RowIngestionMetersTotals(
        processed.getCount(),
        processedWithError.getCount(),
        thrownAway.getCount(),
        unparseable.getCount()
    );
  }

  @Override
  public Map<String, Object> getMovingAverages()
  {
    Map<String, Object> movingAverages = new HashMap<>();

    Map<String, Object> oneMinute = new HashMap<>();
    oneMinute.put(PROCESSED, processed.getOneMinuteRate());
    oneMinute.put(PROCESSED_WITH_ERROR, processedWithError.getOneMinuteRate());
    oneMinute.put(UNPARSEABLE, unparseable.getOneMinuteRate());
    oneMinute.put(THROWN_AWAY, thrownAway.getOneMinuteRate());

    Map<String, Object> fiveMinute = new HashMap<>();
    fiveMinute.put(PROCESSED, processed.getFiveMinuteRate());
    fiveMinute.put(PROCESSED_WITH_ERROR, processedWithError.getFiveMinuteRate());
    fiveMinute.put(UNPARSEABLE, unparseable.getFiveMinuteRate());
    fiveMinute.put(THROWN_AWAY, thrownAway.getFiveMinuteRate());

    Map<String, Object> fifteenMinute = new HashMap<>();
    fifteenMinute.put(PROCESSED, processed.getFifteenMinuteRate());
    fifteenMinute.put(PROCESSED_WITH_ERROR, processedWithError.getFifteenMinuteRate());
    fifteenMinute.put(UNPARSEABLE, unparseable.getFifteenMinuteRate());
    fifteenMinute.put(THROWN_AWAY, thrownAway.getFifteenMinuteRate());

    movingAverages.put(ONE_MINUTE_NAME, oneMinute);
    movingAverages.put(FIVE_MINUTE_NAME, fiveMinute);
    movingAverages.put(FIFTEEN_MINUTE_NAME, fifteenMinute);

    return movingAverages;
  }
}
