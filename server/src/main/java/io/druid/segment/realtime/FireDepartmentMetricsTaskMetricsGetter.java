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

package io.druid.segment.realtime;

import com.google.common.collect.ImmutableMap;
import io.druid.indexer.TaskMetricsGetter;
import io.druid.indexer.TaskMetricsUtils;
import io.druid.java.util.common.logger.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FireDepartmentMetricsTaskMetricsGetter implements TaskMetricsGetter
{
  public static final List<String> KEYS = Arrays.asList(
      TaskMetricsUtils.ROWS_PROCESSED,
      TaskMetricsUtils.ROWS_THROWN_AWAY,
      TaskMetricsUtils.ROWS_UNPARSEABLE
  );

  private static final Logger log = new Logger(FireDepartmentMetricsTaskMetricsGetter.class);

  private final FireDepartmentMetrics fireDepartmentMetrics;

  private double processed = 0;
  private double thrownAway = 0;
  private double unparseable = 0;

  public FireDepartmentMetricsTaskMetricsGetter(
      FireDepartmentMetrics fireDepartmentMetrics
  )
  {
    this.fireDepartmentMetrics = fireDepartmentMetrics;
  }

  @Override
  public List<String> getKeys()
  {
    return KEYS;
  }

  @Override
  public Map<String, Double> getMetrics()
  {
    double curProcessed = fireDepartmentMetrics.processed();
    double curThrownAway = fireDepartmentMetrics.thrownAway();
    double curUnparseable = fireDepartmentMetrics.unparseable();

    double processedDiff = curProcessed - processed;
    double thrownAwayDiff = curThrownAway - thrownAway;
    double unparseableDiff = curUnparseable - unparseable;

    processed = curProcessed;
    thrownAway = curThrownAway;
    unparseable = curUnparseable;
    
    return ImmutableMap.of(
        TaskMetricsUtils.ROWS_PROCESSED, processedDiff,
        TaskMetricsUtils.ROWS_THROWN_AWAY, thrownAwayDiff,
        TaskMetricsUtils.ROWS_UNPARSEABLE, unparseableDiff
    );
  }
}
