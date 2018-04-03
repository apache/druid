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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FireDepartmentMetricsTaskMetricsGetter implements TaskMetricsGetter
{
  public static final List<String> KEYS = Arrays.asList(
      TaskMetricsUtils.ROWS_PROCESSED,
      TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS,
      TaskMetricsUtils.ROWS_THROWN_AWAY,
      TaskMetricsUtils.ROWS_UNPARSEABLE
  );

  private final FireDepartmentMetrics fireDepartmentMetrics;

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
  public Map<String, Number> getTotalMetrics()
  {
    return ImmutableMap.of(
        TaskMetricsUtils.ROWS_PROCESSED, fireDepartmentMetrics.processed(),
        TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, fireDepartmentMetrics.processedWithErrors(),
        TaskMetricsUtils.ROWS_THROWN_AWAY, fireDepartmentMetrics.thrownAway(),
        TaskMetricsUtils.ROWS_UNPARSEABLE, fireDepartmentMetrics.unparseable()
    );
  }
}
