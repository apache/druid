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

package org.apache.druid.indexer;

import java.util.HashMap;
import java.util.Map;

public class TaskMetricsUtils
{
  public static final String ROWS_PROCESSED = "rowsProcessed";
  public static final String ROWS_PROCESSED_WITH_ERRORS = "rowsProcessedWithErrors";
  public static final String ROWS_UNPARSEABLE = "rowsUnparseable";
  public static final String ROWS_THROWN_AWAY = "rowsThrownAway";

  public static Map<String, Object> makeIngestionRowMetrics(
      long rowsProcessed,
      long rowsProcessedWithErrors,
      long rowsUnparseable,
      long rowsThrownAway
  )
  {
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put(ROWS_PROCESSED, rowsProcessed);
    metricsMap.put(ROWS_PROCESSED_WITH_ERRORS, rowsProcessedWithErrors);
    metricsMap.put(ROWS_UNPARSEABLE, rowsUnparseable);
    metricsMap.put(ROWS_THROWN_AWAY, rowsThrownAway);
    return metricsMap;
  }
}
