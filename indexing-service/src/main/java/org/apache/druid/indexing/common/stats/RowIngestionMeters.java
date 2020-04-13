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

import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.Map;

/**
 * A collection of meters for row ingestion stats, with support for moving average calculations.
 * This can eventually replace FireDepartmentMetrics, but moving averages for other stats collected by
 * FireDepartmentMetrics are not currently supported, so we continue to use FireDepartmentMetrics alongside
 * RowIngestionMeters to avoid unnecessary overhead from maintaining these moving averages.
 */
@ExtensionPoint
public interface RowIngestionMeters
{
  String BUILD_SEGMENTS = "buildSegments";
  String DETERMINE_PARTITIONS = "determinePartitions";

  String PROCESSED = "processed";
  String PROCESSED_WITH_ERROR = "processedWithError";
  String UNPARSEABLE = "unparseable";
  String THROWN_AWAY = "thrownAway";

  long getProcessed();
  void incrementProcessed();

  long getProcessedWithError();
  void incrementProcessedWithError();

  long getUnparseable();
  void incrementUnparseable();

  long getThrownAway();
  void incrementThrownAway();

  RowIngestionMetersTotals getTotals();

  Map<String, Object> getMovingAverages();
}
