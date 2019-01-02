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

package org.apache.druid.extensions.watermarking.storage;

import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * WatermarkSource describes an interface for retrieving timeline watermark events associated
 * with a druid datasource
 */
public interface WatermarkSource
{
  /**
   * Get most recent DateTime for a specific type of timeline watermark event for a druid datasource.
   *
   * @param datasource druid datasource name
   * @param type       type of watermark
   *
   * @return DateTime of most recent timestamp for requested timeline watermark event
   */
  @Nullable
  DateTime getValue(String datasource, String type);

  /**
   * Get most recent DateTime for each type of timeline watermark events for a druid datasource.
   *
   * @param datasource druid datasource name
   *
   * @return all timeline watermarks
   */
  @Nullable
  Map<String, DateTime> getValues(String datasource);

  /**
   * Fetch a list of potential datasources
   *
   * @return A list of datasources
   */
  @Nullable
  Collection<String> getDatasources();

  /**
   * Get an ordered list of event timestamps and insertion timestamps for a type of timeline watermark event,
   * returned in DESCENDING ORDER (by event timestamp, not insertion time)
   *
   * @param datasource druid datasource name
   * @param type       type of watermark
   * @param range      interval to fetch history
   *
   * @return List of pairs of DateTimes, where 'lhs' is the event timestamp, and 'rhs' is the insertion timestamp
   */
  @Nullable
  List<Pair<DateTime, DateTime>> getValueHistory(String datasource, String type, Interval range);

  /**
   * Perform any initialization required for the WatermarkSource to function correctly
   */
  void initialize();
}
