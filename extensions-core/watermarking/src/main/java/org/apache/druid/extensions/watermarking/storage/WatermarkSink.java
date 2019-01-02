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

import org.joda.time.DateTime;

/**
 * WatermarkSink describes an interface for updating timeline watermark events associated with a
 * druid datasource
 */
public interface WatermarkSink
{
  /**
   * Update a timeline watermark event for a datasource
   *
   * @param datasource druid datasource name
   * @param type       type of watermark
   * @param timestamp  timestamp of the timeline watermark event
   */
  void update(String datasource, String type, DateTime timestamp);

  /**
   * Rollback a timeline watermark event for a datasource
   *
   * @param datasource druid datasource name
   * @param type       type of watermark
   * @param timestamp  timestamp of the timeline watermark event
   */
  void rollback(String datasource, String type, DateTime timestamp);

  /**
   * Delete all watermarks collected before the given timestamp
   *
   * @param datasource
   * @param type
   * @param timestamp
   */
  void purgeHistory(String datasource, String type, DateTime timestamp);

  /**
   * Perform any initialization required for the WatermarkSink to function correctly
   */
  void initialize();
}
