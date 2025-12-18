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

package org.apache.druid.segment.incremental;

/**
 * Reasons why an input row may be thrown away during ingestion.
 */
public enum ThrownAwayReason
{
  /**
   * The row was null or the input record was empty.
   */
  NULL,

  /**
   * The row's timestamp is before the minimum message time (late message rejection).
   */
  BEFORE_MIN_MESSAGE_TIME,

  /**
   * The row's timestamp is after the maximum message time (early message rejection).
   */
  AFTER_MAX_MESSAGE_TIME,

  /**
   * The row was filtered out by a transformSpec filter or other row filter.
   */
  FILTERED;

  /**
   * Pre-computed metric dimension values, indexed by ordinal.
   */
  private static final String[] METRIC_VALUES = {
      "null",
      "beforeMinMessageTime",
      "afterMaxMessageTime",
      "filtered"
  };

  /**
   * Returns the value to be used as the dimension value in metrics.
   */
  public String getMetricValue()
  {
    return METRIC_VALUES[ordinal()];
  }
}
