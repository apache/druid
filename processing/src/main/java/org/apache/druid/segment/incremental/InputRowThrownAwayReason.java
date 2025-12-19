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

import java.util.EnumMap;
import java.util.Map;

/**
 * Reasons why an input row may be thrown away during ingestion.
 */
public enum InputRowThrownAwayReason
{
  /**
   * The row was null or the input record was empty.
   */
  NULL_OR_EMPTY_RECORD("null"),

  /**
   * The row's timestamp is before the minimum message time (late message rejection).
   */
  BEFORE_MIN_MESSAGE_TIME("beforeMinMessageTime"),

  /**
   * The row's timestamp is after the maximum message time (early message rejection).
   */
  AFTER_MAX_MESSAGE_TIME("afterMaxMessageTime"),

  /**
   * The row was filtered out by a transformSpec filter or other row filter.
   */
  FILTERED("filtered"),

  /**
   * A backwards-compatible value for tracking filter reasons for ingestion tasks using older Druid versions without filter reason tracking.
   */
  UNKNOWN("unknown");

  private final String reason;

  InputRowThrownAwayReason(String reason)
  {
    this.reason = reason;
  }

  /**
   * Returns string value representation of this {@link InputRowThrownAwayReason} for metric emission.
   */
  public String getReason()
  {
    return reason;
  }

  /**
   * Public utility for building a mutable frequency map over the possible {@link InputRowThrownAwayReason} values.
   */
  public static Map<InputRowThrownAwayReason, Long> buildBaseCounterMap()
  {
    final EnumMap<InputRowThrownAwayReason, Long> result = new EnumMap<>(InputRowThrownAwayReason.class);
    for (InputRowThrownAwayReason reason : InputRowThrownAwayReason.values()) {
      result.put(reason, 0L);
    }
    return result;
  }
}
