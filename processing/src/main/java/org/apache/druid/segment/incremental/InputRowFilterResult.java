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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Result of filtering an input row during ingestion.
 */
public enum InputRowFilterResult
{
  /**
   * The row passed the filter and should be processed.
   */
  ACCEPTED("accepted"),
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

  private static final InputRowFilterResult[] REJECTED_VALUES = Arrays.stream(InputRowFilterResult.values())
                                                                      .filter(InputRowFilterResult::isRejected)
                                                                      .toArray(InputRowFilterResult[]::new);
  public static final int NUM_FILTER_RESULT = InputRowFilterResult.values().length;

  private final String reason;

  InputRowFilterResult(String reason)
  {
    this.reason = reason;
  }

  /**
   * Returns string value representation of this {@link InputRowFilterResult} for metric emission.
   */
  public String getReason()
  {
    return reason;
  }

  /**
   * Returns true if this result indicates the row was rejected (thrown away).
   * Returns false for {@link #ACCEPTED}.
   */
  public boolean isRejected()
  {
    return this != ACCEPTED;
  }

  /**
   * Public utility for building a mutable frequency map over the possible rejection {@link InputRowFilterResult} values.
   * Keys on {@link InputRowFilterResult#getReason()} rather than the enum name as the latter is more likely to change longer-term.
   * It is also easier to have stats payload keys match what is being emitted in metrics.
   */
  public static Map<String, Long> buildRejectedCounterMap()
  {
    final Map<String, Long> result = new HashMap<>();
    for (InputRowFilterResult reason : InputRowFilterResult.rejectedValues()) {
      result.put(reason.getReason(), 0L);
    }
    return result;
  }

  /**
   * Returns {@link InputRowFilterResult} that are not rejection states.
   */
  public static InputRowFilterResult[] rejectedValues()
  {
    return REJECTED_VALUES;
  }
}
