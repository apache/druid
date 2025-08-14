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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Enum denoting the reason for query cancellation.
 */
public enum CancellationReason
{
  /**
   * Query was cancaled due to the task shutting down.
   */
  TASK_SHUTDOWN("Task shutdown"),
  /**
   * Query was cancaled due to a request from the user.
   */
  USER_REQUEST("User request"),
  /**
   * Query was canceled due to exceeding the configured query timeout.
   */
  QUERY_TIMEOUT("Query timeout"),
  /**
   * Query was canceled due to an unknown reason.
   */
  UNKNOWN("Unknown");

  private final String reason;

  CancellationReason(String reason)
  {
    this.reason = reason;
  }

  @JsonCreator
  public static CancellationReason fromString(String value)
  {
    for (CancellationReason mode : values()) {
      if (mode.name().equals(value)) {
        return mode;
      }
    }

    // If the reason is not known, it might be due to the sending task being on a newer version. Just return UNKNOWN.
    return UNKNOWN;
  }

  @Override
  public String toString()
  {
    return reason;
  }
}
