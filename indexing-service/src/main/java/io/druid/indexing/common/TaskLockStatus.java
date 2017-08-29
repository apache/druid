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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.java.util.common.ISE;

public enum TaskLockStatus
{
  PREEMPTIBLE,
  NON_PREEMPTIBLE,
  REVOKED;

  @JsonCreator
  public static TaskLockStatus fromString(String status)
  {
    return TaskLockStatus.valueOf(status);
  }

  @Override
  @JsonValue
  public String toString()
  {
    return this.name();
  }

  public TaskLockStatus transitTo(TaskLockStatus toStatus)
  {
    // The allowed transitions are
    // PREEMPTIBLE -> NON_PREEMPTIBLE -> PREEMPTIBLE
    // PREEMPTIBLE -> REVOKED

    if (this == toStatus) {
      throw new ISE("Invalid transition from [%s] to [%s]", this, toStatus);
    }

    if (this == NON_PREEMPTIBLE && toStatus == PREEMPTIBLE) {
      return toStatus;
    } else if (this == PREEMPTIBLE) {
      return toStatus;
    } else {
      throw new ISE("Invalid transition from [%s] to [%s]", this, toStatus);
    }
  }
}
