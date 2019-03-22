/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import java.util.List;

public class SupervisorStateManager
{
  public enum SupervisorState {
    WAITING_TO_RUN,
    CONNECTING_TO_STREAM,
    DISCOVERING_INITIAL_TASKS,
    CREATING_TASKS,
    RUNNING,
    SUSPENDED,
    SHUTTING_DOWN,
    UNABLE_TO_CONNECT_TO_STREAM,
    LOST_CONTACT_WITH_STREAM,
    UNHEALTHY
  }

  private SupervisorState state;
  private final CircularBuffer<ThrowableEvent> throwableEvents;
  private final int unhealthinessThreshold;
  private boolean firstRun;

  public SupervisorStateManager(
      SupervisorState initialState,
      int maxSavedExceptions,
      int unhealthinessThreshold
  )
  {
    this.state = initialState;
    this.throwableEvents = new CircularBuffer<>(maxSavedExceptions);
    this.unhealthinessThreshold = unhealthinessThreshold;
  }

  public void setState(SupervisorState state)
  {
    this.state = state;
  }

  public void storeThrowableEventAndUpdateState(Throwable t)
  {
    throwableEvents.add(new ThrowableEvent(DateTimes.nowUtc(), t));
  }

  public List<ThrowableEvent> getThrowableEventList()
  {
    return throwableEvents.toList();
  }

  public static class ThrowableEvent
  {
    private final DateTime timestamp;
    private final Throwable t;

    public ThrowableEvent(
        DateTime timestamp,
        Throwable t
    )
    {
      this.timestamp = timestamp;
      this.t = t;
    }

    @JsonProperty
    public DateTime getTimestamp()
    {
      return timestamp;
    }

    @JsonProperty
    public Throwable getThrowable()
    {
      return t;
    }
  }
}
