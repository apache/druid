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
import com.google.common.base.Optional;
import org.apache.druid.indexing.seekablestream.exceptions.NonTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.PossiblyTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.TransientStreamException;
import org.apache.druid.java.util.common.DateTimes;
import org.codehaus.plexus.util.ExceptionUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SeekableStreamSupervisorStateManager
{
  public enum SupervisorState
  {
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
  // Group error (throwable) events by the type of Throwable (i.e. class name)
  private final ConcurrentHashMap<Class, List<ThrowableEvent>> throwableEvents;
  // Remove all throwableEvents that aren't in this set at the end of each run (transient)
  private final Set<Class> errorsEncounteredOnRun;
  private final int unhealthinessThreshold;
  private boolean atLeastOneSuccessfulRun;
  private boolean currentRunSuccessful;

  public SeekableStreamSupervisorStateManager(
      SupervisorState initialState,
      int unhealthinessThreshold
  )
  {
    this.state = initialState;
    this.throwableEvents = new ConcurrentHashMap<>();
    this.errorsEncounteredOnRun = new HashSet<>();
    this.unhealthinessThreshold = unhealthinessThreshold;
    this.atLeastOneSuccessfulRun = false;
    this.currentRunSuccessful = true;
  }

  public Optional<SupervisorState> setStateIfNoSuccessfulRunYet(SupervisorState state)
  {
    if (!atLeastOneSuccessfulRun) {
      return Optional.of(setState(state));
    }
    return Optional.absent();
  }

  public SupervisorState setState(SupervisorState state)
  {
    if (state.equals(SupervisorState.SUSPENDED))
    {
      atLeastOneSuccessfulRun = false; // We want the startup states again
    }
    this.state = state;
    return state;
  }

  public void storeThrowableEvent(Throwable t)
  {
    if (t instanceof PossiblyTransientStreamException && !atLeastOneSuccessfulRun) {
      t = new NonTransientStreamException(t);
    } else if (t instanceof PossiblyTransientStreamException) {
      t = new TransientStreamException(t);
    }

    List<ThrowableEvent> throwableEventsForClassT = throwableEvents.getOrDefault(
        t.getClass().getCanonicalName(),
        new ArrayList<>()
    );
    throwableEventsForClassT.add(
        new ThrowableEvent(
            t.getMessage(),
            ExceptionUtils.getStackTrace(t),
            DateTimes.nowUtc()
        ));
    throwableEvents.put(t.getClass(), throwableEventsForClassT);
  }

  public void markRunFinished()
  {
    if (currentRunSuccessful) {
      atLeastOneSuccessfulRun = true;
    }

    for (Class throwableClass : errorsEncounteredOnRun) {
      if (!throwableEvents.keySet().contains(throwableClass)) {
        throwableEvents.remove(throwableClass);
      }
    }
    // At this point, all the events in throwableEvents should be non-transient
    errorsEncounteredOnRun.clear();

    boolean stateUpdated = false;
    for (Map.Entry<Class, List<ThrowableEvent>> events : throwableEvents.entrySet()) {
      if (events.getValue().size() > unhealthinessThreshold && events.getKey().equals(NonTransientStreamException.class)) {
        setState(SupervisorState.UNABLE_TO_CONNECT_TO_STREAM);
        stateUpdated = true;
      } else if (events.getValue().size() > unhealthinessThreshold) {
        setState(SupervisorState.UNHEALTHY);
        stateUpdated = true;
      }
    }
    if (!stateUpdated) {
      setState(SupervisorState.RUNNING);
    }
  }

  public Map<Class, List<ThrowableEvent>> getThrowableEventList()
  {
    return throwableEvents;
  }

  public SupervisorState getState()
  {
    return state;
  }

  public static class ThrowableEvent
  {
    private String message;
    private String stackTrace;
    private DateTime timestamp;

    public ThrowableEvent(
        String message,
        String stackTrace,
        DateTime timestamp
    )
    {
      this.stackTrace = stackTrace;
      this.message = message;
      this.timestamp = timestamp;
    }

    @JsonProperty
    public String getMessage()
    {
      return message;
    }

    @JsonProperty
    public String getStackTrace()
    {
      return stackTrace;
    }

    @JsonProperty
    public DateTime getTimestamp()
    {
      return timestamp;
    }
  }
}
