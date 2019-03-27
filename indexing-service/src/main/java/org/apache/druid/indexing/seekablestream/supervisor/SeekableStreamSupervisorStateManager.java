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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.seekablestream.exceptions.NonTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.PossiblyTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.TransientStreamException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.utils.CircularBuffer;
import org.codehaus.plexus.util.ExceptionUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SeekableStreamSupervisorStateManager
{
  public enum State
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

  private State state;
  // Group error (throwable) events by the type of Throwable (i.e. class name)
  private final ConcurrentHashMap<Class, List<ThrowableEvent>> throwableEvents;
  // Remove all throwableEvents that aren't in this set at the end of each run (transient)
  private final Set<Class> errorsEncounteredOnRun;
  private final int healthinessThreshold;
  private final int unhealthinessThreshold;
  private final int healthinessTaskThreshold;
  private final int unhealthinessTaskThreshold;

  private boolean atLeastOneSuccessfulRun = false;
  private boolean currentRunSuccessful = true;
  private int numConsecutiveSuccessfulRuns = 0;
  private int numConsecutiveFailingRuns = 0;
  private CircularBuffer<TaskState> completedTaskHistory;

  public SeekableStreamSupervisorStateManager(
      State initialState,
      int healthinessThreshold,
      int unhealthinessThreshold,
      int healthinessTaskThreshold,
      int unhealthinessTaskThreshold
  )
  {
    this.state = initialState;
    this.throwableEvents = new ConcurrentHashMap<>();
    this.errorsEncounteredOnRun = new HashSet<>();
    this.healthinessThreshold = healthinessThreshold;
    this.unhealthinessThreshold = unhealthinessThreshold;
    this.healthinessTaskThreshold = healthinessTaskThreshold;
    this.unhealthinessTaskThreshold = unhealthinessTaskThreshold;
    this.completedTaskHistory = new CircularBuffer<>(Math.max(healthinessTaskThreshold, unhealthinessTaskThreshold));
  }

  public Optional<State> setStateIfNoSuccessfulRunYet(State state)
  {
    if (!atLeastOneSuccessfulRun) {
      return Optional.of(setState(state));
    }
    return Optional.absent();
  }

  public State setState(State state)
  {
    if (state.equals(State.SUSPENDED)) {
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
    currentRunSuccessful = false;
  }

  public void markRunFinishedAndEvaluateHealth()
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

    boolean noIssues = true;
    for (Map.Entry<Class, List<ThrowableEvent>> events : throwableEvents.entrySet()) {
      if (events.getValue().size() > unhealthinessThreshold) {
        if (events.getKey().equals(NonTransientStreamException.class)) {
          setState(State.UNABLE_TO_CONNECT_TO_STREAM);
        } else if (events.getKey().equals(TransientStreamException.class)) {
          setState(State.LOST_CONTACT_WITH_STREAM);
        } else {
          setState(State.UNHEALTHY);
        }
        noIssues = false;
      }
    }

    // TODO check task health here

    if (noIssues) {
      numConsecutiveSuccessfulRuns++;
      numConsecutiveFailingRuns = 0;
    } else {
      numConsecutiveFailingRuns++;
      numConsecutiveSuccessfulRuns = 0;
    }

    if (ImmutableSet.of(State.UNHEALTHY, State.UNABLE_TO_CONNECT_TO_STREAM, State.LOST_CONTACT_WITH_STREAM)
                    .contains(state) && numConsecutiveSuccessfulRuns > healthinessThreshold) {
      setState(State.RUNNING);
    } else if (state == State.RUNNING && numConsecutiveFailingRuns > unhealthinessThreshold) {
      setState(State.UNHEALTHY);
    }
  }

  public Map<Class, List<ThrowableEvent>> getThrowableEvents()
  {
    return throwableEvents;
  }

  public State getState()
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
        @Nullable String stackTrace,
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
    @Nullable
    // TODO only set if a debug-level property is set
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
