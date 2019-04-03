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
import org.apache.druid.indexing.seekablestream.SeekableStreamSupervisorConfig;
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
    // Error states are ordered from high to low priority
    UNHEALTHY_SUPERVISOR(1),
    UNHEALTHY_TASKS(2),
    UNABLE_TO_CONNECT_TO_STREAM(3),
    LOST_CONTACT_WITH_STREAM(4),
    // Non-error states are equal priority
    WAITING_TO_RUN(5),
    CONNECTING_TO_STREAM(5),
    DISCOVERING_INITIAL_TASKS(5),
    CREATING_TASKS(5),
    RUNNING(5),
    SUSPENDED(5),
    SHUTTING_DOWN(5);

    // Lower priority number means higher priority and vice versa
    private final int priority;

    State(int priority)
    {
      this.priority = priority;
    }

    public boolean isHealthy()
    {
      return !ImmutableSet.of(
          UNHEALTHY_SUPERVISOR,
          UNHEALTHY_TASKS,
          UNABLE_TO_CONNECT_TO_STREAM,
          LOST_CONTACT_WITH_STREAM
      ).contains(this);
    }
  }

  private State supervisorState;
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
  private final boolean storingStackTraces;
  private final CircularBuffer<TaskState> completedTaskHistory;
  private final CircularBuffer<State> stateHistory; // From previous runs

  public SeekableStreamSupervisorStateManager(
      State initialState,
      SeekableStreamSupervisorConfig config
  )
  {
    this.supervisorState = initialState;
    this.throwableEvents = new ConcurrentHashMap<>();
    this.errorsEncounteredOnRun = new HashSet<>();
    this.healthinessThreshold = config.getSupervisorHealthinessThreshold();
    this.unhealthinessThreshold = config.getSupervisorUnhealthinessThreshold();
    this.healthinessTaskThreshold = config.getSupervisorTaskHealthinessThreshold();
    this.unhealthinessTaskThreshold = config.getSupervisorTaskUnhealthinessThreshold();
    this.storingStackTraces = config.isStoringStackTraces();
    this.completedTaskHistory = new CircularBuffer<>(Math.max(healthinessTaskThreshold, unhealthinessTaskThreshold));
    this.stateHistory = new CircularBuffer<>(Math.max(healthinessThreshold, unhealthinessThreshold));
  }

  public Optional<State> setStateIfNoSuccessfulRunYet(State newState)
  {
    if (!atLeastOneSuccessfulRun) {
      return Optional.of(setState(newState));
    }
    return Optional.absent();
  }

  public State setState(State newState)
  {
    if (newState.equals(State.SUSPENDED)) {
      atLeastOneSuccessfulRun = false; // We want the startup states again after being suspended
    }
    this.supervisorState = newState;
    return newState;
  }

  public void storeThrowableEvent(Throwable t)
  {
    if (t instanceof PossiblyTransientStreamException && !atLeastOneSuccessfulRun) {
      t = new NonTransientStreamException(t);
    } else if (t instanceof PossiblyTransientStreamException) {
      t = new TransientStreamException(t);
    }

    List<ThrowableEvent> throwableEventsForClassT = throwableEvents.getOrDefault(
        t.getClass(),
        new ArrayList<>()
    );
    throwableEventsForClassT.add(
        new ThrowableEvent(
            t.getMessage(),
            storingStackTraces ? ExceptionUtils.getStackTrace(t) : null,
            DateTimes.nowUtc()
        ));
    errorsEncounteredOnRun.add(t.getClass());
    throwableEvents.put(t.getClass(), throwableEventsForClassT);
    currentRunSuccessful = false;
  }

  public void storeCompletedTaskState(TaskState state)
  {
    completedTaskHistory.add(state);
  }

  public void markRunFinishedAndEvaluateHealth()
  {
    if (currentRunSuccessful) {
      atLeastOneSuccessfulRun = true;
    }
    currentRunSuccessful = true;

    // Remove transient errors from throwableEvents
    for (Class throwableClass : throwableEvents.keySet()) {
      if (!errorsEncounteredOnRun.contains(throwableClass)) {
        throwableEvents.remove(throwableClass);
      }
    }

    errorsEncounteredOnRun.clear();

    State currentRunState = State.RUNNING;

    for (Map.Entry<Class, List<ThrowableEvent>> events : throwableEvents.entrySet()) {
      if (events.getValue().size() >= unhealthinessThreshold) {
        if (events.getKey().equals(NonTransientStreamException.class)) {
          currentRunState = getHigherPriorityState(currentRunState, State.UNABLE_TO_CONNECT_TO_STREAM);
        } else if (events.getKey().equals(TransientStreamException.class)) {
          currentRunState = getHigherPriorityState(currentRunState, State.LOST_CONTACT_WITH_STREAM);
        } else {
          currentRunState = getHigherPriorityState(currentRunState, State.UNHEALTHY_SUPERVISOR);
        }
      }
    }

    // Evaluate task health
    if (supervisorState == State.UNHEALTHY_TASKS) {
      boolean tasksHealthy = completedTaskHistory.size() >= healthinessTaskThreshold;
      for (int i = 0; i < Math.min(healthinessTaskThreshold, completedTaskHistory.size()); i++) {
        // Last healthinessTaskThreshold tasks must be healthy for state to change from
        // UNHEALTHY_TASKS to RUNNING
        if (completedTaskHistory.getLatest(i) != TaskState.SUCCESS) {
          tasksHealthy = false;
        }
      }
      if (tasksHealthy && currentRunState == State.UNHEALTHY_TASKS) {
        currentRunState = State.RUNNING;
      } else if (!tasksHealthy) {
        currentRunState = State.UNHEALTHY_TASKS;
      }
    } else {
      boolean tasksUnhealthy = completedTaskHistory.size() >= unhealthinessTaskThreshold;
      for (int i = 0; i < Math.min(unhealthinessTaskThreshold, completedTaskHistory.size()); i++) {
        // Last unhealthinessTaskThreshold tasks must be unhealthy for state to change to
        // UNHEALTHY_TASKS
        if (completedTaskHistory.getLatest(i) != TaskState.FAILED) {
          tasksUnhealthy = false;
        }
      }

      if (tasksUnhealthy) {
        currentRunState = getHigherPriorityState(currentRunState, State.UNHEALTHY_TASKS);
      }
    }

    stateHistory.add(currentRunState);

    if (currentRunState.isHealthy() && supervisorState == State.UNHEALTHY_SUPERVISOR) {
      currentRunState = State.UNHEALTHY_SUPERVISOR;
      boolean supervisorHealthy = stateHistory.size() >= healthinessThreshold;
      for (int i = 0; i < Math.min(healthinessThreshold, stateHistory.size()); i++) {
        if (!stateHistory.getLatest(i).isHealthy()) {
          supervisorHealthy = false;
        }
      }
      if (supervisorHealthy) {
        currentRunState = State.RUNNING;
      }
    }

    setState(currentRunState);
  }

  public Map<Class, List<ThrowableEvent>> getThrowableEvents()
  {
    return throwableEvents;
  }

  public State getSupervisorState()
  {
    return supervisorState;
  }

  private State getHigherPriorityState(State s1, State s2)
  {
    return s1.priority < s2.priority ? s1 : s2;
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
