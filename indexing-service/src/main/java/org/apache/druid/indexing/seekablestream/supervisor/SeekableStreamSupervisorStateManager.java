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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.seekablestream.SeekableStreamSupervisorConfig;
import org.apache.druid.indexing.seekablestream.exceptions.NonTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.PossiblyTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.TransientStreamException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.utils.CircularBuffer;
import org.codehaus.plexus.util.ExceptionUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class SeekableStreamSupervisorStateManager
{
  public enum State
  {
    // Error states - ordered from high to low priority
    UNHEALTHY_SUPERVISOR(1, false, false),
    UNHEALTHY_TASKS(2, false, false),
    UNABLE_TO_CONNECT_TO_STREAM(3, false, false),
    LOST_CONTACT_WITH_STREAM(4, false, false),

    // Non-error states - equal priority
    WAITING_TO_RUN(5, true, true),
    CONNECTING_TO_STREAM(5, true, true),
    DISCOVERING_INITIAL_TASKS(5, true, true),
    CREATING_TASKS(5, true, true),
    RUNNING(5, true, false),
    SUSPENDED(5, true, false),
    SHUTTING_DOWN(5, true, false);

    // Lower priority number means higher priority and vice versa
    private final int priority;
    private final boolean healthy;
    private final boolean firstRunOnly;

    State(int priority, boolean healthy, boolean firstRunOnly)
    {
      this.priority = priority;
      this.healthy = healthy;
      this.firstRunOnly = firstRunOnly;
    }

    // We only want to set these only if the supervisor hasn't had a successful iteration yet
    public boolean isFirstRunOnly()
    {
      return firstRunOnly;
    }

    public boolean isHealthy()
    {
      return healthy;
    }
  }

  public enum StreamErrorTransience
  {
    TRANSIENT,
    POSSIBLY_TRANSIENT,
    NON_TRANSIENT,
    NON_STREAM_ERROR
  }

  private State supervisorState;
  private final int healthinessThreshold;
  private final int unhealthinessThreshold;
  private final int healthinessTaskThreshold;
  private final int unhealthinessTaskThreshold;

  private boolean atLeastOneSuccessfulRun = false;
  private boolean currentRunSuccessful = true;
  private final CircularBuffer<TaskState> completedTaskHistory;
  private final CircularBuffer<State> supervisorStateHistory; // From previous runs
  private final ExceptionEventStore eventStore;

  public SeekableStreamSupervisorStateManager(
      State initialState,
      SeekableStreamSupervisorConfig supervisorConfig
  )
  {
    this.supervisorState = initialState;
    this.healthinessThreshold = supervisorConfig.getHealthinessThreshold();
    this.unhealthinessThreshold = supervisorConfig.getUnhealthinessThreshold();
    this.healthinessTaskThreshold = supervisorConfig.getTaskHealthinessThreshold();
    this.unhealthinessTaskThreshold = supervisorConfig.getTaskUnhealthinessThreshold();

    Preconditions.checkArgument(supervisorConfig.getMaxStoredExceptionEvents() >= Math.max(
        healthinessThreshold,
        unhealthinessThreshold
    ), "maxStoredExceptionEvents must be >= to max(healthinessThreshold, unhealthinessThreshold)");

    this.eventStore = new ExceptionEventStore(
        supervisorConfig.getMaxStoredExceptionEvents(),
        supervisorConfig.isStoringStackTraces()
    );
    this.completedTaskHistory = new CircularBuffer<>(Math.max(healthinessTaskThreshold, unhealthinessTaskThreshold));
    this.supervisorStateHistory = new CircularBuffer<>(Math.max(healthinessThreshold, unhealthinessThreshold));
  }

  /**
   * Certain supervisor states are only valid if the supervisor hasn't had a successful iteration yet.  This function
   * checks if there's been at least one successful iteration, and if applicable sets supervisor state to an appropriate
   * new state.
   */
  public void maybeSetState(State newState)
  {
    if (!newState.isFirstRunOnly() || !atLeastOneSuccessfulRun) {
      supervisorState = newState;
    }

    if (State.SUSPENDED.equals(newState)) {
      atLeastOneSuccessfulRun = false; // We want the startup states again after being suspended
    }
  }

  public void storeThrowableEvent(Throwable t)
  {
    eventStore.storeThrowable(t instanceof PossiblyTransientStreamException && atLeastOneSuccessfulRun
                              ? new TransientStreamException(t.getCause())
                              : t);

    currentRunSuccessful = false;
  }

  public void storeCompletedTaskState(TaskState state)
  {
    completedTaskHistory.add(state);
  }

  public void markRunFinishedAndEvaluateHealth()
  {
    atLeastOneSuccessfulRun |= currentRunSuccessful;

    State currentRunState = State.RUNNING;

    for (Map.Entry<Class, Queue<ExceptionEvent>> events : eventStore.getRecentEventsMatchingExceptionsThrownOnCurrentRun().entrySet()) {
      if (events.getValue().size() >= unhealthinessThreshold) {
        if (events.getKey().equals(NonTransientStreamException.class) ||
            events.getKey().equals(TransientStreamException.class) ||
            events.getKey().equals(PossiblyTransientStreamException.class)) {

          currentRunState = atLeastOneSuccessfulRun
                            ? getHigherPriorityState(currentRunState, State.LOST_CONTACT_WITH_STREAM)
                            : getHigherPriorityState(currentRunState, State.UNABLE_TO_CONNECT_TO_STREAM);
        } else {
          currentRunState = getHigherPriorityState(currentRunState, State.UNHEALTHY_SUPERVISOR);
        }
      }
    }

    // Evaluate task health
    if (supervisorState == State.UNHEALTHY_TASKS) {
      boolean tasksHealthy = completedTaskHistory.size() >= healthinessTaskThreshold;
      for (int i = 0; i < Math.min(healthinessTaskThreshold, completedTaskHistory.size()); i++) {
        if (completedTaskHistory.getLatest(i) != TaskState.SUCCESS) {
          tasksHealthy = false;
        }
      }
      if (tasksHealthy) {
        currentRunState = State.RUNNING;
      } else {
        currentRunState = State.UNHEALTHY_TASKS;
      }
    } else if (supervisorState != State.UNHEALTHY_SUPERVISOR) {
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

    supervisorStateHistory.add(currentRunState);

    if (currentRunState.isHealthy() && supervisorState == State.UNHEALTHY_SUPERVISOR) {
      boolean supervisorHealthy = supervisorStateHistory.size() >= healthinessThreshold;
      for (int i = 0; i < Math.min(healthinessThreshold, supervisorStateHistory.size()); i++) {
        supervisorHealthy &= supervisorStateHistory.getLatest(i).isHealthy();
      }

      if (!supervisorHealthy) {
        currentRunState = State.UNHEALTHY_SUPERVISOR;
      }
    }

    this.supervisorState = currentRunState;

    // Reset manager state for next run
    currentRunSuccessful = true;
    eventStore.resetErrorsEncounteredOnRun();
  }

  public List<ExceptionEvent> getExceptionEvents()
  {
    return eventStore.getRecentEvents();
  }

  public State getSupervisorState()
  {
    return supervisorState;
  }

  private State getHigherPriorityState(State s1, State s2)
  {
    return s1.priority < s2.priority ? s1 : s2;
  }

  @JsonPropertyOrder({"timestamp", "exceptionClass", "streamErrorTransience", "message"})
  public static class ExceptionEvent
  {
    private Class exceptionClass;
    // Contains full stackTrace if storingStackTraces is true
    private String errorMessage;
    private DateTime timestamp;
    private StreamErrorTransience streamErrorTransience;

    public ExceptionEvent(
        Throwable t,
        boolean storingStackTraces,
        StreamErrorTransience streamErrorTransience
    )
    {
      this.exceptionClass = t.getClass();
      this.errorMessage = storingStackTraces ? ExceptionUtils.getStackTrace(t) : t.getMessage();
      this.timestamp = DateTimes.nowUtc();
      this.streamErrorTransience = streamErrorTransience;
    }

    @JsonProperty("exceptionClass")
    public Class getExceptionClass()
    {
      return exceptionClass;
    }

    @JsonProperty("message")
    public String getErrorMessage()
    {
      return errorMessage;
    }

    @JsonProperty("timestamp")
    public DateTime getTimestamp()
    {
      return timestamp;
    }

    @JsonProperty("errorTransience")
    public StreamErrorTransience getStreamErrorTransience()
    {
      return streamErrorTransience;
    }
  }

  private static class ExceptionEventStore
  {
    private final Queue<ExceptionEvent> recentEventsQueue;
    private final ConcurrentHashMap<Class, Queue<ExceptionEvent>> recentEventsMap;
    private final int numEventsToStore;
    private final boolean storeStackTraces;
    private final Set<Class> errorsEncounteredOnRun;

    private ExceptionEventStore(int numEventsToStore, boolean storeStackTraces)
    {
      this.recentEventsQueue = new ConcurrentLinkedQueue<>();
      this.recentEventsMap = new ConcurrentHashMap<>(numEventsToStore);
      this.numEventsToStore = numEventsToStore;
      this.storeStackTraces = storeStackTraces;
      this.errorsEncounteredOnRun = new HashSet<>();
    }

    private void storeThrowable(Throwable t)
    {
      Queue<ExceptionEvent> exceptionEventsForClassT = recentEventsMap.getOrDefault(
          t.getClass(),
          new ConcurrentLinkedQueue<>()
      );

      StreamErrorTransience transience;
      if (t instanceof PossiblyTransientStreamException) {
        transience = StreamErrorTransience.POSSIBLY_TRANSIENT;
      } else if (t instanceof TransientStreamException) {
        transience = StreamErrorTransience.TRANSIENT;
      } else if (t instanceof NonTransientStreamException) {
        transience = StreamErrorTransience.NON_TRANSIENT;
      } else {
        transience = StreamErrorTransience.NON_STREAM_ERROR;
      }

      ExceptionEvent eventToAdd = new ExceptionEvent(t, storeStackTraces, transience);

      recentEventsQueue.add(eventToAdd);

      if (recentEventsQueue.size() > numEventsToStore) {
        ExceptionEvent removedEvent = recentEventsQueue.poll();
        recentEventsMap.get(removedEvent.getExceptionClass()).poll();
      }
      exceptionEventsForClassT.add(eventToAdd);
      errorsEncounteredOnRun.add(t.getClass());
      recentEventsMap.put(t.getClass(), exceptionEventsForClassT);
    }

    private void resetErrorsEncounteredOnRun()
    {
      errorsEncounteredOnRun.clear();
    }

    private List<ExceptionEvent> getRecentEvents()
    {
      return new ArrayList<>(recentEventsQueue);
    }

    private Map<Class, Queue<ExceptionEvent>> getRecentEventsMatchingExceptionsThrownOnCurrentRun()
    {
      return recentEventsMap.entrySet()
                            .stream()
                            .filter(x -> errorsEncounteredOnRun.contains(x.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }
}
