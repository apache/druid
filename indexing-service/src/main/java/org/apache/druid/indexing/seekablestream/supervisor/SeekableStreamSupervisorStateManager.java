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
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
      Set<State> unhealthyStates = ImmutableSet.of(
          UNHEALTHY_SUPERVISOR,
          UNHEALTHY_TASKS,
          UNABLE_TO_CONNECT_TO_STREAM,
          LOST_CONTACT_WITH_STREAM
      );
      return !unhealthyStates.contains(this);
    }
  }

  private State supervisorState;
  // Remove all throwableEvents that aren't in this set at the end of each run (transient)
  private final int healthinessThreshold;
  private final int unhealthinessThreshold;
  private final int healthinessTaskThreshold;
  private final int unhealthinessTaskThreshold;

  private boolean atLeastOneSuccessfulRun = false;
  private boolean currentRunSuccessful = true;
  private final CircularBuffer<TaskState> completedTaskHistory;
  private final CircularBuffer<State> stateHistory; // From previous runs
  private final ExceptionEventStorage eventStorage;

  public SeekableStreamSupervisorStateManager(
      State initialState,
      SeekableStreamSupervisorConfig supervisorConfig
  )
  {
    this.supervisorState = initialState;
    this.eventStorage = new ExceptionEventStorage(
        supervisorConfig.getNumExceptionEventsToStore(),
        supervisorConfig.isStoringStackTraces()
    );
    this.healthinessThreshold = supervisorConfig.getSupervisorHealthinessThreshold();
    this.unhealthinessThreshold = supervisorConfig.getSupervisorUnhealthinessThreshold();
    this.healthinessTaskThreshold = supervisorConfig.getSupervisorTaskHealthinessThreshold();
    this.unhealthinessTaskThreshold = supervisorConfig.getSupervisorTaskUnhealthinessThreshold();
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
    if (t instanceof PossiblyTransientStreamException && atLeastOneSuccessfulRun) {
      t = new TransientStreamException(t);
    }

    eventStorage.storeThrowable(t);
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

    State currentRunState = State.RUNNING;

    for (Map.Entry<Class, Queue<ExceptionEvent>> events : eventStorage.getNonTransientRecentEvents().entrySet()) {
      if (events.getValue().size() >= unhealthinessThreshold) {
        if (events.getKey().equals(NonTransientStreamException.class)) {
          currentRunState = getHigherPriorityState(currentRunState, State.UNABLE_TO_CONNECT_TO_STREAM);
        } else if (events.getKey().equals(TransientStreamException.class) ||
                   events.getKey().equals(PossiblyTransientStreamException.class)) {
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

    // Reset manager state for next run
    currentRunSuccessful = true;
    eventStorage.resetErrorsEncounteredOnRun();
  }

  public Queue<ExceptionEvent> getExceptionEvents()
  {
    return eventStorage.getRecentEvents();
  }

  public State getSupervisorState()
  {
    return supervisorState;
  }

  private State getHigherPriorityState(State s1, State s2)
  {
    return s1.priority < s2.priority ? s1 : s2;
  }

  @JsonPropertyOrder({"timestamp", "exceptionClass", "message", "stackTrace"})
  public class ExceptionEvent
  {
    private Class clazz;
    private String message;
    private String stackTrace;
    private DateTime timestamp;

    public ExceptionEvent(
        Throwable t,
        boolean storingStackTraces
    )
    {
      this.clazz = t.getClass();
      this.stackTrace = storingStackTraces ? ExceptionUtils.getStackTrace(t) : null;
      this.message = t.getMessage();
      this.timestamp = DateTimes.nowUtc();
    }

    @JsonProperty
    public Class getExceptionClass()
    {
      return clazz;
    }

    @JsonProperty
    public String getMessage()
    {
      return message;
    }

    @JsonProperty
    @Nullable
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

  private class ExceptionEventStorage
  {
    private final Queue<ExceptionEvent> recentEventsQueue;
    private final ConcurrentHashMap<Class, Queue<ExceptionEvent>> recentEventsMap;
    private final int numEventsToStore;
    private final boolean storeStackTraces;
    private final Set<Class> errorsEncounteredOnRun;

    public ExceptionEventStorage(int numEventsToStore, boolean storeStackTraces)
    {
      this.recentEventsQueue = new ConcurrentLinkedQueue<>();
      this.recentEventsMap = new ConcurrentHashMap<>(numEventsToStore);
      this.numEventsToStore = numEventsToStore;
      this.storeStackTraces = storeStackTraces;
      this.errorsEncounteredOnRun = new HashSet<>();
    }

    public void storeThrowable(Throwable t)
    {
      Queue<ExceptionEvent> exceptionEventsForClassT = recentEventsMap.getOrDefault(
          t.getClass(),
          new ConcurrentLinkedQueue<>()
      );

      ExceptionEvent eventToAdd = new ExceptionEvent(t, storeStackTraces);

      recentEventsQueue.add(eventToAdd);

      if (recentEventsQueue.size() > numEventsToStore) {
        ExceptionEvent removedEvent = recentEventsQueue.poll();
        recentEventsMap.get(removedEvent.getExceptionClass()).poll();
      }
      exceptionEventsForClassT.add(eventToAdd);
      errorsEncounteredOnRun.add(t.getClass());
      recentEventsMap.put(t.getClass(), exceptionEventsForClassT);
    }

    public void resetErrorsEncounteredOnRun()
    {
      errorsEncounteredOnRun.clear();
    }

    public Queue<ExceptionEvent> getRecentEvents()
    {
      return recentEventsQueue;
    }

    public ConcurrentHashMap<Class, Queue<ExceptionEvent>> getNonTransientRecentEvents()
    {
      ConcurrentHashMap<Class, Queue<ExceptionEvent>> nonTransientRecentEventsMap =
          new ConcurrentHashMap<>(recentEventsMap);

      for (Class throwableClass : recentEventsMap.keySet()) {
        if (!errorsEncounteredOnRun.contains(throwableClass)) {
          nonTransientRecentEventsMap.remove(throwableClass);
        }
      }

      return nonTransientRecentEventsMap;
    }
  }
}
