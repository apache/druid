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
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SeekableStreamSupervisorStateManager
{
  public enum State
  {
    // Error states - ordered from high to low priority
    UNHEALTHY_SUPERVISOR(false, false),
    UNHEALTHY_TASKS(false, false),
    UNABLE_TO_CONNECT_TO_STREAM(false, false),
    LOST_CONTACT_WITH_STREAM(false, false),

    // Non-error states - equal priority
    WAITING_TO_RUN(true, true),
    CONNECTING_TO_STREAM(true, true),
    DISCOVERING_INITIAL_TASKS(true, true),
    CREATING_TASKS(true, true),
    RUNNING(true, false),
    SUSPENDED(true, false),
    SHUTTING_DOWN(true, false);

    private final boolean healthy;
    private final boolean firstRunOnly;

    State(boolean healthy, boolean firstRunOnly)
    {
      this.healthy = healthy;
      this.firstRunOnly = firstRunOnly;
    }
  }

  private final SeekableStreamSupervisorConfig supervisorConfig;
  private final State healthySteadyState;

  private final Deque<ExceptionEvent> recentEventsQueue;

  private State supervisorState;

  private boolean atLeastOneSuccessfulRun = false;
  private boolean currentRunSuccessful = true;

  // Used to determine if a low consecutiveSuccessfulRuns/consecutiveSuccessfulTasks means that the supervisor is
  // recovering from an unhealthy state, or if the supervisor just started and hasn't run many times yet.
  private boolean hasHitUnhealthinessThreshold = false;
  private boolean hasHitTaskUnhealthinessThreshold = false;

  private int consecutiveFailedRuns = 0;
  private int consecutiveSuccessfulRuns = 0;
  private int consecutiveFailedTasks = 0;
  private int consecutiveSuccessfulTasks = 0;

  public SeekableStreamSupervisorStateManager(
      State initialState,
      State healthySteadyState,
      SeekableStreamSupervisorConfig supervisorConfig
  )
  {
    Preconditions.checkArgument(supervisorConfig.getMaxStoredExceptionEvents() >= Math.max(
        supervisorConfig.getHealthinessThreshold(),
        supervisorConfig.getUnhealthinessThreshold()
    ), "maxStoredExceptionEvents must be >= to max(healthinessThreshold, unhealthinessThreshold)");

    this.supervisorState = initialState;
    this.supervisorConfig = supervisorConfig;
    this.healthySteadyState = healthySteadyState;

    this.recentEventsQueue = new ConcurrentLinkedDeque<>();
  }

  /**
   * Certain states are only valid if the supervisor hasn't had a successful iteration. This method checks if there's
   * been at least one successful iteration, and if applicable sets supervisor state to an appropriate new state.
   */
  public void maybeSetState(State proposedState)
  {
    // if we're over our unhealthiness threshold, set the state to the appropriate unhealthy state
    if (consecutiveFailedRuns >= supervisorConfig.getUnhealthinessThreshold()) {
      hasHitUnhealthinessThreshold = true;
      supervisorState = recentEventsQueue.getLast().isStreamException()
                        ? (atLeastOneSuccessfulRun ? State.LOST_CONTACT_WITH_STREAM : State.UNABLE_TO_CONNECT_TO_STREAM)
                        : State.UNHEALTHY_SUPERVISOR;
      return;
    }

    // if we're over our task unhealthiness threshold, set the state to UNHEALTHY_TASKS
    if (consecutiveFailedTasks >= supervisorConfig.getTaskUnhealthinessThreshold()) {
      hasHitTaskUnhealthinessThreshold = true;
      supervisorState = State.UNHEALTHY_TASKS;
      return;
    }

    // if we're currently in an unhealthy state and are below our healthiness threshold for either runs and tasks,
    // ignore the proposed state; the healthiness threshold only applies if we've had a failure in the past
    if (!this.supervisorState.healthy
        && ((hasHitUnhealthinessThreshold && consecutiveSuccessfulRuns < supervisorConfig.getHealthinessThreshold())
            || (hasHitTaskUnhealthinessThreshold
                && consecutiveSuccessfulTasks < supervisorConfig.getTaskHealthinessThreshold()))) {
      return;
    }

    // if we're trying to switch to a healthy steady state (i.e. RUNNING or SUSPENDED) but haven't had a successful run
    // yet, refuse to switch and prefer the more specific states used for first run (CONNECTING_TO_STREAM,
    // DISCOVERING_INITIAL_TASKS, CREATING_TASKS, etc.)
    if (healthySteadyState.equals(proposedState) && !atLeastOneSuccessfulRun) {
      return;
    }

    // accept the state if it is not a firstRunOnly state OR we are still on the first run
    if (!proposedState.firstRunOnly || !atLeastOneSuccessfulRun) {
      supervisorState = proposedState;
    }
  }

  public void storeThrowableEvent(Throwable t)
  {
    recentEventsQueue.add(new ExceptionEvent(t, supervisorConfig.isStoringStackTraces()));

    if (recentEventsQueue.size() > supervisorConfig.getMaxStoredExceptionEvents()) {
      recentEventsQueue.poll();
    }

    currentRunSuccessful = false;
  }

  public void storeCompletedTaskState(TaskState state)
  {
    if (state.isSuccess()) {
      consecutiveSuccessfulTasks++;
      consecutiveFailedTasks = 0;
    } else if (state.isFailure()) {
      consecutiveFailedTasks++;
      consecutiveSuccessfulTasks = 0;
    }
  }

  public void markRunFinished()
  {
    atLeastOneSuccessfulRun |= currentRunSuccessful;

    consecutiveSuccessfulRuns = currentRunSuccessful ? consecutiveSuccessfulRuns + 1 : 0;
    consecutiveFailedRuns = currentRunSuccessful ? 0 : consecutiveFailedRuns + 1;

    // Try to set the state to RUNNING or SUSPENDED, depending on how the supervisor was configured. This will be
    // rejected if we haven't had atLeastOneSuccessfulRun (in favor of the more specific states for the initial run) and
    // will instead trigger setting the state to an unhealthy one if we are now over the error thresholds.
    maybeSetState(healthySteadyState);

    // reset for next run
    currentRunSuccessful = true;
  }

  public List<ExceptionEvent> getExceptionEvents()
  {
    return new ArrayList<>(recentEventsQueue);
  }

  public State getSupervisorState()
  {
    return supervisorState;
  }

  public boolean isAtLeastOneSuccessfulRun()
  {
    return atLeastOneSuccessfulRun;
  }

  @JsonPropertyOrder({"timestamp", "exceptionClass", "streamException", "message"})
  public static class ExceptionEvent
  {
    private static final List<String> SKIPPED_EXCEPTION_CLASSES = ImmutableList.of(
        RuntimeException.class.getName(),
        StreamException.class.getName()
    );

    private final DateTime timestamp;
    private final String exceptionClass;
    private final boolean streamException;
    private final String message; // contains full stackTrace if storingStackTraces is true

    public ExceptionEvent(Throwable t, boolean storingStackTraces)
    {
      this.timestamp = DateTimes.nowUtc();
      this.exceptionClass = getMeaningfulExceptionClass(t);
      this.streamException = ExceptionUtils.indexOfType(t, StreamException.class) != -1;
      this.message = storingStackTraces ? ExceptionUtils.getStackTrace(t) : t.getMessage();
    }

    @JsonProperty
    public DateTime getTimestamp()
    {
      return timestamp;
    }

    @JsonProperty
    public String getExceptionClass()
    {
      return exceptionClass;
    }

    @JsonProperty
    public boolean isStreamException()
    {
      return streamException;
    }

    @JsonProperty
    public String getMessage()
    {
      return message;
    }

    private String getMeaningfulExceptionClass(Throwable t)
    {
      return ((List<Throwable>) ExceptionUtils.getThrowableList(t))
          .stream()
          .map(x -> x.getClass().getName())
          .filter(x -> !SKIPPED_EXCEPTION_CLASSES.contains(x))
          .findFirst()
          .orElse(Exception.class.getName());
    }
  }
}
