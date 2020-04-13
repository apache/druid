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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SupervisorStateManager
{
  public interface State
  {
    /**
     * If we are in this state, is the supervisor healthy or unhealthy?
     */
    boolean isHealthy();

    /**
     * It may be helpful to provide more detailed state information (e.g. CONNECTING_TO_STREAM, CREATING_TASKS, etc.)
     * during the first run of the supervisor so that if the supervisor is unable to complete the run, we have
     * information about what stage it was in when it failed. Once the supervisor is stable, we may not be as concerned
     * about all the stages it cycles through, and just want to know if it's healthy or unhealthy. This flag indicates
     * if the state should only be accepted prior to having completed a successful run.
     */
    boolean isFirstRunOnly();

    default State getBasicState()
    {
      return this;
    }
  }

  public enum BasicState implements State
  {
    UNHEALTHY_SUPERVISOR(false, false),
    UNHEALTHY_TASKS(false, false),

    PENDING(true, true),
    RUNNING(true, false),
    SUSPENDED(true, false),
    STOPPING(true, false);

    private final boolean healthy;
    private final boolean firstRunOnly;

    BasicState(boolean healthy, boolean firstRunOnly)
    {
      this.healthy = healthy;
      this.firstRunOnly = firstRunOnly;
    }

    @Override
    public boolean isHealthy()
    {
      return healthy;
    }

    @Override
    public boolean isFirstRunOnly()
    {
      return firstRunOnly;
    }
  }

  private final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final State healthySteadyState;

  private final Deque<ExceptionEvent> recentEventsQueue = new ConcurrentLinkedDeque<>();

  private State supervisorState = BasicState.PENDING;

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

  public SupervisorStateManager(SupervisorStateManagerConfig supervisorStateManagerConfig, boolean suspended)
  {
    Preconditions.checkArgument(supervisorStateManagerConfig.getMaxStoredExceptionEvents() >= Math.max(
        supervisorStateManagerConfig.getHealthinessThreshold(),
        supervisorStateManagerConfig.getUnhealthinessThreshold()
    ), "maxStoredExceptionEvents must be >= to max(healthinessThreshold, unhealthinessThreshold)");

    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
    this.healthySteadyState = suspended ? BasicState.SUSPENDED : BasicState.RUNNING;
  }

  /**
   * Certain states are only valid if the supervisor hasn't had a successful iteration. This method checks if there's
   * been at least one successful iteration, and if applicable sets supervisor state to an appropriate new state.
   */
  public void maybeSetState(State proposedState)
  {
    // if we're over our unhealthiness threshold, set the state to the appropriate unhealthy state
    if (consecutiveFailedRuns >= supervisorStateManagerConfig.getUnhealthinessThreshold()) {
      hasHitUnhealthinessThreshold = true;
      supervisorState = getSpecificUnhealthySupervisorState();
      return;
    }

    // if we're over our task unhealthiness threshold, set the state to UNHEALTHY_TASKS
    if (consecutiveFailedTasks >= supervisorStateManagerConfig.getTaskUnhealthinessThreshold()) {
      hasHitTaskUnhealthinessThreshold = true;
      supervisorState = BasicState.UNHEALTHY_TASKS;
      return;
    }

    // if we're currently in an unhealthy state and are below our healthiness threshold for either runs and tasks,
    // ignore the proposed state; the healthiness threshold only applies if we've had a failure in the past
    if (!this.supervisorState.isHealthy()
        && ((hasHitUnhealthinessThreshold
             && consecutiveSuccessfulRuns < supervisorStateManagerConfig.getHealthinessThreshold())
            || (hasHitTaskUnhealthinessThreshold
                && consecutiveSuccessfulTasks < supervisorStateManagerConfig.getTaskHealthinessThreshold()))) {
      return;
    }

    // if we're trying to switch to a healthy steady state (i.e. RUNNING or SUSPENDED) but haven't had a successful run
    // yet, refuse to switch and prefer the more specific states used for first run (CONNECTING_TO_STREAM,
    // DISCOVERING_INITIAL_TASKS, CREATING_TASKS, etc.)
    if (healthySteadyState.equals(proposedState) && !atLeastOneSuccessfulRun) {
      return;
    }

    // accept the state if it is not a firstRunOnly state OR we are still on the first run
    if (!proposedState.isFirstRunOnly() || !atLeastOneSuccessfulRun) {
      supervisorState = proposedState;
    }
  }

  public void recordThrowableEvent(Throwable t)
  {
    recentEventsQueue.add(buildExceptionEvent(t));

    if (recentEventsQueue.size() > supervisorStateManagerConfig.getMaxStoredExceptionEvents()) {
      recentEventsQueue.poll();
    }

    currentRunSuccessful = false;
  }

  public void recordCompletedTaskState(TaskState state)
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

    // Try to set the state to RUNNING or SUSPENDED. This will be rejected if we haven't had atLeastOneSuccessfulRun
    // (in favor of the more specific states for the initial run) and will instead trigger setting the state to an
    // unhealthy one if we are now over the error thresholds.
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

  public boolean isHealthy()
  {
    return supervisorState != null && supervisorState.isHealthy();
  }

  public boolean isAtLeastOneSuccessfulRun()
  {
    return atLeastOneSuccessfulRun;
  }

  protected Deque<ExceptionEvent> getRecentEventsQueue()
  {
    return recentEventsQueue;
  }

  protected boolean isStoreStackTrace()
  {
    return supervisorStateManagerConfig.isStoreStackTrace();
  }

  protected State getSpecificUnhealthySupervisorState()
  {
    return BasicState.UNHEALTHY_SUPERVISOR;
  }

  protected ExceptionEvent buildExceptionEvent(Throwable t)
  {
    return new ExceptionEvent(t, isStoreStackTrace());
  }

  public static class ExceptionEvent
  {
    private final DateTime timestamp;
    private final String exceptionClass;
    private final String message; // contains full stackTrace if storeStackTrace is true

    public ExceptionEvent(Throwable t, boolean storeStackTrace)
    {
      this.timestamp = DateTimes.nowUtc();
      this.exceptionClass = getMeaningfulExceptionClass(t);
      this.message = storeStackTrace ? ExceptionUtils.getStackTrace(t) : t.getMessage();
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
    public String getMessage()
    {
      return message;
    }

    protected boolean shouldSkipException(String className)
    {
      return RuntimeException.class.getName().equals(className);
    }

    private String getMeaningfulExceptionClass(Throwable t)
    {
      return ((List<Throwable>) ExceptionUtils.getThrowableList(t))
          .stream()
          .map(x -> x.getClass().getName())
          .filter(x -> !shouldSkipException(x))
          .findFirst()
          .orElse(Exception.class.getName());
    }
  }
}
