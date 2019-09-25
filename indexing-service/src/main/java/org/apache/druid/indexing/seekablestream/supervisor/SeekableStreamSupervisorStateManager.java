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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.common.StreamException;

public class SeekableStreamSupervisorStateManager extends SupervisorStateManager
{
  public enum SeekableStreamState implements State
  {
    UNABLE_TO_CONNECT_TO_STREAM(false, true),
    LOST_CONTACT_WITH_STREAM(false, false),

    CONNECTING_TO_STREAM(true, true),
    DISCOVERING_INITIAL_TASKS(true, true),
    CREATING_TASKS(true, true);

    private final boolean healthy;
    private final boolean firstRunOnly;

    SeekableStreamState(boolean healthy, boolean firstRunOnly)
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

    @Override
    public State getBasicState()
    {
      return healthy ? BasicState.RUNNING : BasicState.UNHEALTHY_SUPERVISOR;
    }
  }

  public SeekableStreamSupervisorStateManager(SupervisorStateManagerConfig supervisorConfig, boolean suspended)
  {
    super(supervisorConfig, suspended);
  }

  @Override
  protected State getSpecificUnhealthySupervisorState()
  {
    ExceptionEvent event = getRecentEventsQueue().getLast();
    if (event instanceof SeekableStreamExceptionEvent && ((SeekableStreamExceptionEvent) event).isStreamException()) {
      return isAtLeastOneSuccessfulRun()
             ? SeekableStreamState.LOST_CONTACT_WITH_STREAM
             : SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM;
    }

    return BasicState.UNHEALTHY_SUPERVISOR;
  }

  @Override
  protected ExceptionEvent buildExceptionEvent(Throwable t)
  {
    return new SeekableStreamExceptionEvent(t, isStoreStackTrace());
  }

  public static class SeekableStreamExceptionEvent extends ExceptionEvent
  {
    private final boolean streamException;

    public SeekableStreamExceptionEvent(Throwable t, boolean storeStackTrace)
    {
      super(t, storeStackTrace);

      this.streamException = ExceptionUtils.indexOfType(t, StreamException.class) != -1;
    }

    @JsonProperty
    public boolean isStreamException()
    {
      return streamException;
    }

    @Override
    protected boolean shouldSkipException(String className)
    {
      return RuntimeException.class.getName().equals(className) || StreamException.class.getName().equals(className);
    }
  }
}
