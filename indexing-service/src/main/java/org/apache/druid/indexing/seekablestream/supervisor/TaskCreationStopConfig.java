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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Configuration class for controlling the transition of a Supervisor
 * to the UNHEALTHY_TASKS_STOP_CREATING_NEW state in the event of parse exceptions.
 */
public class TaskCreationStopConfig
{
  private final boolean enabled;
  private final Long noProgressTimeoutMillis;

  @JsonCreator
  public TaskCreationStopConfig(
      @Nullable @JsonProperty("enabled") Boolean enabled,
      @Nullable @JsonProperty("noProgressTimeoutMillis") Long noProgressTimeoutMillis
  )
  {
    Preconditions.checkArgument(
        noProgressTimeoutMillis == null || noProgressTimeoutMillis > 0,
        "noProgressTimeoutMillis should be a postive number"
    );
    this.enabled = enabled != null && enabled;
    this.noProgressTimeoutMillis = noProgressTimeoutMillis;
  }

  @JsonProperty
  public boolean isEnabled()
  {
    return this.enabled;
  }

  @JsonProperty
  public Long getNoProgressTimeoutMillis()
  {
    return this.noProgressTimeoutMillis;
  }

  @Override
  public String toString()
  {
    return "TaskCreationStopConfig{" +
           "enabled=" + enabled +
           ", noProgressTimeoutMillis=" + noProgressTimeoutMillis +
           '}';
  }
}
