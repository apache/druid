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
 * Defines if and when {@link SeekableStreamSupervisor} can become idle.
 */
public class IdleConfig
{
  private final boolean enabled;
  private final Long inactiveAfterMillis;

  @JsonCreator
  public IdleConfig(
      @Nullable @JsonProperty("enabled") Boolean enabled,
      @Nullable @JsonProperty("inactiveAfterMillis") Long inactiveAfterMillis
  )
  {
    Preconditions.checkArgument(
        inactiveAfterMillis == null || inactiveAfterMillis > 0,
        "inactiveAfterMillis should be a postive number"
    );
    this.enabled = enabled != null && enabled;
    this.inactiveAfterMillis = inactiveAfterMillis;
  }

  @JsonProperty
  public boolean isEnabled()
  {
    return this.enabled;
  }

  @JsonProperty
  public Long getInactiveAfterMillis()
  {
    return this.inactiveAfterMillis;
  }

  @Override
  public String toString()
  {
    return "idleConfig{" +
           "enabled=" + enabled +
           ", inactiveAfterMillis=" + inactiveAfterMillis +
           '}';
  }
}
