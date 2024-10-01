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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexer.CompactionEngine;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This config must be bound on CliOverlord to enable running compaction supervisors
 * on the Overlord. When compaction supervisors are enabled, the Coordinator
 * does not run auto-compact duty.
 */
public class CompactionSupervisorConfig
{
  private static final CompactionSupervisorConfig DEFAULT = new CompactionSupervisorConfig(null, null);

  @JsonProperty
  private final boolean enabled;
  @JsonProperty
  private final CompactionEngine engine;

  public static CompactionSupervisorConfig defaultConfig()
  {
    return DEFAULT;
  }

  @JsonCreator
  public CompactionSupervisorConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("engine") @Nullable CompactionEngine engine
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.engine = Configs.valueOrDefault(engine, CompactionEngine.NATIVE);
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public CompactionEngine getEngine()
  {
    return engine;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompactionSupervisorConfig that = (CompactionSupervisorConfig) o;
    return enabled == that.enabled && engine == that.engine;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(enabled, engine);
  }

  @Override
  public String toString()
  {
    return "CompactionSchedulerConfig{" +
           "enabled=" + enabled +
           "engine=" + engine +
           '}';
  }
}
