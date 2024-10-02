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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class CompactionSupervisorSpec implements SupervisorSpec
{
  public static final String TYPE = "autocompact";
  public static final String ID_PREFIX = "autocompact__";

  private final boolean suspended;
  private final DataSourceCompactionConfig spec;
  private final CompactionScheduler scheduler;
  private final CompactionConfigValidationResult validationResult;

  @JsonCreator
  public CompactionSupervisorSpec(
      @JsonProperty("spec") DataSourceCompactionConfig spec,
      @JsonProperty("suspended") @Nullable Boolean suspended,
      @JacksonInject CompactionScheduler scheduler
  )
  {
    this.spec = spec;
    this.suspended = Configs.valueOrDefault(suspended, false);
    this.scheduler = scheduler;
    this.validationResult = scheduler.validateCompactionConfig(spec);
  }

  @JsonProperty
  public DataSourceCompactionConfig getSpec()
  {
    return spec;
  }

  @Override
  @JsonProperty
  public boolean isSuspended()
  {
    return suspended;
  }

  @Override
  public String getId()
  {
    return ID_PREFIX + spec.getDataSource();
  }

  public CompactionConfigValidationResult getValidationResult()
  {
    return validationResult;
  }

  @Override
  public CompactionSupervisor createSupervisor()
  {
    return new CompactionSupervisor(this, scheduler);
  }

  @Override
  public List<String> getDataSources()
  {
    return Collections.singletonList(spec.getDataSource());
  }

  @Override
  public CompactionSupervisorSpec createSuspendedSpec()
  {
    return new CompactionSupervisorSpec(spec, true, scheduler);
  }

  @Override
  public CompactionSupervisorSpec createRunningSpec()
  {
    return new CompactionSupervisorSpec(spec, false, scheduler);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public String getSource()
  {
    return "";
  }
}
