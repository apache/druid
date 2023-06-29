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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LegacySinglePhaseSubTask extends SinglePhaseSubTask
{
  @JsonCreator
  public LegacySinglePhaseSubTask(
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("groupId") final String groupId,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("supervisorTaskId") final String supervisorTaskId,
      @JsonProperty("numAttempts") final int numAttempts, // zero-based counting
      @JsonProperty("spec") final ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        id,
        groupId,
        taskResource,
        supervisorTaskId,
        null,
        numAttempts,
        ingestionSchema,
        context
    );
  }

  @Override
  public String getType()
  {
    return SinglePhaseSubTask.OLD_TYPE_NAME;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    if (getIngestionSchema().getIOConfig().getFirehoseFactory() != null) {
      throw getInputSecurityOnFirehoseUnsupportedError();
    }
    return getIngestionSchema().getIOConfig().getInputSource() != null ?
           getIngestionSchema().getIOConfig().getInputSource().getTypes()
                               .stream()
                               .map(i -> new ResourceAction(new Resource(i, ResourceType.EXTERNAL), Action.READ))
                               .collect(Collectors.toSet()) :
           ImmutableSet.of();
  }
}
