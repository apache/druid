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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public record WorkerHandoffResult(
    Map<String, List<Integer>> taskGroups,
    List<SkippedTask> skipped
)
{
  public static final String REASON_MISSING_TASK_GROUP_ID = "missing_taskGroupId";
  public static final String REASON_MISSING_SUPERVISOR_ID = "missing_supervisorId";
  public static final String REASON_SUPERVISOR_NOT_FOUND = "supervisor_not_found";
  public static final String REASON_HANDOFF_UNSUPPORTED = "handoff_unsupported";

  public WorkerHandoffResult
  {
    taskGroups = taskGroups == null ? Collections.emptyMap() : taskGroups;
    skipped = skipped == null ? Collections.emptyList() : skipped;
  }

  public record SkippedTask(
      @JsonProperty("id") String id,
      @JsonProperty("reason") String reason
  )
  {
    @JsonCreator
    public SkippedTask
    {
    }
  }
}
