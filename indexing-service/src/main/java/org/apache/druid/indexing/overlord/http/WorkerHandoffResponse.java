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

package org.apache.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.supervisor.WorkerHandoffResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 202 body for worker early-handoff. {@code taskGroups} were queued;
 * {@code skipped} are seekable-stream task ids that were not queued.
 * 202 means notices were accepted, not that handoff has finished.
 */
public record WorkerHandoffResponse(
    @JsonProperty("host") String host,
    @JsonProperty("taskGroups") Map<String, List<Integer>> taskGroups,
    @JsonProperty("skipped") List<WorkerHandoffResult.SkippedTask> skipped
)
{
  @JsonCreator
  public WorkerHandoffResponse
  {
    taskGroups = taskGroups == null ? Collections.emptyMap() : taskGroups;
    skipped = skipped == null ? Collections.emptyList() : skipped;
  }
}
