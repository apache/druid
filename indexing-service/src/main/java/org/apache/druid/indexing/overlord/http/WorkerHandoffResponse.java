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
import org.apache.druid.indexing.overlord.WorkerHandoffHelper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 202 body for worker early-handoff. {@code taskGroups} were queued;
 * {@code skipped} are seekable-stream task ids that were not queued.
 * 202 means notices were accepted, not that handoff has finished.
 */
public class WorkerHandoffResponse
{
  private final String host;
  private final Map<String, List<Integer>> taskGroups;
  private final List<WorkerHandoffHelper.SkippedTask> skipped;

  @JsonCreator
  public WorkerHandoffResponse(
      @JsonProperty("host") final String host,
      @JsonProperty("taskGroups") final Map<String, List<Integer>> taskGroups,
      @JsonProperty("skipped") final List<WorkerHandoffHelper.SkippedTask> skipped
  )
  {
    this.host = host;
    this.taskGroups = taskGroups == null ? Collections.emptyMap() : taskGroups;
    this.skipped = skipped == null ? Collections.emptyList() : skipped;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public Map<String, List<Integer>> getTaskGroups()
  {
    return taskGroups;
  }

  @JsonProperty
  public List<WorkerHandoffHelper.SkippedTask> getSkipped()
  {
    return skipped;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WorkerHandoffResponse that = (WorkerHandoffResponse) o;
    return Objects.equals(host, that.host)
           && Objects.equals(taskGroups, that.taskGroups)
           && Objects.equals(skipped, that.skipped);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, taskGroups, skipped);
  }
}
