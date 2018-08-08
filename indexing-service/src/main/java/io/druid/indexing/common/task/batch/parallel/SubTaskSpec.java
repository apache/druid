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

package io.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.InputSplit;
import io.druid.indexing.common.task.Task;

import java.util.Map;

public abstract class SubTaskSpec<T extends Task>
{
  private final String id;
  private final String groupId;
  private final String supervisorTaskId;
  private final Map<String, Object> context;
  private final InputSplit inputSplit;

  @JsonCreator
  public SubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit inputSplit
  )
  {
    this.id = id;
    this.groupId = groupId;
    this.supervisorTaskId = supervisorTaskId;
    this.context = context;
    this.inputSplit = inputSplit;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public String getSupervisorTaskId()
  {
    return supervisorTaskId;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @JsonProperty
  public InputSplit getInputSplit()
  {
    return inputSplit;
  }

  public abstract T newSubTask(int numAttempts);
}
