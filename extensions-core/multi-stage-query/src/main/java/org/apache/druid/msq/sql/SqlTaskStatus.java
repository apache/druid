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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.query.QueryException;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Response object for {@link SqlTaskResource#doPost}.
 */
public class SqlTaskStatus
{
  private final String taskId;
  private final TaskState state;
  @Nullable
  private final QueryException error;

  @JsonCreator
  public SqlTaskStatus(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("state") final TaskState state,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.error = error;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public TaskState getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public QueryException getError()
  {
    return error;
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
    SqlTaskStatus response = (SqlTaskStatus) o;
    return Objects.equals(taskId, response.taskId)
           && state == response.state
           && Objects.equals(error, response.error);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, state, error);
  }

  @Override
  public String toString()
  {
    return "SqlTaskStatus{" +
           "taskId='" + taskId + '\'' +
           ", state=" + state +
           ", error=" + error +
           '}';
  }
}
