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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.msq.util.MultiStageQueryContext;

import java.util.Objects;

@JsonTypeName(TaskStartTimeoutFault.CODE)
public class TaskStartTimeoutFault extends BaseMSQFault
{
  static final String CODE = "TaskStartTimeout";

  private final int numTasks;

  @JsonCreator
  public TaskStartTimeoutFault(@JsonProperty("numTasks") int numTasks)
  {
    super(
        CODE,
        "Unable to launch all the worker tasks in time. There might be insufficient available slots to start all the worker tasks simultaneously."
        + " Try lowering '%s' in your query context to lower than [%d] tasks, or increasing capacity.",
        MultiStageQueryContext.CTX_MAX_NUM_TASKS,
        numTasks
    );
    this.numTasks = numTasks;
  }

  @JsonProperty
  public int getNumTasks()
  {
    return numTasks;
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
    if (!super.equals(o)) {
      return false;
    }
    TaskStartTimeoutFault that = (TaskStartTimeoutFault) o;
    return numTasks == that.numTasks;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numTasks);
  }
}
