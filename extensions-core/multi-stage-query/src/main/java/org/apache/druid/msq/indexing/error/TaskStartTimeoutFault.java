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
import java.util.concurrent.TimeUnit;

@JsonTypeName(TaskStartTimeoutFault.CODE)
public class TaskStartTimeoutFault extends BaseMSQFault
{
  static final String CODE = "TaskStartTimeout";

  private final int numTasksNotStarted;
  private final int totalTasks;
  private final long timeout;

  @JsonCreator
  public TaskStartTimeoutFault(
      @JsonProperty("numTasksNotStarted") int numTasksNotStarted,
      @JsonProperty("totalTasks") int totalTasks,
      @JsonProperty("timeout") long timeout
  )
  {
    super(
        CODE,
        "Unable to launch [%d] workers out of the total [%d] worker tasks within [%,d] seconds of the last successful worker launch."
        + "There might be insufficient available slots to start all worker tasks simultaneously. "
        + "Try lowering '%s' in your query context to a number that fits within your available task capacity, "
        + "or try increasing capacity.",
        numTasksNotStarted,
        totalTasks,
        TimeUnit.MILLISECONDS.toSeconds(timeout),
        MultiStageQueryContext.CTX_MAX_NUM_TASKS
    );
    this.numTasksNotStarted = numTasksNotStarted;
    this.totalTasks = totalTasks;
    this.timeout = timeout;
  }

  @JsonProperty
  public int getNumTasksNotStarted()
  {
    return numTasksNotStarted;
  }

  @JsonProperty
  public int getTotalTasks()
  {
    return totalTasks;
  }

  @JsonProperty
  public long getTimeout()
  {
    return timeout;
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
    return numTasksNotStarted == that.numTasksNotStarted && totalTasks == that.totalTasks && timeout == that.timeout;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numTasksNotStarted, totalTasks, timeout);
  }


}
