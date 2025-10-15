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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class MSQTaskList
{
  private final List<String> taskIds;

  @JsonCreator
  public MSQTaskList(@JsonProperty("taskIds") List<String> taskIds)
  {
    this.taskIds = Preconditions.checkNotNull(taskIds, "taskIds");
  }

  @JsonProperty
  public List<String> getTaskIds()
  {
    return taskIds;
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
    MSQTaskList that = (MSQTaskList) o;
    return Objects.equals(taskIds, that.taskIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskIds);
  }

  @Override
  public String toString()
  {
    return "MSQTaskList{" +
           "taskIds=" + taskIds +
           '}';
  }
}
