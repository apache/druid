/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;

import java.util.List;

public class LockListAction implements TaskAction<List<TaskLock>>
{
  @Override
  public TypeReference<List<TaskLock>> getReturnTypeReference()
  {
    return new TypeReference<List<TaskLock>>() {};
  }

  @Override
  public List<TaskLock> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getTaskLockbox().findLocksForTask(task);
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "LockListAction{}";
  }
}
