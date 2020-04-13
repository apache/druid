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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Task;

public class TestTasks
{
  private static final String DATASOURCE = "dummyDs";

  public static void registerSubtypes(ObjectMapper mapper)
  {
    mapper.registerSubtypes(ImmediateSuccessTask.class, UnendingTask.class);
  }

  public static Task immediateSuccess(String id)
  {
    return new ImmediateSuccessTask(id);
  }

  public static Task unending(String id)
  {
    return new UnendingTask(id);
  }

  @JsonTypeName("immediateSuccess")
  public static class ImmediateSuccessTask extends AbstractTask
  {
    @JsonCreator
    public ImmediateSuccessTask(@JsonProperty("id") String id)
    {
      super(id, DATASOURCE, null);
    }

    @Override
    public String getType()
    {
      return "immediateSuccess";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox)
    {
      return TaskStatus.success(getId());
    }
  }

  @JsonTypeName("unending")
  public static class UnendingTask extends AbstractTask
  {
    @JsonCreator
    public UnendingTask(@JsonProperty("id") String id)
    {
      super(id, DATASOURCE, null);
    }

    @Override
    public String getType()
    {
      return "unending";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      while (!Thread.currentThread().isInterrupted()) {
        Thread.sleep(1000);
      }

      return TaskStatus.failure(getId());
    }
  }
}
