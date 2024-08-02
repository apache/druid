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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;

import java.util.Map;

public class NoopFailingTask extends NoopTask
{
  private static final int DEFAULT_RUN_TIME = 2500;
  @JsonIgnore
  private final long runTime;

  public NoopFailingTask(String id, String groupId, String dataSource, long runTimeMillis, long isReadyTime, Map<String, Object> context)
  {
    super(id, groupId, dataSource, runTimeMillis, isReadyTime, context);
    this.runTime = (runTimeMillis == 0) ? DEFAULT_RUN_TIME : runTimeMillis;
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    Thread.sleep(runTime);
    return TaskStatus.failure(getId(), "Failed to complete the task");
  }
}
