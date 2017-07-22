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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;

/**
*/
public class TaskRunnerTaskLogStreamer implements TaskLogStreamer
{
  private final TaskMaster taskMaster;

  @Inject
  public TaskRunnerTaskLogStreamer(final TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    final TaskRunner runner = taskMaster.getTaskRunner().orNull();
    if (runner instanceof TaskLogStreamer) {
      return ((TaskLogStreamer) runner).streamTaskLog(taskid, offset);
    } else {
      return Optional.absent();
    }
  }
}
