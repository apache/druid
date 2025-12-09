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

package org.apache.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.io.InputStream;

/**
*/
public class TaskRunnerTaskLogStreamer implements TaskLogStreamer
{
  private static final Logger log = new Logger(TaskRunnerTaskLogStreamer.class);
  private final TaskMaster taskMaster;

  @Inject
  public TaskRunnerTaskLogStreamer(final TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
  {
    log.info("🔧 [RUNNER-WRAPPER] streamTaskLog() called for task [%s], offset [%d]", taskid, offset);
    
    final TaskRunner runner = taskMaster.getTaskRunner().orNull();
    
    if (runner == null) {
      log.warn("⚠️  [RUNNER-WRAPPER] TaskRunner is NULL for task [%s] - returning Optional.absent()", taskid);
      return Optional.absent();
    }
    
    log.info("🔧 [RUNNER-WRAPPER] TaskRunner class: [%s] for task [%s]", runner.getClass().getName(), taskid);
    
    if (runner instanceof TaskLogStreamer) {
      log.info("✅ [RUNNER-WRAPPER] Runner implements TaskLogStreamer - delegating streamTaskLog() for task [%s]", taskid);
      return ((TaskLogStreamer) runner).streamTaskLog(taskid, offset);
    } else {
      log.warn("⚠️  [RUNNER-WRAPPER] Runner does NOT implement TaskLogStreamer for task [%s] - returning Optional.absent()", taskid);
      return Optional.absent();
    }
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskId) throws IOException
  {
    log.info("🔧 [RUNNER-WRAPPER] streamTaskReports() called for task [%s]", taskId);
    
    final TaskRunner runner = taskMaster.getTaskRunner().orNull();
    
    if (runner == null) {
      log.warn("⚠️  [RUNNER-WRAPPER] TaskRunner is NULL for task [%s] - returning Optional.absent()", taskId);
      return Optional.absent();
    }
    
    log.info("🔧 [RUNNER-WRAPPER] TaskRunner class: [%s] for task [%s]", runner.getClass().getName(), taskId);
    log.info("🔧 [RUNNER-WRAPPER] Checking if runner implements TaskLogStreamer: %s", (runner instanceof TaskLogStreamer));
    
    if (runner instanceof TaskLogStreamer) {
      log.info("✅ [RUNNER-WRAPPER] Runner implements TaskLogStreamer - delegating streamTaskReports() for task [%s]", taskId);
      return ((TaskLogStreamer) runner).streamTaskReports(taskId);
    } else {
      log.warn("⚠️  [RUNNER-WRAPPER] Runner does NOT implement TaskLogStreamer for task [%s] - returning Optional.absent()", taskId);
      return Optional.absent();
    }
  }
}
