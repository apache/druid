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

package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;

public class NoopTaskLogs implements TaskLogs
{
  private final Logger log = new Logger(TaskLogs.class);

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset)
  {
    return Optional.absent();
  }

  @Override
  public void pushTaskLog(String taskid, File logFile)
  {
    log.info("Not pushing logs for task: %s", taskid);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile)
  {
    log.info("Not pushing reports for task: %s", taskid);
  }

  @Override
  public void killAll()
  {
    log.info("Noop: No task logs are deleted.");
  }

  @Override
  public void killOlderThan(long timestamp)
  {
    log.info("Noop: No task logs are deleted.");
  }
}
