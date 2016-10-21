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

package io.druid.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;

import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;

public class NoopTaskLogs implements TaskLogs
{
  private final Logger log = new Logger(TaskLogs.class);

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    return Optional.absent();
  }

  @Override
  public void pushTaskLog(String taskid, File logFile) throws IOException
  {
    log.info("Not pushing logs for task: %s", taskid);
  }
}
