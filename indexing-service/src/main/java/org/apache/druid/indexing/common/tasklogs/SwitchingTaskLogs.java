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
import com.google.inject.name.Named;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class SwitchingTaskLogs implements TaskLogs
{
  private final TaskLogs delegate;
  private final TaskLogs streamer;
  private final TaskLogs pusher;

  @Inject
  public SwitchingTaskLogs(
      @Named("delegate") TaskLogs delegate, 
      @Named("streamer") TaskLogs streamer,
      @Named("pusher") TaskLogs pusher
  )
  {
    this.delegate = delegate;
    this.streamer = streamer;
    this.pusher = pusher;
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
  {
    return streamer.streamTaskLog(taskid, offset);
  }

  @Override
  public Optional<InputStream> streamTaskReports(final String taskid) throws IOException
  {
    return delegate.streamTaskReports(taskid);
  }

  @Override
  public Optional<InputStream> streamTaskStatus(final String taskid) throws IOException
  {
    return delegate.streamTaskReports(taskid);
  }

  @Override
  public void pushTaskLog(String taskid, File logFile) throws IOException
  {
    pusher.pushTaskLog(taskid, logFile);
  }

  @Override
  public void pushTaskPayload(String taskid, File taskPayloadFile) throws IOException
  {
    delegate.pushTaskPayload(taskid, taskPayloadFile);
  }

  @Override
  public void killAll() throws IOException
  {
    delegate.killAll();
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    delegate.killOlderThan(timestamp);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    delegate.pushTaskReports(taskid, reportFile);
  }

  @Override
  public void pushTaskStatus(String taskid, File reportFile) throws IOException
  {
    delegate.pushTaskStatus(taskid, reportFile);
  }

  @Override
  public Optional<InputStream> streamTaskPayload(String taskid) throws IOException
  {
    return delegate.streamTaskPayload(taskid);
  }
}
