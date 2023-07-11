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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Provides task logs based on a series of underlying task log providers.
 */
public class SwitchingTaskLogStreamer implements TaskLogStreamer
{
  private final TaskLogStreamer taskRunnerTaskLogStreamer;
  private final List<TaskLogStreamer> deepStorageStreamers;

  @Inject
  public SwitchingTaskLogStreamer(
      @Named("taskstreamer") TaskLogStreamer taskRunnerTaskLogStreamer,
      List<TaskLogStreamer> deepStorageStreamer
  )
  {
    this.taskRunnerTaskLogStreamer = taskRunnerTaskLogStreamer;
    this.deepStorageStreamers = ImmutableList.copyOf(deepStorageStreamer);
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
  {
    IOException deferIOException = null;
    try {
      final Optional<InputStream> stream = taskRunnerTaskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return stream;
      }
    }
    catch (IOException e) {
      // defer first IO exception due to race in the way tasks update their exit status in the overlord
      // It may happen that the task sent the log to deep storage but is still running with http chat handlers unregistered
      // In such a case, catch and ignore the 1st IOException and try deepStorage for the log. If the log is still not found, return the caught exception
      deferIOException = e;
    }

    for (TaskLogStreamer provider : deepStorageStreamers) {
      try {
        final Optional<InputStream> stream = provider.streamTaskLog(taskid, offset);
        if (stream.isPresent()) {
          return stream;
        }
      }
      catch (IOException e) {
        if (deferIOException != null) {
          e.addSuppressed(deferIOException);
        }
        throw e;
      }
    }
    // Could not find any InputStream. Throw deferred exception if exists
    if (deferIOException != null) {
      throw deferIOException;
    }
    return Optional.absent();
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    IOException deferIOException = null;

    try {
      final Optional<InputStream> stream = taskRunnerTaskLogStreamer.streamTaskReports(taskid);
      if (stream.isPresent()) {
        return stream;
      }
    }
    catch (IOException e) {
      // defer first IO exception due to race in the way tasks update their exit status in the overlord
      // It may happen that the task sent the report to deep storage but the task is still running with http chat handlers unregistered
      // In such a case, catch and ignore the 1st IOException and try deepStorage for the report. If the report is still not found, return the caught exception
      deferIOException = e;
    }

    for (TaskLogStreamer provider : deepStorageStreamers) {
      try {
        final Optional<InputStream> stream = provider.streamTaskReports(taskid);
        if (stream.isPresent()) {
          return stream;
        }
      }
      catch (IOException e) {
        if (deferIOException != null) {
          e.addSuppressed(deferIOException);
        }
        throw e;
      }
    }
    // Could not find any InputStream. Throw deferred exception if exists
    if (deferIOException != null) {
      throw deferIOException;
    }
    return Optional.absent();
  }
}
