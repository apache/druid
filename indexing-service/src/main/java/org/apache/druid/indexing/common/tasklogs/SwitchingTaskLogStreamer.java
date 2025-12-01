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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Provides task logs based on a series of underlying task log providers.
 */
public class SwitchingTaskLogStreamer implements TaskLogStreamer
{
  private static final Logger log = new Logger(SwitchingTaskLogStreamer.class);
  
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
    log.info("🔀 [SWITCHING] streamTaskReports() called for task [%s]", taskid);
    IOException deferIOException = null;

    // Try task runner first (for live reports from running tasks)
    log.info("🔀 [SWITCHING] Trying task runner (live reports) for task [%s]", taskid);
    try {
      final Optional<InputStream> stream = taskRunnerTaskLogStreamer.streamTaskReports(taskid);
      if (stream.isPresent()) {
        log.info("✅ [SWITCHING] Task runner returned live reports for task [%s]", taskid);
        return stream;
      } else {
        log.info("🔀 [SWITCHING] Task runner returned Optional.absent() for task [%s] - will try deep storage", taskid);
      }
    }
    catch (IOException e) {
      // defer first IO exception due to race in the way tasks update their exit status in the overlord
      // It may happen that the task sent the report to deep storage but the task is still running with http chat handlers unregistered
      // In such a case, catch and ignore the 1st IOException and try deepStorage for the report. If the report is still not found, return the caught exception
      log.warn(e, "⚠️  [SWITCHING] Task runner threw IOException for task [%s] - will try deep storage fallback", taskid);
      deferIOException = e;
    }

    // Try deep storage (for completed tasks)
    log.info("🔀 [SWITCHING] Trying deep storage providers (%d configured) for task [%s]", deepStorageStreamers.size(), taskid);
    for (int i = 0; i < deepStorageStreamers.size(); i++) {
      TaskLogStreamer provider = deepStorageStreamers.get(i);
      log.info("🔀 [SWITCHING] Trying deep storage provider #%d (%s) for task [%s]", 
               i + 1, provider.getClass().getSimpleName(), taskid);
      try {
        final Optional<InputStream> stream = provider.streamTaskReports(taskid);
        if (stream.isPresent()) {
          log.info("✅ [SWITCHING] Deep storage provider #%d returned reports for task [%s]", i + 1, taskid);
          return stream;
        } else {
          log.info("🔀 [SWITCHING] Deep storage provider #%d returned Optional.absent() for task [%s]", i + 1, taskid);
        }
      }
      catch (IOException e) {
        log.error(e, "❌ [SWITCHING] Deep storage provider #%d failed for task [%s]", i + 1, taskid);
        if (deferIOException != null) {
          e.addSuppressed(deferIOException);
        }
        throw e;
      }
    }
    
    // Could not find any InputStream. Throw deferred exception if exists
    if (deferIOException != null) {
      log.warn("❌ [SWITCHING] All providers failed for task [%s], throwing deferred IOException from task runner", taskid);
      throw deferIOException;
    }
    
    log.warn("⚠️  [SWITCHING] No reports found for task [%s] from any provider - returning Optional.absent()", taskid);
    return Optional.absent();
  }
}
