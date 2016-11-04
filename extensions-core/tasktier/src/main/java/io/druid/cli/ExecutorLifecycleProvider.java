/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.guice.annotations.Json;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class ExecutorLifecycleProvider implements Provider<ExecutorLifecycle>
{
  private static final Logger log = new Logger(ExecutorLifecycleProvider.class);
  private final TaskActionClientFactory taskActionClientFactory;
  private final TaskRunner taskRunner;
  private final TaskConfig taskConfig;
  private final InputStream parentStream;
  private final ObjectMapper jsonMapper;

  @Inject
  public ExecutorLifecycleProvider(
      final TaskActionClientFactory taskActionClientFactory,
      final TaskRunner taskRunner,
      final TaskConfig taskConfig,
      final ParentMonitorInputStreamFaker parentStream,
      final @Json ObjectMapper jsonMapper
  )
  {
    this.taskActionClientFactory = taskActionClientFactory;
    this.taskRunner = taskRunner;
    this.taskConfig = taskConfig;
    this.parentStream = parentStream;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ExecutorLifecycle get()
  {
    final File statusFile = Paths.get(TierLocalTaskRunner.STATUS_FILE_NAME).toFile();
    final File taskFile = Paths.get(TierLocalTaskRunner.TASK_FILE_NAME).toFile();
    try {
      if (!statusFile.exists() && !statusFile.createNewFile()) {
        throw new IOException(String.format("Could not create file [%s]", statusFile));
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    log.info("Using status and task files: [%s] [%s]", statusFile, taskFile);
    return new ExecutorLifecycle(
        new ExecutorLifecycleConfig()
        {
          // This stream closes whenever the parent dies.
          @Override
          public InputStream getParentStream()
          {
            return parentStream;
          }
        }
            .setStatusFile(statusFile)
            .setTaskFile(taskFile),
        taskConfig,
        taskActionClientFactory,
        taskRunner,
        jsonMapper
    );
  }
}
