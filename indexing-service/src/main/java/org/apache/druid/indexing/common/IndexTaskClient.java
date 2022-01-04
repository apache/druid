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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

/**
 * Abstract class to communicate with index tasks via HTTP. This class
 * provides interfaces to serialize/deserialize data and send an HTTP request.
 *
 * This subclass assumes that tasks run within the Druid Indexer task
 * system.
 */
public abstract class IndexTaskClient extends TaskClient
{
  private static final String BASE_PATH = "/druid/worker/v1/chat";

  private final TaskInfoProvider taskInfoProvider;

  public IndexTaskClient(
      HttpClient httpClient,
      ObjectMapper objectMapper,
      TaskInfoProvider taskInfoProvider,
      Duration httpTimeout,
      String callerId,
      int numThreads,
      long numRetries
  )
  {
    super(httpClient, objectMapper, httpTimeout, callerId, numThreads, numRetries);
    this.taskInfoProvider = taskInfoProvider;
  }

  @Override
  protected String buildPath(String taskId, String encodedPathSuffix)
  {
    return StringUtils.format("%s/%s/%s", BASE_PATH, StringUtils.urlEncode(taskId), encodedPathSuffix);
  }

  @Override
  protected TaskLocation resolveLocation(String taskId)
  {
    Optional<TaskStatus> status = taskInfoProvider.getTaskStatus(taskId);
    if (!status.isPresent() || !status.get().isRunnable()) {
      throw new TaskNotRunnableException(
          StringUtils.format(
              "Aborting request because task [%s] is not runnable",
              taskId
          )
      );
    }

    return taskInfoProvider.getTaskLocation(taskId);
  }
}
