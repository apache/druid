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

package org.apache.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.netty.handler.timeout.TimeoutException;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.inject.Inject;
import java.net.URL;
import java.util.concurrent.ExecutionException;

public class WorkerTaskRunnerQueryAdapter
{
  private static final EmittingLogger log = new EmittingLogger(HttpRemoteTaskRunner.class);

  private final TaskMaster taskMaster;
  private final HttpClient httpClient;

  @Inject
  public WorkerTaskRunnerQueryAdapter(TaskMaster taskMaster, @EscalatedGlobal final HttpClient httpClient)
  {
    this.taskMaster = taskMaster;
    this.httpClient = httpClient;
  }

  public void enableWorker(String host)
  {
    sendRequestToWorker(host, WorkerTaskRunner.ActionType.ENABLE);
  }

  public void disableWorker(String host)
  {
    sendRequestToWorker(host, WorkerTaskRunner.ActionType.DISABLE);
  }

  private void sendRequestToWorker(String workerHost, WorkerTaskRunner.ActionType action)
  {
    WorkerTaskRunner workerTaskRunner = getWorkerTaskRunner();

    if (workerTaskRunner == null) {
      throw new RE("Task Runner does not support enable/disable worker actions");
    }

    Optional<ImmutableWorkerInfo> workerInfo = Iterables.tryFind(
        workerTaskRunner.getWorkers(),
        entry -> entry.getWorker()
                      .getHost()
                      .equals(workerHost)
    );

    if (!workerInfo.isPresent()) {
      throw new RE(
          "Worker on host %s does not exists",
          workerHost
      );
    }

    String actionName = WorkerTaskRunner.ActionType.ENABLE.equals(action) ? "enable" : "disable";
    final URL workerUrl = TaskRunnerUtils.makeWorkerURL(
        workerInfo.get().getWorker(),
        "/druid/worker/v1/%s",
        actionName
    );

    try {
      final StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, workerUrl),
          StatusResponseHandler.getInstance()
      ).get();

      log.info(
          "Sent %s action request to worker: %s, status: %s, response: %s",
          action,
          workerHost,
          response.getStatus(),
          response.getContent()
      );

      if (!HttpResponseStatus.OK.equals(response.getStatus())) {
        throw new RE(
            "Action [%s] failed for worker [%s] with status %s(%s)",
            action,
            workerHost,
            response.getStatus().getCode(),
            response.getStatus().getReasonPhrase()
        );
      }
    }
    catch (ExecutionException | InterruptedException | TimeoutException e) {
      Throwables.propagate(e);
    }
  }

  private WorkerTaskRunner getWorkerTaskRunner()
  {
    Optional<TaskRunner> taskRunnerOpt = taskMaster.getTaskRunner();
    if (taskRunnerOpt.isPresent() && taskRunnerOpt.get() instanceof WorkerTaskRunner) {
      return (WorkerTaskRunner) taskRunnerOpt.get();
    } else {
      return null;
    }
  }
}
