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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class OverlordResourceTestClient
{
  private static final Logger LOG = new Logger(OverlordResourceTestClient.class);
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String indexer;
  private final StatusResponseHandler responseHandler;

  @Inject
  OverlordResourceTestClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.indexer = config.getIndexerUrl();
    this.responseHandler = new StatusResponseHandler(StandardCharsets.UTF_8);
  }

  private String getIndexerURL()
  {
    return StringUtils.format(
        "%s/druid/indexer/v1/",
        indexer
    );
  }

  public String submitTask(final String task)
  {
    try {
      return RetryUtils.retry(
          () -> {
            StatusResponseHolder response = httpClient.go(
                new Request(HttpMethod.POST, new URL(getIndexerURL() + "task"))
                    .setContent(
                        "application/json",
                        StringUtils.toUtf8(task)
                    ),
                responseHandler
            ).get();
            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
              throw new ISE(
                  "Error while submitting task to indexer response [%s %s]",
                  response.getStatus(),
                  response.getContent()
              );
            }
            Map<String, String> responseData = jsonMapper.readValue(
                response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
            );
            String taskID = responseData.get("task");
            LOG.info("Submitted task with TaskID[%s]", taskID);
            return taskID;
          },
          Predicates.alwaysTrue(),
          5
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public TaskState getTaskStatus(String taskID)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format(
              "%stask/%s/status",
              getIndexerURL(),
              StringUtils.urlEncode(taskID)
          )
      );

      LOG.info("Index status response" + response.getContent());
      TaskStatusResponse taskStatusResponse = jsonMapper.readValue(
          response.getContent(),
          new TypeReference<TaskStatusResponse>()
          {
          }
      );
      return taskStatusResponse.getStatus().getStatusCode();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public List<TaskResponseObject> getRunningTasks()
  {
    return getTasks("runningTasks");
  }

  public List<TaskResponseObject> getWaitingTasks()
  {
    return getTasks("waitingTasks");
  }

  public List<TaskResponseObject> getPendingTasks()
  {
    return getTasks("pendingTasks");
  }

  private List<TaskResponseObject> getTasks(String identifier)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format("%s%s", getIndexerURL(), identifier)
      );
      LOG.info("Tasks %s response %s", identifier, response.getContent());
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<TaskResponseObject>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void waitUntilTaskCompletes(final String taskID)
  {
    waitUntilTaskCompletes(taskID, 10000, 60);
  }

  public void waitUntilTaskCompletes(final String taskID, final int millisEach, final int numTimes)
  {
    RetryUtil.retryUntil(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call()
          {
            TaskState status = getTaskStatus(taskID);
            if (status == TaskState.FAILED) {
              throw new ISE("Indexer task FAILED");
            }
            return status == TaskState.SUCCESS;
          }
        },
        true,
        millisEach,
        numTimes,
        taskID
    );
  }

  public String submitSupervisor(String spec)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, new URL(getIndexerURL() + "supervisor"))
              .setContent(
                  "application/json",
                  StringUtils.toUtf8(spec)
              ),
          responseHandler
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while submitting supervisor to overlord, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      Map<String, String> responseData = jsonMapper.readValue(
          response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
      );
      String id = responseData.get("id");
      LOG.info("Submitted supervisor with id[%s]", id);
      return id;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void shutdownSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, new URL(StringUtils.format("%ssupervisor/%s/shutdown", getIndexerURL(), StringUtils.urlEncode(id)))),
          responseHandler
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while shutting down supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.info("Shutdown supervisor with id[%s]", id);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private StatusResponseHolder makeRequest(HttpMethod method, String url)
  {
    try {
      StatusResponseHolder response = this.httpClient
          .go(new Request(method, new URL(url)), responseHandler).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while making request to indexer [%s %s]", response.getStatus(), response.getContent());
      }
      return response;
    }
    catch (Exception e) {
      LOG.error(e, "Exception while sending request");
      throw Throwables.propagate(e);
    }
  }

}
