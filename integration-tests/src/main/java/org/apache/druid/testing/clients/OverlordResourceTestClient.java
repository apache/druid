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
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.overlord.http.TaskPayloadResponse;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.joda.time.Interval;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class OverlordResourceTestClient
{
  private static final Logger LOG = new Logger(OverlordResourceTestClient.class);
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String indexer;

  @Inject
  OverlordResourceTestClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.indexer = config.getOverlordUrl();
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
                StatusResponseHandler.getInstance()
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
            LOG.debug("Submitted task with TaskID[%s]", taskID);
            return taskID;
          },
          Predicates.alwaysTrue(),
          5
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TaskStatusPlus getTaskStatus(String taskID)
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
      LOG.debug("Index status response" + response.getContent());
      TaskStatusResponse taskStatusResponse = jsonMapper.readValue(
          response.getContent(),
          new TypeReference<TaskStatusResponse>()
          {
          }
      );
      return taskStatusResponse.getStatus();
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<TaskResponseObject> getAllTasks()
  {
    return getTasks("tasks");
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

  public List<TaskResponseObject> getCompleteTasksForDataSource(final String dataSource)
  {
    return getTasks(StringUtils.format("tasks?state=complete&datasource=%s", StringUtils.urlEncode(dataSource)));
  }

  public List<TaskResponseObject> getUncompletedTasksForDataSource(final String dataSource)
  {
    List<TaskResponseObject> uncompletedTasks = new ArrayList<>();
    uncompletedTasks.addAll(getTasks(StringUtils.format("tasks?state=pending&datasource=%s", StringUtils.urlEncode(dataSource))));
    uncompletedTasks.addAll(getTasks(StringUtils.format("tasks?state=running&datasource=%s", StringUtils.urlEncode(dataSource))));
    uncompletedTasks.addAll(getTasks(StringUtils.format("tasks?state=waiting&datasource=%s", StringUtils.urlEncode(dataSource))));
    return uncompletedTasks;
  }

  private List<TaskResponseObject> getTasks(String identifier)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format("%s%s", getIndexerURL(), identifier)
      );
      LOG.debug("Tasks %s response %s", identifier, response.getContent());
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<TaskResponseObject>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TaskPayloadResponse getTaskPayload(String taskId)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format("%stask/%s", getIndexerURL(), StringUtils.urlEncode(taskId))
      );
      LOG.debug("Task %s response %s", taskId, response.getContent());
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<TaskPayloadResponse>()
          {
          }
      );
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getTaskLog(String taskId)
  {
    return getTaskLog(taskId, -88000);
  }

  public String getTaskLog(String taskId, long offsetValue)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format("%s%s", getIndexerURL(), StringUtils.format("task/%s/log?offset=%s", StringUtils.urlEncode(taskId), offsetValue))
      );
      return response.getContent();
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getTaskErrorMessage(String taskId)
  {
    return ((IngestionStatsAndErrorsTaskReportData) getTaskReport(taskId).get("ingestionStatsAndErrors").getPayload()).getErrorMsg();
  }

  public RowIngestionMetersTotals getTaskStats(String taskId)
  {
    try {
      Object buildSegment = ((IngestionStatsAndErrorsTaskReportData) getTaskReport(taskId).get("ingestionStatsAndErrors").getPayload()).getRowStats().get("buildSegments");
      return jsonMapper.convertValue(buildSegment, RowIngestionMetersTotals.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, IngestionStatsAndErrorsTaskReport> getTaskReport(String taskId)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format(
              "%s%s",
              getIndexerURL(),
              StringUtils.format("task/%s/reports", StringUtils.urlEncode(taskId))
          )
      );
      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, IngestionStatsAndErrorsTaskReport>>()
          {
          }
      );
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, List<Interval>> getLockedIntervals(Map<String, Integer> minTaskPriority)
  {
    try {
      String jsonBody = jsonMapper.writeValueAsString(minTaskPriority);

      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, new URL(getIndexerURL() + "lockedIntervals"))
              .setContent(
                  "application/json",
                  StringUtils.toUtf8(jsonBody)
              ),
          StatusResponseHandler.getInstance()
      ).get();
      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, List<Interval>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitUntilTaskCompletes(final String taskID)
  {
    waitUntilTaskCompletes(taskID, ITRetryUtil.DEFAULT_RETRY_SLEEP, ITRetryUtil.DEFAULT_RETRY_COUNT);
  }

  public void waitUntilTaskCompletes(final String taskID, final long millisEach, final int numTimes)
  {
    ITRetryUtil.retryUntil(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call()
          {
            TaskState status = getTaskStatus(taskID).getStatusCode();
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

  public void waitUntilTaskFails(final String taskID)
  {
    waitUntilTaskFails(taskID, 10000, 60);
  }


  public void waitUntilTaskFails(final String taskID, final long millisEach, final int numTimes)
  {
    ITRetryUtil.retryUntil(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call()
          {
            TaskState status = getTaskStatus(taskID).getStatusCode();
            if (status == TaskState.SUCCESS) {
              throw new ISE("Indexer task SUCCEED");
            }
            return status == TaskState.FAILED;
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
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while submitting supervisor to overlord, response [%s: %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      Map<String, String> responseData = jsonMapper.readValue(
          response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
      );
      String id = responseData.get("id");
      LOG.debug("Submitted supervisor with id[%s]", id);
      return id;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdownSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/shutdown",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while shutting down supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Shutdown supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void terminateSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/terminate",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while terminating supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Terminate supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdownTask(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%stask/%s/shutdown",
                      getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while shutdown task, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Shutdown task with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SupervisorStateManager.BasicState getSupervisorStatus(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/status",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while getting supervisor status, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      Map<String, Object> responseData = jsonMapper.readValue(
          response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      Map<String, Object> payload = jsonMapper.convertValue(
          responseData.get("payload"),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      String state = (String) payload.get("state");
      LOG.debug("Supervisor id[%s] has state [%s]", id, state);
      return SupervisorStateManager.BasicState.valueOf(state);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void suspendSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/suspend",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while suspending supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Suspended supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void statsSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/stats",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while stats supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("stats supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void getSupervisorHealth(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/health",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while get supervisor health, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("get supervisor health with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void resumeSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/resume",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while resuming supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Resumed supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void resetSupervisor(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/reset",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while resetting supervisor, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      LOG.debug("Reset supervisor with id[%s]", id);
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Object> getSupervisorHistory(String id)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%ssupervisor/%s/history",
                  getIndexerURL(),
                  StringUtils.urlEncode(id)
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while getting supervisor status, response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      List<Object> responseData = jsonMapper.readValue(
          response.getContent(), new TypeReference<List<Object>>()
          {
          }
      );
      return responseData;
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private StatusResponseHolder makeRequest(HttpMethod method, String url)
  {
    try {
      StatusResponseHolder response = this.httpClient
          .go(new Request(method, new URL(url)), StatusResponseHandler.getInstance()).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while making request to indexer [%s %s]", response.getStatus(), response.getContent());
      }
      return response;
    }
    catch (ISE e) {
      LOG.error("Exception while sending request: %s", e.getMessage());
      throw e;
    }
    catch (Exception e) {
      LOG.error(e, "Exception while sending request");
      throw new RuntimeException(e);
    }
  }

}
