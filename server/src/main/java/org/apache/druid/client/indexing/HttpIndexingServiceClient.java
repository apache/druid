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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HttpIndexingServiceClient implements IndexingServiceClient
{
  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public HttpIndexingServiceClient(
      ObjectMapper jsonMapper,
      @IndexingService DruidLeaderClient druidLeaderClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.druidLeaderClient = druidLeaderClient;
  }

  @Override
  public void killUnusedSegments(String dataSource, Interval interval)
  {
    runTask(new ClientKillUnusedSegmentsTaskQuery(dataSource, interval));
  }

  @Override
  public String compactSegments(
      List<DataSegment> segments,
      int compactionTaskPriority,
      ClientCompactionTaskQueryTuningConfig tuningConfig,
      @Nullable Map<String, Object> context
  )
  {
    Preconditions.checkArgument(!segments.isEmpty(), "Expect non-empty segments to compact");

    final String dataSource = segments.get(0).getDataSource();
    Preconditions.checkArgument(
        segments.stream().allMatch(segment -> segment.getDataSource().equals(dataSource)),
        "Segments must have the same dataSource"
    );

    context = context == null ? new HashMap<>() : context;
    context.put("priority", compactionTaskPriority);

    return runTask(
        new ClientCompactionTaskQuery(
            dataSource,
            new ClientCompactionIOConfig(ClientCompactionIntervalSpec.fromSegments(segments)),
            tuningConfig,
            context
        )
    );
  }

  @Override
  public String runTask(Object taskObject)
  {
    try {
      // Warning, magic: here we may serialize ClientTaskQuery objects, but OverlordResource.taskPost() deserializes
      // Task objects from the same data. See the comment for ClientTaskQuery for details.
      final StringFullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/task")
                           .setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(taskObject))
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        if (!Strings.isNullOrEmpty(response.getContent())) {
          throw new ISE(
              "Failed to post task[%s] with error[%s].",
              taskObject,
              response.getContent()
          );
        } else {
          throw new ISE("Failed to post task[%s]. Please check overlord log", taskObject);
        }
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final String taskId = (String) resultMap.get("task");
      return Preconditions.checkNotNull(taskId, "Null task id for task[%s]", taskObject);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String cancelTask(String taskId)
  {
    try {
      final StringFullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.POST,
              StringUtils.format("/druid/indexer/v1/task/%s/shutdown", StringUtils.urlEncode(taskId))
          )
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Failed to cancel task[%s]", taskId);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final String cancelledTaskId = (String) resultMap.get("task");
      Preconditions.checkNotNull(cancelledTaskId, "Null task id returned for task[%s]", taskId);
      Preconditions.checkState(
          taskId.equals(cancelledTaskId),
          "Requested to cancel task[%s], but another task[%s] was cancelled!",
          taskId,
          cancelledTaskId
      );
      return cancelledTaskId;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getTotalWorkerCapacity()
  {
    try {
      final StringFullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/workers")
                           .setHeader("Content-Type", MediaType.APPLICATION_JSON)
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while getting available cluster capacity. status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }
      final Collection<IndexingWorkerInfo> workers = jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Collection<IndexingWorkerInfo>>() {}
      );

      return workers.stream().mapToInt(workerInfo -> workerInfo.getWorker().getCapacity()).sum();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<TaskStatusPlus> getActiveTasks()
  {
    // Must retrieve waiting, then pending, then running, so if tasks move from one state to the next between
    // calls then we still catch them. (Tasks always go waiting -> pending -> running.)
    //
    // Consider switching to new-style /druid/indexer/v1/tasks API in the future.
    final List<TaskStatusPlus> tasks = new ArrayList<>();
    final Set<String> taskIdsSeen = new HashSet<>();

    final Iterable<TaskStatusPlus> activeTasks = Iterables.concat(
        getTasks("waitingTasks"),
        getTasks("pendingTasks"),
        getTasks("runningTasks")
    );

    for (TaskStatusPlus task : activeTasks) {
      // Use taskIdsSeen to prevent returning the same task ID more than once (if it hops from 'pending' to 'running',
      // for example, and we see it twice.)
      if (taskIdsSeen.add(task.getId())) {
        tasks.add(task);
      }
    }

    return tasks;
  }

  private List<TaskStatusPlus> getTasks(String endpointSuffix)
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format("/druid/indexer/v1/%s", endpointSuffix))
      );

      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while fetching the status of tasks");
      }

      return jsonMapper.readValue(
          responseHolder.getContent(),
          new TypeReference<List<TaskStatusPlus>>()
          {
          }
      );
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TaskStatusResponse getTaskStatus(String taskId)
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format(
              "/druid/indexer/v1/task/%s/status",
              StringUtils.urlEncode(taskId)
          ))
      );

      return jsonMapper.readValue(
          responseHolder.getContent(),
          new TypeReference<TaskStatusResponse>()
          {
          }
      );
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, TaskStatus> getTaskStatuses(Set<String> taskIds) throws InterruptedException
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/taskStatus")
                           .setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(taskIds))
      );

      return jsonMapper.readValue(
          responseHolder.getContent(),
          new TypeReference<Map<String, TaskStatus>>()
          {
          }
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nullable
  public TaskStatusPlus getLastCompleteTask()
  {
    final List<TaskStatusPlus> completeTaskStatuses = getTasks("completeTasks?n=1");
    return completeTaskStatuses.isEmpty() ? null : completeTaskStatuses.get(0);
  }

  @Override
  public TaskPayloadResponse getTaskPayload(String taskId)
  {
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.GET,
              StringUtils.format("/druid/indexer/v1/task/%s", StringUtils.urlEncode(taskId))
          )
      );

      return jsonMapper.readValue(
          responseHolder.getContent(),
          new TypeReference<TaskPayloadResponse>()
          {
          }
      );
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int killPendingSegments(String dataSource, DateTime end)
  {
    final String endPoint = StringUtils.format(
        "/druid/indexer/v1/pendingSegments/%s?interval=%s",
        StringUtils.urlEncode(dataSource),
        new Interval(DateTimes.MIN, end)
    );
    try {
      final StringFullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.DELETE, endPoint)
      );

      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while killing pendingSegments of dataSource[%s] created until [%s]", dataSource, end);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          responseHolder.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      final Object numDeletedObject = resultMap.get("numDeleted");
      return (Integer) Preconditions.checkNotNull(numDeletedObject, "numDeletedObject");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
