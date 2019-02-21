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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  public void mergeSegments(List<DataSegment> segments)
  {
    final Iterator<DataSegment> segmentsIter = segments.iterator();
    if (!segmentsIter.hasNext()) {
      return;
    }

    final String dataSource = segmentsIter.next().getDataSource();
    while (segmentsIter.hasNext()) {
      DataSegment next = segmentsIter.next();
      if (!dataSource.equals(next.getDataSource())) {
        throw new IAE("Cannot merge segments of different dataSources[%s] and [%s]", dataSource, next.getDataSource());
      }
    }

    runTask(new ClientAppendQuery(dataSource, segments));
  }

  @Override
  public void killSegments(String dataSource, Interval interval)
  {
    runTask(new ClientKillQuery(dataSource, interval));
  }

  @Override
  public String compactSegments(
      List<DataSegment> segments,
      boolean keepSegmentGranularity,
      @Nullable Long targetCompactionSizeBytes,
      int compactionTaskPriority,
      @Nullable ClientCompactQueryTuningConfig tuningConfig,
      @Nullable Map<String, Object> context
  )
  {
    Preconditions.checkArgument(segments.size() > 1, "Expect two or more segments to compact");

    final String dataSource = segments.get(0).getDataSource();
    Preconditions.checkArgument(
        segments.stream().allMatch(segment -> segment.getDataSource().equals(dataSource)),
        "Segments must have the same dataSource"
    );

    context = context == null ? new HashMap<>() : context;
    context.put("priority", compactionTaskPriority);

    return runTask(
        new ClientCompactQuery(
            dataSource,
            segments,
            keepSegmentGranularity,
            targetCompactionSizeBytes,
            tuningConfig,
            context
        )
    );
  }

  @Override
  public String runTask(Object taskObject)
  {
    try {
      final FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.POST,
              "/druid/indexer/v1/task"
          ).setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(taskObject))
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Failed to post task[%s]", taskObject);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final String taskId = (String) resultMap.get("task");
      return Preconditions.checkNotNull(taskId, "Null task id for task[%s]", taskObject);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String killTask(String taskId)
  {
    try {
      final FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.POST,
              StringUtils.format(
                  "/druid/indexer/v1/task/%s/shutdown",
                  StringUtils.urlEncode(taskId)
              )
          )
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Failed to kill task[%s]", taskId);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final String killedTaskId = (String) resultMap.get("task");
      Preconditions.checkNotNull(killedTaskId, "Null task id returned for task[%s]", taskId);
      Preconditions.checkState(
          taskId.equals(killedTaskId),
          "Requested to kill task[%s], but another task[%s] was killed!",
          taskId,
          killedTaskId
      );
      return killedTaskId;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getTotalWorkerCapacity()
  {
    try {
      final FullResponseHolder response = druidLeaderClient.go(
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
  public List<TaskStatusPlus> getRunningTasks()
  {
    return getTasks("runningTasks");
  }

  @Override
  public List<TaskStatusPlus> getPendingTasks()
  {
    return getTasks("pendingTasks");
  }

  @Override
  public List<TaskStatusPlus> getWaitingTasks()
  {
    return getTasks("waitingTasks");
  }

  private List<TaskStatusPlus> getTasks(String endpointSuffix)
  {
    try {
      final FullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format("/druid/indexer/v1/%s", endpointSuffix))
      );

      if (!responseHolder.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while fetching the status of the last complete task");
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
      final FullResponseHolder responseHolder = druidLeaderClient.go(
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
      final FullResponseHolder responseHolder = druidLeaderClient.go(
          druidLeaderClient.makeRequest(HttpMethod.GET, StringUtils.format("/druid/indexer/v1/task/%s", taskId))
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
      final FullResponseHolder responseHolder = druidLeaderClient.go(
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
