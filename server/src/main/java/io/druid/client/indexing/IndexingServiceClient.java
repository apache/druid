/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.discovery.DruidLeaderClient;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexingServiceClient
{
  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public IndexingServiceClient(
      ObjectMapper jsonMapper,
      @IndexingService DruidLeaderClient druidLeaderClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.druidLeaderClient = druidLeaderClient;
  }

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

    runQuery(new ClientAppendQuery(dataSource, segments));
  }

  public void killSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientKillQuery(dataSource, interval));
  }

  public void upgradeSegment(DataSegment dataSegment)
  {
    runQuery(new ClientConversionQuery(dataSegment));
  }

  public void upgradeSegments(String dataSource, Interval interval)
  {
    runQuery(new ClientConversionQuery(dataSource, interval));
  }

  public List<TaskStatusPlus> getRunningTasks()
  {
    return getTasks("runningTasks");
  }

  public List<TaskStatusPlus> getPendingTasks()
  {
    return getTasks("pendingTasks");
  }

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

  @Nullable
  public TaskStatusPlus getLastCompleteTask()
  {
    final List<TaskStatusPlus> completeTaskStatuses = getTasks("completeTasks?n=1");
    return completeTaskStatuses.isEmpty() ? null : completeTaskStatuses.get(0);
  }

  public int killPendingSegments(String dataSource, DateTime end)
  {
    final String endPoint = StringUtils.format(
        "/druid/indexer/v1/pendingSegments/%s?interval=%s",
        dataSource,
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

  private void runQuery(Object queryObject)
  {
    try {
      druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.POST,
              "/druid/indexer/v1/task"
          ).setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(queryObject))
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
