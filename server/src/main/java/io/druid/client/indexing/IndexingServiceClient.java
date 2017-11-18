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
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.discovery.DruidLeaderClient;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.HashMap;
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

  public String compactSegments(
      List<DataSegment> segments,
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

    return runQuery(new ClientCompactQuery(dataSource, segments, tuningConfig, context));
  }

  private String runQuery(Object queryObject)
  {
    try {
      final FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.POST,
              "/druid/indexer/v1/task"
          ).setContent(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(queryObject))
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Failed to post query[%s]", queryObject);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final String taskId = (String) resultMap.get("task");
      return Preconditions.checkNotNull(taskId, "Null task id for query[%s]", queryObject);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public QueryStatus queryStatus(String queryId)
  {
    try {
      final FullResponseHolder response = druidLeaderClient.go(
          druidLeaderClient.makeRequest(
              HttpMethod.GET,
              StringUtils.format("/druid/indexer/v1/task/%s/status", queryId)
          )
      );

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Failed to get status for query[%s]", queryId);
      }

      final Map<String, Object> resultMap = jsonMapper.readValue(
          response.getContent(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      final Map<String, Object> statusMap = (Map<String, Object>) resultMap.get("status");
      final QueryStatus queryStatus = jsonMapper.convertValue(
          statusMap,
          new TypeReference<QueryStatus>() {}
      );
      Preconditions.checkState(queryStatus != null, "Null status for query[%s]", queryId);

      return queryStatus;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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
}
