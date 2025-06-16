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

package org.apache.druid.rpc.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Production implementation of {@link OverlordClient}.
 */
public class OverlordClientImpl implements OverlordClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public OverlordClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = Preconditions.checkNotNull(client, "client");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
  }

  @Override
  public ListenableFuture<URI> findCurrentLeader()
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/leader"),
            new StringFullResponseHandler(StandardCharsets.UTF_8)
        ),
        holder -> {
          try {
            return new URI(holder.getContent());
          }
          catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }
    );
  }

  @Override
  public ListenableFuture<Void> runTask(final String taskId, final Object taskObject)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/task")
                .jsonContent(jsonMapper, taskObject),
            new BytesFullResponseHandler()
        ),
        holder -> {
          final Map<String, Object> map =
              JacksonUtils.readValue(jsonMapper, holder.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
          final String returnedTaskId = (String) map.get("task");

          Preconditions.checkState(
              taskId.equals(returnedTaskId),
              "Got a different taskId[%s]. Expected taskId[%s]",
              returnedTaskId,
              taskId
          );

          return null;
        }
    );
  }

  @Override
  public ListenableFuture<Void> cancelTask(final String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/shutdown", StringUtils.urlEncode(taskId));

    return client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, path),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
      @Nullable String state,
      @Nullable String dataSource,
      @Nullable Integer maxCompletedTasks
  )
  {
    final StringBuilder pathBuilder = new StringBuilder("/druid/indexer/v1/tasks");
    int params = 0;

    if (state != null) {
      pathBuilder.append('?').append("state=").append(StringUtils.urlEncode(state));
      params++;
    }

    if (dataSource != null) {
      pathBuilder.append(params == 0 ? '?' : '&').append("datasource=").append(StringUtils.urlEncode(dataSource));
      params++;
    }

    if (maxCompletedTasks != null) {
      pathBuilder.append(params == 0 ? '?' : '&').append("max=").append(maxCompletedTasks);
    }

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, pathBuilder.toString()),
            new InputStreamResponseHandler()
        ),
        in -> asJsonParserIterator(in, TaskStatusPlus.class)
    );
  }

  @Override
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(final Set<String> taskIds)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/taskStatus")
                .jsonContent(jsonMapper, taskIds),
            new BytesFullResponseHandler()
        ),
        holder ->
            JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<TaskStatusResponse> taskStatus(final String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/status", StringUtils.urlEncode(taskId));

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), TaskStatusResponse.class)
    );
  }

  @Override
  public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(
      List<LockFilterPolicy> lockFilterPolicies
  )
  {
    final String path = "/druid/indexer/v1/lockedIntervals/v2";

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, lockFilterPolicies),
            new BytesFullResponseHandler()
        ),
        holder -> {
          final Map<String, List<Interval>> response = JacksonUtils.readValue(
              jsonMapper,
              holder.getContent(),
              new TypeReference<>() {}
          );

          return response == null ? Collections.emptyMap() : response;
        }
    );
  }

  @Override
  public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/reports", StringUtils.urlEncode(taskId));

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), TaskReport.ReportMap.class)
    );
  }

  @Override
  public ListenableFuture<Map<String, String>> postSupervisor(SupervisorSpec supervisor)
  {
    final String path = "/druid/indexer/v1/supervisor?skipRestartIfUnmodified=true";

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, supervisor),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<CloseableIterator<SupervisorStatus>> supervisorStatuses()
  {
    final String path = "/druid/indexer/v1/supervisor?system";

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new InputStreamResponseHandler()
        ),
        in -> asJsonParserIterator(in, SupervisorStatus.class)
    );
  }

  @Override
  public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
  {
    final String path = "/druid/indexer/v1/workers";

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder ->
            JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
  {
    final String path = "/druid/indexer/v1/totalWorkerCapacity";

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), IndexingTotalWorkerCapacityInfo.class)
    );
  }

  @Override
  public ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval)
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/pendingSegments/%s?interval=%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(interval.toString())
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.DELETE, path),
            new BytesFullResponseHandler()
        ),
        holder -> {
          final Map<String, Object> resultMap = JacksonUtils.readValue(
              jsonMapper,
              holder.getContent(),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          );

          final Object numDeletedObject = resultMap.get("numDeleted");
          return (Integer) Preconditions.checkNotNull(numDeletedObject, "numDeletedObject");
        }
    );
  }

  @Override
  public ListenableFuture<TaskPayloadResponse> taskPayload(final String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s", StringUtils.urlEncode(taskId));

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), TaskPayloadResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(String dataSource)
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s",
        StringUtils.urlEncode(dataSource)
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      SegmentsToUpdateFilter filter
  )
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s/markUsed",
        StringUtils.urlEncode(dataSource)
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, filter),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentAsUsed(SegmentId segmentId)
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s/segments/%s",
        StringUtils.urlEncode(segmentId.getDataSource()),
        StringUtils.urlEncode(segmentId.toString())
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(String dataSource)
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s",
        StringUtils.urlEncode(dataSource)
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.DELETE, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentsAsUnused(
      String dataSource,
      SegmentsToUpdateFilter filter
  )
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s/markUnused",
        StringUtils.urlEncode(dataSource)
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, filter),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<SegmentUpdateResponse> markSegmentAsUnused(SegmentId segmentId)
  {
    final String path = StringUtils.format(
        "/druid/indexer/v1/datasources/%s/segments/%s",
        StringUtils.urlEncode(segmentId.getDataSource()),
        StringUtils.urlEncode(segmentId.toString())
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.DELETE, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SegmentUpdateResponse.class)
    );
  }

  @Override
  public ListenableFuture<Boolean> isCompactionSupervisorEnabled()
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/druid/indexer/v1/compaction/isSupervisorEnabled"),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), Boolean.class)
    );
  }

  @Override
  public OverlordClientImpl withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new OverlordClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }

  private <T> JsonParserIterator<T> asJsonParserIterator(final InputStream in, final Class<T> clazz)
  {
    return new JsonParserIterator<>(
        jsonMapper.getTypeFactory().constructType(clazz),
        Futures.immediateFuture(in),
        jsonMapper
    );
  }
}
