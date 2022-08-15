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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
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
  public ListenableFuture<Void> runTask(final String taskId, final Object taskObject)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/task")
                .jsonContent(jsonMapper, taskObject),
            new BytesFullResponseHandler()
        ),
        holder -> {
          final Map<String, Object> map = deserialize(holder, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
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
  public ListenableFuture<Map<String, TaskStatus>> taskStatuses(final Set<String> taskIds)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/taskStatus")
                .jsonContent(jsonMapper, taskIds),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<Map<String, TaskStatus>>() {})
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
        holder -> deserialize(holder, TaskStatusResponse.class)
    );
  }

  @Override
  public ListenableFuture<Map<String, Object>> taskReportAsMap(String taskId)
  {
    final String path = StringUtils.format("/druid/indexer/v1/task/%s/reports", StringUtils.urlEncode(taskId));

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
    );
  }

  @Override
  public OverlordClientImpl withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new OverlordClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }

  private <T> T deserialize(final BytesFullResponseHolder bytesHolder, final Class<T> clazz)
  {
    try {
      return jsonMapper.readValue(bytesHolder.getContent(), clazz);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T deserialize(final BytesFullResponseHolder bytesHolder, final TypeReference<T> typeReference)
  {
    try {
      return jsonMapper.readValue(bytesHolder.getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
