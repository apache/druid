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

package org.apache.druid.client.broker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.query.Query;
import org.apache.druid.query.explain.ExplainPlan;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class BrokerClientImpl implements BrokerClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public BrokerClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<String> submitNativeQuery(Query<?> query)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/v2")
                .jsonContent(jsonMapper, query),
            new StringFullResponseHandler(StandardCharsets.UTF_8)
        ),
        FullResponseHolder::getContent
    );
  }

  @Override
  public ListenableFuture<String> submitSqlQuery(final ClientSqlQuery sqlQuery)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/")
                    .jsonContent(jsonMapper, sqlQuery),
            new StringFullResponseHandler(StandardCharsets.UTF_8)
        ),
        FullResponseHolder::getContent
    );
  }

  @Override
  public ListenableFuture<SqlTaskStatus> submitSqlTask(final ClientSqlQuery sqlQuery)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/task/")
                .jsonContent(jsonMapper, sqlQuery),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SqlTaskStatus.class)
    );
  }

  @Override
  public ListenableFuture<List<ExplainPlan>> fetchExplainPlan(final ClientSqlQuery sqlQuery)
  {
    final ClientSqlQuery explainSqlQuery = new ClientSqlQuery(
        StringUtils.format("EXPLAIN PLAN FOR %s", sqlQuery.getQuery()),
        null,
        false,
        false,
        false,
        null,
        null
    );
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/task/")
                .jsonContent(jsonMapper, explainSqlQuery),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<Boolean> updateCoordinatorDynamicConfig(CoordinatorDynamicConfig config)
  {
    final RequestBuilder requestBuilder =
        new RequestBuilder(HttpMethod.POST, "/druid-internal/v1/config/coordinator")
            .jsonContent(jsonMapper, config);

    return FutureUtils.transform(
        client.asyncRequest(requestBuilder, new BytesFullResponseHandler()),
        holder -> {
          final HttpResponseStatus status = holder.getStatus();
          return status.equals(HttpResponseStatus.OK);
        }
    );
  }

  @Override
  public ListenableFuture<String> getQueryReport(String sqlQueryId, boolean selfOnly)
  {
    final String path = StringUtils.format(
        "/druid/v2/sql/queries/%s/report%s",
        StringUtils.urlEncode(sqlQueryId),
        selfOnly ? "?selfOnly" : ""
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new StringFullResponseHandler(StandardCharsets.UTF_8)
        ),
        FullResponseHolder::getContent
    );
  }
}

