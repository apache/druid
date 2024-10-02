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

package org.apache.druid.msq.dart.controller.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.msq.dart.controller.http.GetQueriesResponse;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

/**
 * Production implementation of {@link DartSqlClient}.
 */
public class DartSqlClientImpl implements DartSqlClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public DartSqlClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<GetQueriesResponse> getRunningQueries(final boolean selfOnly)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, selfOnly ? "/?selfOnly" : "/"),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), GetQueriesResponse.class)
    );
  }
}
