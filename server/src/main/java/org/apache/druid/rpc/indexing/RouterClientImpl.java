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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

/**
 * Production implementation of {@link RouterClient}.
 */
public class RouterClientImpl implements RouterClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public RouterClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = Preconditions.checkNotNull(client, "client");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
  }

  @Override
  public ListenableFuture<Void> runQuery(final Object query)
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/druid/v2/sql/task")
                .jsonContent(jsonMapper, query),
            new BytesFullResponseHandler()
        ),
        holder -> null
    );
  }
}
