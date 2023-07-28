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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceCallBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;

public class LookupServiceClientImpl implements LookupServiceClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public LookupServiceClientImpl(ServiceClient client, ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<Map<String, LookupExtractorFactoryContainer>> fetchLookupsForTier(String tier)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/lookups/config/%s?detailed=true",
        StringUtils.urlEncode(tier)
    );

    return ServiceCallBuilder
        .forRequest(new RequestBuilder(HttpMethod.GET, path))
        .handler(new BytesFullResponseHandler())
        .onSuccess(holder -> {
          final Map<String, Object> lookupMap = JacksonUtils.readValue(
              jsonMapper,
              holder.getContent(),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          );

          return LookupUtils.tryConvertObjectMapToLookupConfigMap(lookupMap, jsonMapper);
        })
        .onHttpError(e -> {
          if (e.getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
            return Either.value(null);
          } else {
            return Either.error(e);
          }
        })
        .go(client);
  }
}
