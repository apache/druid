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

package io.druid.testing.clients;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.query.Query;
import io.druid.testing.IntegrationTestingConfig;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class QueryResourceTestClient
{
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String router;
  private final StatusResponseHandler responseHandler;

  @Inject
  QueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Global HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.router = config.getRouterHost();
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  }

  private String getBrokerURL()
  {
    return String.format(
        "http://%s/druid/v2/",
        router
    );
  }

  public List<Map<String, Object>> query(Query query)
  {
    return query(getBrokerURL(), query);
  }

  public List<Map<String, Object>> query(String url, Query query)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, new URL(url)).setContent(
              "application/json",
              jsonMapper.writeValueAsBytes(query)
          ), responseHandler

      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while querying[%s] status[%s] content[%s]",
            getBrokerURL(),
            response.getStatus(),
            response.getContent()
        );
      }

      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<Map<String, Object>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
