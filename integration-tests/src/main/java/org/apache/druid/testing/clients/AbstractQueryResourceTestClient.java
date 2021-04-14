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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public abstract class AbstractQueryResourceTestClient<QueryType>
{
  protected boolean requestSmileEncoding = false;
  protected boolean responseSmileEncoding = false;

  final ObjectMapper jsonMapper;
  final ObjectMapper smileMapper;
  final HttpClient httpClient;
  final String routerUrl;

  @Inject
  AbstractQueryResourceTestClient(
      ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @TestClient HttpClient httpClient,
      String routerUrl
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.routerUrl = routerUrl;
  }

  public abstract String getBrokerURL();

  protected Request buildRequest(String url, QueryType query) throws IOException
  {
    Request request = new Request(HttpMethod.POST, new URL(url));
    if (requestSmileEncoding) {
      request.setContent(
          SmileMediaTypes.APPLICATION_JACKSON_SMILE,
          smileMapper.writeValueAsBytes(query)
      );
    } else {
      request.setContent(
          MediaType.APPLICATION_JSON,
          smileMapper.writeValueAsBytes(query)
      );
    }

    if (responseSmileEncoding) {
      request.addHeader("Accpet", SmileMediaTypes.APPLICATION_JACKSON_SMILE);
    }
    return request;
  }

  public List<Map<String, Object>> query(String url, QueryType query)
  {
    try {
      BytesFullResponseHolder response = httpClient.go(
          buildRequest(url, query),
          new BytesFullResponseHandler()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while querying[%s] status[%s] content[%s]",
            getBrokerURL(),
            response.getStatus(),
            response.getContent()
        );
      }

      ObjectMapper responseMapper = this.responseSmileEncoding ? smileMapper : jsonMapper;
      return responseMapper.readValue(
          response.getContent(), new TypeReference<List<Map<String, Object>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Future<StatusResponseHolder> queryAsync(String url, QueryType query)
  {
    try {
      return httpClient.go(
          buildRequest(url, query),
          StatusResponseHandler.getInstance()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
