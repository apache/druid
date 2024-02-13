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

package org.apache.druid.discovery;

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

/**
 * This class facilitates interaction with Broker.
 */
public class BrokerClient
{
  private static final int MAX_RETRIES = 5;

  private final HttpClient brokerHttpClient;
  private final DruidNodeDiscovery druidNodeDiscovery;

  @Inject
  public BrokerClient(
      @EscalatedGlobal HttpClient brokerHttpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
  )
  {
    this.brokerHttpClient = brokerHttpClient;
    this.druidNodeDiscovery = druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER);
  }

  /**
   * Creates and returns a {@link Request} after choosing a broker.
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
  {
    String host = ClientUtils.pickOneHost(druidNodeDiscovery);

    if (host == null) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("A leader node could not be found for [%s] service. Check the logs to validate that service is healthy.", NodeRole.BROKER);
    }
    return new Request(httpMethod, new URL(StringUtils.format("%s%s", host, urlPath)));
  }

  public String sendQuery(final Request request) throws Exception
  {
    return RetryUtils.retry(
        () -> {
          Request newRequestUrl = getNewRequestUrl(request);
          final StringFullResponseHolder fullResponseHolder = brokerHttpClient.go(newRequestUrl, new StringFullResponseHandler(StandardCharsets.UTF_8)).get();

          HttpResponseStatus responseStatus = fullResponseHolder.getResponse().getStatus();
          if (HttpResponseStatus.SERVICE_UNAVAILABLE.equals(responseStatus)
              || HttpResponseStatus.GATEWAY_TIMEOUT.equals(responseStatus)) {
            throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build("Request to broker failed due to failed response status: [%s]", responseStatus);
          } else if (responseStatus.getCode() != HttpServletResponse.SC_OK) {
            throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build("Request to broker failed due to failed response code: [%s]", responseStatus.getCode());
          }
          return fullResponseHolder.getContent();
        },
        (throwable) -> {
          if (throwable instanceof ExecutionException) {
            return throwable.getCause() instanceof IOException || throwable.getCause() instanceof ChannelException;
          }
          return throwable instanceof IOE;
        },
        MAX_RETRIES
    );
  }

  private Request getNewRequestUrl(Request oldRequest)
  {
    try {
      return ClientUtils.withUrl(
          oldRequest,
          new URL(StringUtils.format("%s%s", ClientUtils.pickOneHost(druidNodeDiscovery), oldRequest.getUrl().getPath()))
      );
    }
    catch (MalformedURLException e) {
      // Not an IOException; this is our own fault.
      throw DruidException.defensive(
          "Failed to build url with path[%s] and query string [%s].",
          oldRequest.getUrl().getPath(),
          oldRequest.getUrl().getQuery()
      );
    }
  }
}
