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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
  private final Logger log = new Logger(BrokerClient.class);
  private static final int MAX_RETRIES = 5;

  private final HttpClient httpClient;
  private final DruidNodeDiscovery druidNodeDiscovery;

  @Inject
  public BrokerClient(
      @EscalatedGlobal HttpClient httpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
  )
  {
    this.httpClient = httpClient;
    this.druidNodeDiscovery = druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER);
  }

  /**
   * Creates and returns a {@link Request} after choosing a broker.
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
  {
    String host = ClientUtils.pickOneHost(druidNodeDiscovery);

    if (host == null) {
      throw new IOE("No known server");
    }
    return new Request(httpMethod, new URL(StringUtils.format("%s%s", host, urlPath)));
  }

  public StringFullResponseHolder sendQuery(Request request) throws Exception
  {
    StringFullResponseHandler responseHandler = new StringFullResponseHandler(StandardCharsets.UTF_8);

    for (int counter = 0; counter < MAX_RETRIES; counter++) {

      final StringFullResponseHolder fullResponseHolder;

      try {
        try {
          fullResponseHolder = httpClient.go(request, responseHandler).get();
        }
        catch (ExecutionException e) {
          // Unwrap IOExceptions and ChannelExceptions, re-throw others
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          Throwables.propagateIfInstanceOf(e.getCause(), ChannelException.class);
          throw new RE(e, "HTTP request to[%s] failed", request.getUrl());
        }
      }
      catch (IOException | ChannelException ex) {
        // can happen if the node is stopped.
        log.warn(ex, "Request[%s] failed.", request.getUrl());
        request = getNewRequestUrl(request);
        continue;
      }
      HttpResponseStatus responseStatus = fullResponseHolder.getResponse().getStatus();
      if (HttpResponseStatus.SERVICE_UNAVAILABLE.equals(responseStatus)
          || HttpResponseStatus.GATEWAY_TIMEOUT.equals(responseStatus)) {
        log.warn(
            "Request[%s] received a %s response. Attempt %s/%s",
            request.getUrl(),
            responseStatus,
            counter + 1,
            MAX_RETRIES
        );
        request = getNewRequestUrl(request);
      } else {
        return fullResponseHolder;
      }
    }

    throw new IOE("Retries exhausted, couldn't fulfill request to [%s].", request.getUrl());
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
      throw new ISE(
          e,
          "failed to build url with path[%] and query string [%s].",
          oldRequest.getUrl().getPath(),
          oldRequest.getUrl().getQuery()
      );
    }
  }
}
