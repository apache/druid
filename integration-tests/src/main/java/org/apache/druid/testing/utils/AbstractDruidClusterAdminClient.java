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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.nio.channels.ClosedChannelException;

public abstract class AbstractDruidClusterAdminClient
{
  private static final Logger LOG = new Logger(AbstractDruidClusterAdminClient.class);

  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private IntegrationTestingConfig config;

  @Inject
  AbstractDruidClusterAdminClient(
      ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.config = config;
  }

  public abstract void restartCoordinatorContainer();

  public abstract void restartCoordinatorTwoContainer();

  public abstract void restartHistoricalContainer();

  public abstract void restartOverlordContainer();

  public abstract void restartOverlordTwoContainer();

  public abstract void restartBrokerContainer();

  public abstract void restartRouterContainer();

  public abstract void restartMiddleManagerContainer();

  public void waitUntilCoordinatorReady()
  {
    waitUntilInstanceReady(config.getCoordinatorUrl());
    postDynamicConfig(CoordinatorDynamicConfig.builder()
                                              .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(1)
                                              .build());
  }

  public void waitUntilCoordinatorTwoReady()
  {
    waitUntilInstanceReady(config.getCoordinatorTwoUrl());
    postDynamicConfig(CoordinatorDynamicConfig.builder()
                                              .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(1)
                                              .build());
  }

  public void waitUntilOverlordTwoReady()
  {
    waitUntilInstanceReady(config.getOverlordTwoUrl());
  }

  public void waitUntilHistoricalReady()
  {
    waitUntilInstanceReady(config.getHistoricalUrl());
  }

  public void waitUntilIndexerReady()
  {
    waitUntilInstanceReady(config.getOverlordUrl());
  }

  public void waitUntilBrokerReady()
  {
    waitUntilInstanceReady(config.getBrokerUrl());
  }

  public void waitUntilRouterReady()
  {
    waitUntilInstanceReady(config.getRouterUrl());
  }

  private void waitUntilInstanceReady(final String host)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          try {
            StatusResponseHolder response = httpClient.go(
                new Request(HttpMethod.GET, new URL(StringUtils.format("%s/status/health", host))),
                StatusResponseHandler.getInstance()
            ).get();

            LOG.info("%s %s", response.getStatus(), response.getContent());
            return response.getStatus().equals(HttpResponseStatus.OK);
          }
          catch (Throwable e) {
            //
            // supress stack trace logging for some specific exceptions
            // to reduce excessive stack trace messages when waiting druid nodes to start up
            //
            if (e.getCause() instanceof ChannelException) {
              Throwable channelException = e.getCause();

              if (channelException.getCause() instanceof ClosedChannelException) {
                LOG.error("Channel Closed");
              } else if ("Channel disconnected".equals(channelException.getMessage())) {
                // log message only
                LOG.error("Channel disconnected");
              } else {
                // log stack trace for unknown exception
                LOG.error(e, "");
              }
            } else {
              // log stack trace for unknown exception
              LOG.error(e, "");
            }

            return false;
          }
        },
        "Waiting for instance to be ready: [" + host + "]"
    );
  }

  private void postDynamicConfig(CoordinatorDynamicConfig coordinatorDynamicConfig)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          try {
            String url = StringUtils.format("%s/druid/coordinator/v1/config", config.getCoordinatorUrl());
            StatusResponseHolder response = httpClient.go(
                new Request(HttpMethod.POST, new URL(url)).setContent(
                    "application/json",
                    jsonMapper.writeValueAsBytes(coordinatorDynamicConfig)
                ), StatusResponseHandler.getInstance()
            ).get();

            LOG.info("%s %s", response.getStatus(), response.getContent());
            // if coordinator is not leader then it will return 307 instead of 200
            return response.getStatus().equals(HttpResponseStatus.OK) || response.getStatus().equals(HttpResponseStatus.TEMPORARY_REDIRECT);
          }
          catch (Throwable e) {
            LOG.error(e, "");
            return false;
          }
        },
        "Posting dynamic config after startup"
    );
  }
}
