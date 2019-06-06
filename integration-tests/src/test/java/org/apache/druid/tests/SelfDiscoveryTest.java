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

package org.apache.druid.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.AbstractQueryResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.RetryUtil;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Map;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class SelfDiscoveryTest
{
  private static final Logger LOG = new Logger(SelfDiscoveryTest.class);

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @Client
  HttpClient httpClient;

  @Test
  public void testHistorical()
  {
    RetryUtil.retryUntilTrue(() -> selfDiscovered(config.getHistoricalUrl()), "Historical self-discovered");
  }

  private boolean selfDiscovered(String nodeUrl)
  {
    StatusResponseHolder response = AbstractQueryResourceTestClient.makeRequestWithExpectedStatus(
        httpClient,
        HttpMethod.GET,
        nodeUrl + "/status/selfDiscoveredStatus",
        null,
        HttpResponseStatus.OK
    );
    try {
      Map<String, Boolean> selfDiscoveredStatus =
          jsonMapper.readValue(response.getContent(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_BOOLEAN);
      return selfDiscoveredStatus.get("selfDiscovered");
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to probe selfDiscoveryStatus on %s", nodeUrl);
      return false;
    }
  }
}
