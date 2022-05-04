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

package org.apache.druid.tests.console;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.net.URL;

@Test(groups = TestNGGroup.WEB_CONSOLE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITWebConsoleTest
{
  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testWebConsoleHomePageExists()
  {
    ITRetryUtil.retryUntil(
        () -> {
          ListenableFuture<StatusResponseHolder> status = httpClient.go(
              new Request(
                  HttpMethod.GET,
                  new URL(config.getRouterUrl())
              ),
              StatusResponseHandler.getInstance()
          );

          int httpStatusCode = status.get().getStatus().getCode();
          return httpStatusCode == HttpResponseStatus.OK.getCode()
                 || httpStatusCode == HttpResponseStatus.FOUND.getCode();
        },
        true,
        ITRetryUtil.DEFAULT_RETRY_SLEEP,
        5,
        StringUtils.format("WebConsole home page at [%s] does not exist.", config.getRouterUrl())
    );
  }
}
