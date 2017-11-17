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

package io.druid.tests.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.http.client.CredentialedHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.auth.BasicCredentials;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Client;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthConfigurationTest.class);

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @Client
  HttpClient httpClient;

  StatusResponseHandler responseHandler = new StatusResponseHandler(Charsets.UTF_8);

  @Test
  public void testAuthConfiguration() throws Exception
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    HttpClient internalSystemClient = new CredentialedHttpClient(
        new BasicCredentials("druid_system", "warlock"),
        httpClient
    );

    HttpClient newUserClient = new CredentialedHttpClient(
        new BasicCredentials("druid", "helloworld"),
        httpClient
    );

    // check that admin works
    checkNodeAccess(adminClient);

    // check that internal user works
    checkNodeAccess(internalSystemClient);

    // create a new user that can read /status
    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/basic/users/druid",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/basic/users/druid/credentials",
        StringUtils.toUtf8("helloworld")
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/basic/users/druid",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/basic/roles/druidrole",
        null
    );

    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/basic/users/druid/roles/druidrole",
        null
    );

    List<ResourceAction> permissions = Arrays.asList(
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );
    byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
    makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/basic/roles/druidrole/permissions",
        permissionsBytes
    );

    // check that the new user works
    checkNodeAccess(newUserClient);
  }

  private void checkNodeAccess(HttpClient httpClient)
  {
    makeRequest(httpClient, HttpMethod.GET, config.getCoordinatorUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getIndexerUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getHistoricalUrl() + "/status", null);
    makeRequest(httpClient, HttpMethod.GET, config.getRouterUrl() + "/status", null);
  }

  private StatusResponseHolder makeRequest(HttpClient httpClient, HttpMethod method, String url, byte[] content)
  {
    try {
      Request request = new Request(method, new URL(url));
      if (content != null) {
        request.setContent(MediaType.APPLICATION_JSON, content);
      }
      int retryCount = 0;

      StatusResponseHolder response;

      while (true) {
        response = httpClient.go(
            request,
            responseHandler
        ).get();

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          String errMsg = StringUtils.format(
              "Error while making request to url[%s] status[%s] content[%s]",
              url,
              response.getStatus(),
              response.getContent()
          );
          // it can take time for the auth config to propagate, so we retry
          if (retryCount > 4) {
            throw new ISE(errMsg);
          } else {
            LOG.error(errMsg);
            LOG.error("retrying in 3000ms, retryCount: " + retryCount);
            retryCount++;
            Thread.sleep(3000);
          }
        } else {
          break;
        }
      }
      return response;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
