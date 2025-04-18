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

package org.apache.druid.tests.query;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Test the SQL endpoint with different Content-Type
 */
@Test(groups = {TestNGGroup.QUERY, TestNGGroup.CENTRALIZED_DATASOURCE_SCHEMA})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITSqlQueryTest
{
  private static final Logger LOG = new Logger(ITSqlQueryTest.class);

  @Inject
  IntegrationTestingConfig config;

  interface IExecutable {
    void execute(String endpoint) throws IOException;
  }

  private void executeWithRetry(String contentType, IExecutable executable)
  {
    String endpoint = "/druid/v2/sql/";

    Throwable lastException = null;
    for (int i = 1; i <= 5; i++) {
      LOG.info("Query to %s with Content-Type = %s, tries = %s", endpoint, contentType, i);
      try {
        executable.execute(StringUtils.format("%s%s", config.getBrokerUrl(), endpoint));
        return;
      }
      catch (IOException e) {
        // Only catch IOException
        lastException = e;
      }
      try {
        Thread.sleep(200);
      }
      catch (InterruptedException ignored) {
        break;
      }
    }
    Assert.fail(contentType + " failed after 5 tries, last exception: " + lastException);
  }

  @Test
  public void testEmptyContentType()
  {
    executeWithRetry(
        "<EMPTY>",
        (endpoint) -> {
          try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(endpoint);
            request.setEntity(new StringEntity("select 1"));

            try (CloseableHttpResponse response = client.execute(request)) {
              Assert.assertEquals(200, response.getStatusLine().getStatusCode());

              HttpEntity entity = response.getEntity();
              String responseBody = entity != null ? EntityUtils.toString(entity).trim() : null;
              Assert.assertEquals("[{\"EXPR$0\":1}]", responseBody);
            }
          }
        }
    );
  }

  @Test
  public void testTextPlain()
  {
    executeWithRetry(
        "text/plain",
        (endpoint) -> {
          try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(endpoint);
            request.setHeader("Content-Type", "text/plain");
            request.setEntity(new StringEntity("select 1"));

            try (CloseableHttpResponse response = client.execute(request)) {
              Assert.assertEquals(200, response.getStatusLine().getStatusCode());

              HttpEntity entity = response.getEntity();
              String responseBody = entity != null ? EntityUtils.toString(entity).trim() : null;
              Assert.assertEquals("[{\"EXPR$0\":1}]", responseBody);
            }
          }
        }
    );
  }

  @Test
  public void testFormURLEncoded()
  {
    executeWithRetry(
        "application/x-www-form-urlencoded",
        (endpoint) -> {
          try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(endpoint);
            request.setHeader("Content-Type", "application/x-www-form-urlencoded");
            request.setEntity(new StringEntity(URLEncoder.encode("select 1", StandardCharsets.UTF_8)));

            try (CloseableHttpResponse response = client.execute(request)) {
              Assert.assertEquals(200, response.getStatusLine().getStatusCode());

              HttpEntity entity = response.getEntity();
              String responseBody = entity != null ? EntityUtils.toString(entity).trim() : null;
              Assert.assertEquals("[{\"EXPR$0\":1}]", responseBody);
            }
          }
        }
    );
  }

  @Test
  public void testJSON()
  {
    executeWithRetry(
        "application/json",
        (endpoint) -> {
          try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(endpoint);
            request.setHeader("Content-Type", "application/json");
            request.setEntity(new StringEntity(StringUtils.format("{\"query\":\"select 1\"}")));

            try (CloseableHttpResponse response = client.execute(request)) {
              Assert.assertEquals(200, response.getStatusLine().getStatusCode());

              HttpEntity entity = response.getEntity();
              String responseBody = entity != null ? EntityUtils.toString(entity).trim() : null;
              Assert.assertEquals("[{\"EXPR$0\":1}]", responseBody);
            }
          }
        }
    );
  }
}
