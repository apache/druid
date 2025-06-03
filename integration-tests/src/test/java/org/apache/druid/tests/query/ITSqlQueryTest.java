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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Test the SQL endpoint with different Content-Type
 */
@Test(groups = {TestNGGroup.QUERY})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITSqlQueryTest
{
  private static final Logger LOG = new Logger(ITSqlQueryTest.class);

  @Inject
  IntegrationTestingConfig config;

  @Inject
  @TestClient
  HttpClient httpClient;

  interface IExecutable
  {
    void execute(String endpoint) throws Exception;
  }

  interface OnRequest
  {
    void on(Request request) throws IOException;
  }

  interface OnResponse
  {
    void on(int statusCode, String response) throws IOException;
  }

  private void executeWithRetry(String endpoint, String contentType, IExecutable executable)
  {
    Throwable lastException = null;
    for (int i = 1; i <= 5; i++) {
      LOG.info("Query to %s with Content-Type = %s, tries = %s", endpoint, contentType, i);
      try {
        executable.execute(endpoint);
        return;
      }
      catch (Exception e) {
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
    throw new ISE(contentType + " failed after 5 tries, last exception: " + lastException);
  }

  private void executeQuery(
      String contentType,
      String query,
      OnRequest onRequest,
      OnResponse onResponse
  )
  {
    IExecutable executable = (endpoint) -> {
      Request request = new Request(HttpMethod.POST, new URL(endpoint));
      if (contentType != null) {
        request.addHeader("Content-Type", contentType);
      }
      
      if (query != null) {
        request.setContent(query.getBytes(StandardCharsets.UTF_8));
      }
      
      if (onRequest != null) {
        onRequest.on(request);
      }

      StatusResponseHolder response = httpClient.go(request, StatusResponseHandler.getInstance())
                                                .get();
      
      assertNotNull(response);
      
      onResponse.on(
          response.getStatus().getCode(),
          response.getContent().trim()
      );
    };

    // Send query to broker to exeucte
    executeWithRetry(StringUtils.format("%s/druid/v2/sql/", config.getBrokerUrl()), contentType, executable);

    // Send query to router to execute
    executeWithRetry(StringUtils.format("%s/druid/v2/sql/", config.getRouterUrl()), contentType, executable);
  }

  private void assertEquals(String expected, String actual)
  {
    if (!expected.equals(actual)) {
      throw new ISE("Expected [%s] but got [%s]", expected, actual);
    }
  }

  private void assertEquals(int expected, int actual, String message)
  {
    if (expected != actual) {
      throw new ISE("Expected [%d] but got [%d]: %s", expected, actual, message);
    }
  }

  private void assertNotNull(Object object)
  {
    if (object == null) {
      throw new ISE("Expected not null");
    }
  }

  private void assertStringCompare(String expected, String actual, Function<String, Boolean> predicate)
  {
    if (!predicate.apply(expected)) {
      throw new ISE("Expected: [%s] but got [%s]", expected, actual);
    }
  }

  @Test
  public void testNullContentType()
  {
    executeQuery(
        null,
        "select 1",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE.getCode(), statusCode, responseBody);
          assertStringCompare("Unsupported Content-Type:", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testUnsupportedContentType()
  {
    executeQuery(
        "application/xml",
        "select 1",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE.getCode(), statusCode, responseBody);
          assertStringCompare("Unsupported Content-Type:", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testTextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        "select \n1",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(200, statusCode, responseBody);
          assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }

  @Test
  public void testFormURLEncoded()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        URLEncoder.encode("select 'x % y'", StandardCharsets.UTF_8),
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(200, statusCode, responseBody);
          assertEquals("[{\"EXPR$0\":\"x % y\"}]", responseBody);
        }
    );
  }

  @Test
  public void testFormURLEncoded_InvalidEncoding()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        "select 'x % y'",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Unable to decode", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testJSON()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        "{\"query\":\"select 567\"}",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(200, statusCode, responseBody);
          assertEquals("[{\"EXPR$0\":567}]", responseBody);
        }
    );
  }

  @Test
  public void testInvalidJSONFormat()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        "{\"query\":select 567}",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Malformed SQL query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_TextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        null,
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_UrlEncoded()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        null,
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testBlankQuery_TextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        "     ",
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_JSON()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        null,
        (request) -> {},
        (statusCode, responseBody) -> {
          assertEquals(400, statusCode, responseBody);
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  /**
   * When multiple Content-Type headers are set, the first one(in this case it's the text format) should be used.
   */
  @Test
  public void testMultipleContentType()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        "SELECT 1",
        (request) -> {
          // Add one more Content-Type header
          request.addHeader("Content-Type", MediaType.APPLICATION_JSON);
        },
        (statusCode, responseBody) -> {
          assertEquals(200, statusCode, responseBody);
          assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }
}
