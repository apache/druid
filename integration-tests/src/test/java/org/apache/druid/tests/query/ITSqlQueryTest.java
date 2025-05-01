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
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

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

  interface IExecutable
  {
    void execute(String endpoint) throws IOException;
  }

  interface OnRequest
  {
    void on(HttpPost request) throws IOException;
  }

  interface OnResponse
  {
    void on(int statusCode, HttpEntity response) throws IOException;
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
    throw new ISE(contentType + " failed after 5 tries, last exception: " + lastException);
  }

  private void executeQuery(
      String contentType,
      OnRequest onRequest,
      OnResponse onResponse
  )
  {
    IExecutable executable = (endpoint) -> {
      try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
        HttpPost request = new HttpPost(endpoint);
        if (contentType != null) {
          request.addHeader("Content-Type", contentType);
        }
        onRequest.on(request);

        try (CloseableHttpResponse response = client.execute(request)) {
          HttpEntity responseEntity = response.getEntity();
          assertNotNull(responseEntity);

          onResponse.on(
              response.getStatusLine().getStatusCode(),
              responseEntity
          );
        }
      }
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

  private void assertEquals(int expected, int actual)
  {
    if (expected != actual) {
      throw new ISE("Expected [%d] but got [%d]", expected, actual);
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
        (request) -> {
          request.setEntity(new StringEntity("select 1"));
        },
        (statusCode, responseEntity) -> {
          assertEquals(HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Unsupported Content-Type:", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testUnsupportedCotentType()
  {
    executeQuery(
        "application/xml",
        (request) -> {
          request.setEntity(new StringEntity("select 1"));
        },
        (statusCode, responseEntity) -> {
          assertEquals(HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Unsupported Content-Type:", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testTextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        (request) -> {
          request.setEntity(new StringEntity("select \n1"));
        },
        (statusCode, responseEntity) -> {
          assertEquals(200, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }

  @Test
  public void testFormURLEncoded()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        (request) -> {
          request.setEntity(new StringEntity(URLEncoder.encode("select 'x % y'", StandardCharsets.UTF_8)));
        },
        (statusCode, responseEntity) -> {
          assertEquals(200, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertEquals("[{\"EXPR$0\":\"x % y\"}]", responseBody);
        }
    );
  }

  @Test
  public void testFormURLEncoded_InvalidEncoding()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        (request) -> {
          request.setEntity(new StringEntity("select 'x % y'"));
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Unable to decoded", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testJSON()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        (request) -> {
          request.setEntity(new StringEntity(StringUtils.format("{\"query\":\"select 567\"}")));
        },
        (statusCode, responseEntity) -> {
          assertEquals(200, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertEquals("[{\"EXPR$0\":567}]", responseBody);
        }
    );
  }

  @Test
  public void testInvalidJSONFormat()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        (request) -> {
          request.setEntity(new StringEntity(StringUtils.format("{\"query\":select 567}")));
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Malformed SQL query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_TextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        (request) -> {
          // Empty query, DO NOTHING
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_UrlEncoded()
  {
    executeQuery(
        MediaType.APPLICATION_FORM_URLENCODED,
        (request) -> {
          // Empty query, DO NOTHING
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testBlankQuery_TextPlain()
  {
    executeQuery(
        MediaType.TEXT_PLAIN,
        (request) -> {
          // an query with blank characters
          request.setEntity(new StringEntity("     "));
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertStringCompare("Empty query", responseBody, responseBody::contains);
        }
    );
  }

  @Test
  public void testEmptyQuery_JSON()
  {
    executeQuery(
        MediaType.APPLICATION_JSON,
        (request) -> {
          // Empty query, DO NOTHING
        },
        (statusCode, responseEntity) -> {
          assertEquals(400, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
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
        (request) -> {
          // Add one more Content-Type header
          request.addHeader("Content-Type", MediaType.APPLICATION_JSON);
          request.setEntity(new StringEntity(StringUtils.format("SELECT 1")));
        },
        (statusCode, responseEntity) -> {
          assertEquals(200, statusCode);

          String responseBody = EntityUtils.toString(responseEntity).trim();
          assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }
}
