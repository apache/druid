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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Suite to test various Content-Type headers attached
 * to SQL query HTTP requests to brokers and routers.
 */
public class SqlQueryHttpRequestHeadersTest extends EmbeddedClusterTestBase
{
  private static final String SQL_QUERY_ROUTE = "%s/druid/v2/sql/";

  public static List<Boolean> flags = List.of(true, false);

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedRouter router = new EmbeddedRouter();

  private HttpClient httpClientRef;
  private String brokerEndpoint;
  private String routerEndpoint;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(new EmbeddedOverlord())
                               .addServer(new EmbeddedCoordinator())
                               .addServer(broker)
                               .addServer(router)
                               .addServer(new EmbeddedIndexer());
  }

  @BeforeEach
  void setUp()
  {
    httpClientRef = router.bindings().globalHttpClient();
    brokerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(broker));
    routerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(router));
  }

  private void executeQuery(
      String endpoint,
      String contentType,
      String query,
      Consumer<Request> onRequest,
      BiConsumer<Integer, String> onResponse
  )
  {
    URL url;
    try {
      url = new URL(endpoint);
    }
    catch (MalformedURLException e) {
      throw new AssertionError("Malformed URL");
    }

    Request request = new Request(HttpMethod.POST, url);
    if (contentType != null) {
      request.addHeader("Content-Type", contentType);
    }

    if (query != null) {
      request.setContent(query.getBytes(StandardCharsets.UTF_8));
    }

    if (onRequest != null) {
      onRequest.accept(request);
    }

    StatusResponseHolder response;
    try {
      response = httpClientRef.go(request, StatusResponseHandler.getInstance())
                              .get();
    }
    catch (InterruptedException | ExecutionException e) {
      throw new AssertionError("Failed to execute a request", e);
    }

    Assertions.assertNotNull(response);

    onResponse.accept(
        response.getStatus().getCode(),
        response.getContent().trim()
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testNullContentType(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        null,
        "select 1",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(statusCode, HttpResponseStatus.BAD_REQUEST.getCode());
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testUnsupportedContentType(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        "application/xml",
        "select 1",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(statusCode, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE.getCode());
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testTextPlain(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.TEXT_PLAIN,
        "select \n1",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(200, statusCode);
          Assertions.assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testFormURLEncoded(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_FORM_URLENCODED,
        URLEncoder.encode("select 'x % y'", StandardCharsets.UTF_8),
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(200, statusCode);
          Assertions.assertEquals("[{\"EXPR$0\":\"x % y\"}]", responseBody);
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testFormURLEncoded_InvalidEncoding(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_FORM_URLENCODED,
        "select 'x % y'",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Unable to decode"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testJSON(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_JSON,
        "{\"query\":\"select 567\"}",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(200, statusCode);
          Assertions.assertEquals("[{\"EXPR$0\":567}]", responseBody);
        }
    );

    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        "application/json; charset=UTF-8",
        "{\"query\":\"select 567\"}",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(200, statusCode);
          Assertions.assertEquals("[{\"EXPR$0\":567}]", responseBody);
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testInvalidJSONFormat(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_JSON,
        "{\"query\":select 567}",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Malformed SQL query"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testEmptyQuery_TextPlain(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.TEXT_PLAIN,
        null,
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Empty query"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testEmptyQuery_UrlEncoded(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_FORM_URLENCODED,
        null,
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Empty query"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testBlankQuery_TextPlain(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.TEXT_PLAIN,
        "     ",
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Empty query"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testEmptyQuery_JSON(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.APPLICATION_JSON,
        null,
        (request) -> {
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(400, statusCode);
          Assertions.assertTrue(responseBody.contains("Empty query"));
        }
    );
  }

  @ParameterizedTest
  @FieldSource("flags")
  public void testMultipleContentType_usesFirstOne(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        MediaType.TEXT_PLAIN,
        "SELECT 1",
        (request) -> {
          // Add one more Content-Type header
          request.addHeader("Content-Type", MediaType.APPLICATION_JSON);
        },
        (statusCode, responseBody) -> {
          Assertions.assertEquals(200, statusCode);
          Assertions.assertEquals("[{\"EXPR$0\":1}]", responseBody);
        }
    );
  }
}
