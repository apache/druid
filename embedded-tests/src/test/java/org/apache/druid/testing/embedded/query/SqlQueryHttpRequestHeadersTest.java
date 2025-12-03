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

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import javax.ws.rs.core.MediaType;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Suite to test various Content-Type headers attached
 * to SQL query HTTP requests to brokers and routers.
 */
public class SqlQueryHttpRequestHeadersTest extends QueryTestBase
{
  @ParameterizedTest
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
  public void testNullContentType(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        null,
        "select 1",
        (request) -> {
        },
        (statusCode, responseBody) -> Assertions.assertEquals(statusCode, HttpResponseStatus.BAD_REQUEST.getCode())
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
  public void testUnsupportedContentType(boolean shouldQueryBroker)
  {
    executeQuery(
        shouldQueryBroker ? brokerEndpoint : routerEndpoint,
        "application/xml",
        "select 1",
        (request) -> {
        },
        (statusCode, responseBody) -> Assertions.assertEquals(
            statusCode,
            HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE.getCode()
        )
    );
  }

  @ParameterizedTest
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
  @FieldSource("SHOULD_USE_BROKER_TO_QUERY")
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
