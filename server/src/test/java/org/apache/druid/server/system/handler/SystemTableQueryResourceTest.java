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

package org.apache.druid.server.system.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResourceQueryResultPusherFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.ResourceIOReaderWriterFactory;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SystemTableQueryResourceTest
{
  private static final String INTERVAL = "2000-01-01/2001-01-01";

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final QueryLifecycleFactory queryLifecycleFactory = Mockito.mock(QueryLifecycleFactory.class);
  private final QueryResourceQueryResultPusherFactory resultPusherFactory =
      Mockito.mock(QueryResourceQueryResultPusherFactory.class);
  private final MockHttpServletRequest request = new MockHttpServletRequest();

  private SystemTableQueryResource resource;

  @BeforeEach
  public void setUp()
  {
    request.contentType = MediaType.APPLICATION_JSON;
    resource = new SystemTableQueryResource(
        queryLifecycleFactory,
        objectMapper,
        Mockito.mock(QueryScheduler.class),
        Mockito.mock(AuthorizerMapper.class),
        resultPusherFactory,
        new ResourceIOReaderWriterFactory(objectMapper, objectMapper),
        new ServerConfig()
    );
  }

  /** A scan-only node must reject an externally submitted {@code segmentMetadata} native query. */
  @Test
  public void testRejectsSegmentMetadataQuery() throws IOException
  {
    assertRejected(
        "{"
        + "\"queryType\":\"segmentMetadata\","
        + "\"dataSource\":\"foo\","
        + "\"intervals\":[\"" + INTERVAL + "\"]"
        + "}"
    );
  }

  /** A scan-only node must reject a regular segment-backed native Scan query. */
  @Test
  public void testRejectsTableScanQuery() throws IOException
  {
    assertRejected(
        "{"
        + "\"queryType\":\"scan\","
        + "\"dataSource\":\"foo\","
        + "\"intervals\":[\"" + INTERVAL + "\"]"
        + "}"
    );
  }

  /** A direct local system-table Scan is admitted to normal authentication, authorization, and execution. */
  @Test
  public void testAcceptsDirectSystemTableScanQuery() throws IOException
  {
    final QueryLifecycle queryLifecycle = Mockito.mock(QueryLifecycle.class);
    final QueryResourceQueryResultPusherFactory.QueryResourceQueryResultPusher resultPusher =
        Mockito.mock(QueryResourceQueryResultPusherFactory.QueryResourceQueryResultPusher.class);
    Mockito.when(queryLifecycleFactory.factorize()).thenReturn(queryLifecycle);
    Mockito.when(queryLifecycle.threadName(ArgumentMatchers.anyString())).thenReturn("system-table-query-test");
    Mockito.when(queryLifecycle.authorize(request)).thenReturn(AuthorizationResult.ALLOW_NO_RESTRICTION);
    Mockito.when(resultPusherFactory.factorize(
        ArgumentMatchers.any(),
        Mockito.eq(request),
        Mockito.eq(queryLifecycle),
        ArgumentMatchers.any()
    )).thenReturn(resultPusher);
    Mockito.when(resultPusher.push()).thenReturn(Response.ok().build());

    final Response response = post(systemTableScanContext(""));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Mockito.verify(queryLifecycle).initialize(ArgumentMatchers.any());
  }

  private void assertRejected(final String queryJson) throws IOException
  {
    final Response response = post(queryJson);

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertTrue(
        objectMapper.readTree((byte[]) response.getEntity())
                    .get("errorMessage")
                    .asText()
                    .contains("local system-table Scan")
    );
    Mockito.verifyNoInteractions(queryLifecycleFactory);
  }

  private Response post(final String queryJson) throws IOException
  {
    return resource.doPost(
        new ByteArrayInputStream(queryJson.getBytes(StandardCharsets.UTF_8)),
        null,
        request
    );
  }

  private static String systemTableScanContext(final String context)
  {
    return "{"
           + "\"queryType\":\"scan\","
           + "\"dataSource\":{\"type\":\"systemTable\",\"table\":\"server_properties\"},"
           + "\"intervals\":[\"" + INTERVAL + "\"],"
           + "\"context\":{" + context + "}"
           + "}";
  }
}
