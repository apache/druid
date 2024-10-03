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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class RestCatalogTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private int port = 0;
  private HttpServer server = null;
  private ServerSocket serverSocket = null;

  @Before
  public void setup() throws Exception
  {
    serverSocket = new ServerSocket(0);
    port = serverSocket.getLocalPort();
    serverSocket.close();
    server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
    server.createContext(
        "/v1/config", // API for catalog fetchConfig which is invoked on catalog initialization
        (httpExchange) -> {
          String payload = "{}";
          byte[] outputBytes = payload.getBytes(StandardCharsets.UTF_8);
          httpExchange.sendResponseHeaders(200, outputBytes.length);
          OutputStream os = httpExchange.getResponseBody();
          httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
          httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_LENGTH, String.valueOf(outputBytes.length));
          httpExchange.getResponseHeaders().set(HttpHeaders.CONTENT_RANGE, "bytes 0");
          os.write(outputBytes);
          os.close();
        }
    );
    server.start();
  }

  @Test
  public void testCatalogCreate()
  {
    String catalogUri = "http://localhost:" + port;

    RestIcebergCatalog testRestCatalog = new RestIcebergCatalog(
        catalogUri,
        new HashMap<>(),
        mapper,
        new Configuration()
    );
    RESTCatalog innerCatalog = (RESTCatalog) testRestCatalog.retrieveCatalog();

    Assert.assertEquals("rest", innerCatalog.name());
    Assert.assertNotNull(innerCatalog.properties());
    Assert.assertNotNull(testRestCatalog.getCatalogProperties());
    Assert.assertEquals(testRestCatalog.getCatalogUri(), innerCatalog.properties().get("uri"));
  }
  @After
  public void tearDown() throws IOException
  {
    if (server != null) {
      server.stop(0);
    }
    if (serverSocket != null) {
      serverSocket.close();
    }
  }
}
