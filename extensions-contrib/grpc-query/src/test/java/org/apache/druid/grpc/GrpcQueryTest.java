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

package org.apache.druid.grpc;

import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.grpc.server.QueryDriver;
import org.apache.druid.grpc.server.QueryServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Simple test that runs the gRPC server, on top of a test SQL stack.
 * Uses a simple client to send a query to the server. This is a basic
 * sanity check of the gRPC stack.
 */
public class GrpcQueryTest
{
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static QueryFrameworkFixture frameworkFixture;
  private static QueryServer server;
  private static TestClient client;

  @BeforeClass
  public static void setup() throws IOException
  {
    frameworkFixture = new QueryFrameworkFixture(temporaryFolder.newFolder());
    QueryDriver driver = new QueryDriver(
        frameworkFixture.jsonMapper(),
        frameworkFixture.statementFactory()
    );
    server = new QueryServer(50051, driver);
    try {
      server.start();
    }
    catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
    client = new TestClient();
  }

  @AfterClass
  public static void tearDown() throws InterruptedException
  {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
      server.blockUntilShutdown();
    }
  }

  /**
   * Do a very basic query.
   */
  @Test
  public void testBasics()
  {
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery("SELECT * FROM foo")
        .setResultFormat(QueryResultFormat.CSV)
        .build();
    QueryResponse response = client.client.submitQuery(request);
    assertEquals(QueryStatus.OK, response.getStatus());
  }
}
