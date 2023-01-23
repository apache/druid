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

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.druid.grpc.server.QueryDriver;
import org.apache.druid.grpc.server.QueryServer;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryGrpc.QueryBlockingStub;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class GrpcQueryTest
{
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static QueryFrameworkFixture frameworkFixture;
  static QueryServer server;
  static TestClient client;

  public static class TestClient
  {
    ManagedChannel channel;
    QueryBlockingStub client;

    public TestClient()
    {
      // Access a service running on the local machine on port 50051
      String target = "localhost:50051";
      // Create a communication channel to the server, known as a Channel. Channels are thread-safe
      // and reusable. It is common to create channels at the beginning of your application and reuse
      // them until the application shuts down.
      //
      // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
      // use TLS, use TlsChannelCredentials instead.
      channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
          .build();
      client = QueryGrpc.newBlockingStub(channel);
    }

    public void close() throws InterruptedException
    {
      // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
      // resources the channel should be shut down when it will no longer be used. If it may be used
      // again leave it running.
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @BeforeClass
  public static void setup() throws IOException
  {
    frameworkFixture = new QueryFrameworkFixture(temporaryFolder.newFolder());
    QueryDriver driver = new QueryDriver(
        frameworkFixture.jsonMapper(),
        frameworkFixture.statementFactory()
    );
    server = new QueryServer(driver);
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
    QueryRequest request = QueryRequest.newBuilder().setQuery("SELECT * FROM foo").build();
    QueryResponse response = client.client.submitQuery(request);
    assertEquals(QueryStatus.OK, response.getStatus());
  }
}
