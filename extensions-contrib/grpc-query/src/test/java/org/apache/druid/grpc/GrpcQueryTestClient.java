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
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryGrpc.QueryBlockingStub;
import org.apache.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Super-simple client which makes a single query request and prints
 * the response. Useful because Druid provides no other rRPC client
 * to use to test the rRPC endpoint.
 */
public class GrpcQueryTestClient
{
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

  public static void main(String[] args)
  {
    if (args.length != 1) {
      System.err.println("Usage: sql-query");
      System.exit(1);
    }
    TestClient client = new TestClient();
    QueryRequest request = QueryRequest.newBuilder()
        .setQuery(args[0])
        .setResultFormat(QueryResultFormat.CSV)
        .build();
    QueryResponse response = client.client.submitQuery(request);
    if (response.getStatus() != QueryStatus.OK) {
      System.err.println("Failed: " + response.getStatus().name());
      System.err.println(response.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Columns:");
    for (ColumnSchema col : response.getColumnsList()) {
      System.out.println(StringUtils.format("%s %s (%s)", col.getName(), col.getSqlType(), col.getDruidType().name()));
    }
    System.out.println("Data:");
    System.out.println(response.getData().toString(StandardCharsets.UTF_8));
    try {
      client.close();
    } catch (InterruptedException e) {
      // Ignore;
    }
  }
}
