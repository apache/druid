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

import io.grpc.CallCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.apache.druid.grpc.proto.HealthGrpc;
import org.apache.druid.grpc.proto.QueryGrpc;
import org.apache.druid.grpc.proto.QueryGrpc.QueryBlockingStub;

import java.util.concurrent.TimeUnit;

/**
 * Super-simple test client that connects to a gRPC query endpoint
 * and allows submitting a rRPC query request and returns the response.
 * The server can be in the same or another process.
 */
public class TestClient implements AutoCloseable
{
  public static final String DEFAULT_HOST = "localhost:50051";
  private final ManagedChannel channel;
  private QueryBlockingStub queryClient;
  private HealthGrpc.HealthBlockingStub healthCheckClient;

  public TestClient(String target)
  {
    // Access a service running on the local machine on port 50051
    this(target, null);
  }

  public TestClient(String target, String user, String password)
  {
    this(target, new BasicCredentials(user, password));
  }

  public TestClient(String target, CallCredentials callCreds)
  {
    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                  .build();
    queryClient = QueryGrpc.newBlockingStub(channel);
    healthCheckClient = HealthGrpc.newBlockingStub(channel);
    if (callCreds != null) {
      queryClient = queryClient.withCallCredentials(callCreds);
    }
  }

  public QueryBlockingStub getQueryClient()
  {
    return queryClient;
  }

  public HealthGrpc.HealthBlockingStub getHealthCheckClient()
  {
    return healthCheckClient;
  }

  public QueryBlockingStub client()
  {
    return queryClient;
  }

  @Override
  public void close()
  {
    // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
    // resources the channel should be shut down when it will no longer be used. If it may be used
    // again leave it running.
    try {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      // Ignore
    }
  }
}
