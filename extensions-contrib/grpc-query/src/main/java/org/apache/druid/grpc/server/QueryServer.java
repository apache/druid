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

package org.apache.druid.grpc.server;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.druid.grpc.proto.QueryGrpc;
import org.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.druid.grpc.proto.QueryOuterClass.QueryResponse;

import javax.inject.Inject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class QueryServer
{
  private static final Logger logger = Logger.getLogger(QueryServer.class.getName());

  private final QueryDriver driver;
  private Server server;

  @Inject
  public QueryServer(QueryDriver driver)
  {
    this.driver = driver;
  }

  public void start() throws IOException {
    /* The port on which the server should run */
    int port = 50051;
    server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(new QueryImpl(driver))
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          QueryServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("*** server shut down");
      }
    });
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  static class QueryImpl extends QueryGrpc.QueryImplBase {

    private final QueryDriver driver;

    public QueryImpl(QueryDriver driver)
    {
      this.driver = driver;

    }
    @Override
    public void submitQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver)
    {
      QueryResponse reply = driver.submitQuery(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}
