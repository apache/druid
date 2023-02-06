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
import org.apache.druid.java.util.common.logger.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * Basic gRPC server adapted from the gRPC examples. Delegates to the
 * {@link QueryDriver} class to do the actual work of running the query.
 * <p>
 * This class is preliminary. It is good enough for unit tests, but a bit more work
 * is needed to integrate this class into the Druid server.
 * <p>
 * Also, how will authorization be handled in the gRPC path?
 */
public class QueryServer
{
  private static final Logger log = new Logger(QueryServer.class);

  private final int port;
  private final QueryDriver driver;
  private Server server;

  @Inject
  public QueryServer(
      Integer port,
      QueryDriver driver
  )
  {
    this.port = port;
    this.driver = driver;
  }

  public void start() throws IOException
  {
    server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
            .addService(new QueryService(driver))
            .build()
            .start();
    log.info("Server started, listening on " + port);
  }

  public void stop() throws InterruptedException
  {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  public void blockUntilShutdown() throws InterruptedException
  {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
