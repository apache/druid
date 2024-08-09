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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.ManageLifecycleServer;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.SqlStatementFactory;

import javax.inject.Inject;

import java.io.IOException;

/**
 * Initializes the gRPC endpoint (server). This version uses a Netty-based server
 * separate from Druid's primary Jetty-based server. We may want to consider a
 * <link a="https://github.com/grpc/grpc-java/tree/master/examples/example-servlet">
 * recent addition to the gRPC examples</a> to run gRPC as a servlet. However, trying
 * that turned out to incur many issues, including the fact that there was no way
 * to pass the AuthenticationResult down through the many layers of gRPC into the
 * query code. So, we use the gRPC server instead.
 * <p>
 * An instance of this class is created by Guice and managed via Druid's
 * lifecycle manager.
 */
@ManageLifecycleServer
public class GrpcEndpointInitializer
{
  private static final Logger log = new Logger(GrpcEndpointInitializer.class);

  private final GrpcQueryConfig config;
  private final QueryDriver driver;
  private final AuthenticatorMapper authMapper;

  private QueryServer server;

  @Inject
  public GrpcEndpointInitializer(
      GrpcQueryConfig config,
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory,
      final QueryLifecycleFactory queryLifecycleFactory,
      final AuthenticatorMapper authMapper
  )
  {
    this.config = config;
    this.authMapper = authMapper;
    this.driver = new QueryDriver(jsonMapper, sqlStatementFactory, queryLifecycleFactory);
  }

  @LifecycleStart
  public void start()
  {
    server = new QueryServer(config, driver, authMapper);
    try {
      server.start();
    }
    catch (IOException e) {
      // Indicates an error when gRPC tried to start the server
      // (such the port is already in use.)
      log.error(e, "Fatal error: gRPC query server startup failed");

      // This exception will bring down the Broker as there is not much we can
      // do if we can't start the gRPC endpoint.
      throw new ISE(e, "Fatal error: grpc query server startup failed");
    }
    catch (Throwable t) {
      // Catch-all for other errors. The most likely error is that some class was not found
      // (that is, class loader issues in an IDE, or a jar missing in the extension).
      log.error(t, "Fatal error: gRPC query server startup failed");

      // This exception will bring down the Broker as there is not much we can
      // do if we can't start the gRPC endpoint.
      throw t;
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (server != null) {
      try {
        server.blockUntilShutdown();
      }
      catch (InterruptedException e) {
        // Just warn. We're shutting down anyway, so no need to throw an exception.
        log.warn(e, "gRPC query server shutdown failed");
      }
      server = null;
    }
  }
}
