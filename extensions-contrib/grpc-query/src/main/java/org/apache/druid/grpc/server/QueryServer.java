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

import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import org.apache.druid.grpc.proto.HealthOuterClass.HealthCheckResponse.ServingStatus;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AnonymousAuthenticator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

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
  public static final Context.Key<AuthenticationResult> AUTH_KEY = Context.key("druid-auth");
  private static final Logger log = new Logger(QueryServer.class);

  private final AuthenticatorMapper authMapper;
  private final int port;
  private final QueryDriver driver;
  private Server server;

  private final HealthService healthService;

  public QueryServer(
      GrpcQueryConfig config,
      QueryDriver driver,
      AuthenticatorMapper authMapper
  )
  {
    this.port = config.getPort();
    this.driver = driver;
    this.authMapper = authMapper;
    this.healthService = new HealthService();
  }

  public void start() throws IOException
  {
    server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                 .addService(ServerInterceptors.intercept(new QueryService(driver), makeSecurityInterceptor()))
                 .addService(healthService)
                 .build()
                 .start();

    healthService.registerService(QueryService.class.getSimpleName(), ServingStatus.SERVING);
    healthService.registerService("", ServingStatus.SERVING);

    log.info("Grpc Server started, listening on " + port);
  }


  /**
   * Map from a Druid authenticator to a gRPC server interceptor. This is a bit of a hack.
   * Authenticators don't know about gRPC: we have to explicitly do the mapping. This means
   * that auth extensions occur independently of gRPC and are not supported. Longer term,
   * we need a way for the extension itself to do the required mapping.
   */
  private ServerInterceptor makeSecurityInterceptor()
  {
    // First look for a Basic authenticator
    for (Authenticator authenticator : authMapper.getAuthenticatorChain()) {
      // Want authenticator instanceof BasicHTTPAuthenticator, but
      // BasicHTTPAuthenticator is not visible here.
      if ("BasicHTTPAuthenticator".equals(authenticator.getClass().getSimpleName())) {
        log.info("Using Basic authentication");
        return new BasicAuthServerInterceptor(authenticator);
      }
    }

    // Otherwise, look for an Anonymous authenticator
    for (Authenticator authenticator : authMapper.getAuthenticatorChain()) {
      if (authenticator instanceof AnonymousAuthenticator || authenticator instanceof AllowAllAuthenticator) {
        log.info("Using Anonymous authentication");
        return new AnonymousAuthServerInterceptor(authenticator);
      }
    }

    // gRPC does not support other forms of authenticators yet.
    String msg = "The gRPC query server requires either a Basic or Anonymous authorizer: it does not work with others yet.";
    log.error(msg);
    throw new UOE(msg);
  }

  public void stop() throws InterruptedException
  {
    if (server != null) {
      log.info("Server stopping");
      healthService.unregisterService(QueryService.class.getSimpleName());
      healthService.unregisterService("");
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  public void blockUntilShutdown() throws InterruptedException
  {
    if (server != null) {
      log.info("Grpc Server stopping");
      healthService.unregisterService(QueryService.class.getSimpleName());
      healthService.unregisterService("");
      server.awaitTermination();
    }
  }
}
