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
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.SqlStatementFactory;

import javax.inject.Inject;

import java.io.IOException;

@ManageLifecycle
public class GrpcEndpointInitializer
{
  private static final Logger log = new Logger(GrpcEndpointInitializer.class);

  private final GrpcQueryConfig config;
  private final QueryDriver driver;

  private QueryServer server;

  @Inject
  public GrpcEndpointInitializer(
      GrpcQueryConfig config,
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory
  )
  {
    this.config = config;
    this.driver = new QueryDriver(jsonMapper, sqlStatementFactory);
  }

  @LifecycleStart
  public void start()
  {
//    String foo = io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName.METHOD.name();
//    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
//
//    try {
//      Thread.currentThread().setContextClassLoader(GrpcEndpointInitializer.class.getClassLoader());

      server = new QueryServer(config.getPort(), driver);
      try {
        server.start();
      } catch (IOException e) {
        log.error(e, "Fatal error: gRPC query server startup failed");
        throw new ISE(e, "Fatal error: grpc query server startup failed");
      } catch (Throwable t) {
        log.error(t, "Fatal error: gRPC query server startup failed");
        throw t;
      }
//    } finally {
//      Thread.currentThread().setContextClassLoader(oldLoader);
//    }
  }

  @LifecycleStop
  public void stop()
  {
    if (server != null) {
      try {
        server.blockUntilShutdown();
      } catch (InterruptedException e) {
        log.warn(e, "gRPC query server shutdown failed");
      }
      server = null;
    }
  }
}
