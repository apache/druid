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
import org.apache.druid.guice.ExtensionsLoader;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.SqlStatementFactory;

import javax.inject.Inject;

@ManageLifecycle
public class GrpcEndpointInitializer
{
  private static final Logger log = new Logger(GrpcEndpointInitializer.class);

  /**
   * Name of the extension. Must match the name in the
   * {@code $DRUID_HOME/extensions} directory.
   */
  public static final String EXTENSION_NAME = "grpc-query";

  private final GrpcQueryConfig config;
//  private final QueryDriver driver;
  private final ClassLoader extnClassLoader;
  private final ObjectMapper jsonMapper;
  private final SqlStatementFactory sqlStatementFactory;

  private QueryServer server;

  @Inject
  public GrpcEndpointInitializer(
      GrpcQueryConfig config,
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory,
      final ExtensionsLoader extensionsLoader
  )
  {
    this.config = config;
//    this.driver = new QueryDriver(jsonMapper, sqlStatementFactory);
    this.jsonMapper = jsonMapper;
    this.sqlStatementFactory = sqlStatementFactory;

    // Retrieve the class loader for this extension. Necessary because, in an IDE,
    // the class loader for the extension's files will be the AppClassLoader since
    // the IDE puts our classes on the class path.
    this.extnClassLoader = extensionsLoader.getClassLoaderForExtension(EXTENSION_NAME);
    if (this.extnClassLoader == null) {
      throw new ISE("No extension class loader for %s: wrong name?", EXTENSION_NAME);
    }
  }

  @LifecycleStart
  public void start()
  {
//    ClassLoader thisLoader = getClass().getClassLoader();
    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(extnClassLoader);
//      try {
//        Class<?> found = extnClassLoader.loadClass("io.netty.handler.codec.http2.Http2Headers");
//      } catch (ClassNotFoundException e) {
//        throw new ISE(e, "Fatal error: grpc query server startup failed");
//      }

      try {
        QueryDriverImpl driver = new QueryDriverImpl(jsonMapper, sqlStatementFactory);
        Class<?> serverClass = extnClassLoader.loadClass("org.apache.druid.grpc.server.QueryServer");
        Class<?> driverClass = extnClassLoader.loadClass("org.apache.druid.grpc.server.QueryDriver");
//        Object driver = driverClass
//            .getConstructor(ObjectMapper.class, SqlStatementFactory.class)
//            .newInstance(jsonMapper, sqlStatementFactory);
//        Class<?> builderClass = extnClassLoader.loadClass("org.apache.druid.grpc.server.ServerBuilder");
//        ServerBuilder builder = builderClass.getConstructor().newInstance();
//        server = builder.buildServer(config.getPort(), driver);
        server = (QueryServer) serverClass
            .getConstructor(Integer.class, driverClass)
            .newInstance(config.getPort(), driver);

//        server = new QueryServer(config.getPort(), driver);
        server.start();
      } catch (Exception e) {
        log.error(e, "Fatal error: gRPC query server startup failed");
        throw new ISE(e, "Fatal error: grpc query server startup failed");
      } catch (Throwable t) {
        log.error(t, "Fatal error: gRPC query server startup failed");
        throw t;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
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
