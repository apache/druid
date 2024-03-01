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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.grpc.server.GrpcEndpointInitializer;
import org.apache.druid.grpc.server.GrpcQueryConfig;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

import java.io.File;

/**
 * Super-simple test server that uses the CalciteTests setup.
 */
public class TestServer
{
  private static QueryFrameworkFixture frameworkFixture;
  private GrpcEndpointInitializer serverInit;

  public static void main(String[] args)
  {
    new TestServer().run();
  }

  public void run()
  {
    frameworkFixture = new QueryFrameworkFixture(new File("/tmp/druid"));
    GrpcQueryConfig config = new GrpcQueryConfig(50051);
    AuthenticatorMapper authMapper = new AuthenticatorMapper(
        ImmutableMap.of(
            "test",
            new AllowAllAuthenticator()
        )
    );
    serverInit = new GrpcEndpointInitializer(
        config,
        frameworkFixture.jsonMapper(),
        frameworkFixture.statementFactory(),
        null,
        authMapper
    );
    serverInit.start();
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        serverInit.stop();
      }
    });
  }
}
