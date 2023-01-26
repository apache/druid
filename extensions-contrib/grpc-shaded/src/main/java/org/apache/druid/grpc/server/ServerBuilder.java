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

import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

/**
 * Builds the Grpc Server given a port. This builds a Netty-based server
 * internally. Done in the shaded jar so that this class can be created
 * using the extension class loader, so that the gRPC classes are also
 * created using that class loader which allows gRPC to find its components.
 * The rest of the server is created implicitly which will use the
 * the extension class loader in production, the app loader when debugging.
 * If you find you are getting errors about classes not found, or arguments
 * not matching, it is because you have instances created in the app class
 * loader which don't have visibility to classes in this extension.
 */
public class ServerBuilder
{
  public Server buildServer(int port, BindableService service)
  {
    return Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
        .addService(service)
        .build();
  }
}
