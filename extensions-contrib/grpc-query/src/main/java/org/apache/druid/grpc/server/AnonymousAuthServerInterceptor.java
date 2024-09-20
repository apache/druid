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

import com.google.common.collect.ImmutableMap;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.druid.server.security.Authenticator;

import javax.inject.Inject;

/**
 * "Authorizes" an anonymous request, which just means adding an "allow all"
 * authorization result in the context. Use this form for either of Druid's
 * "allow all" authorizers.
 *
 * @see {@link BasicAuthServerInterceptor} for details
 */
public class AnonymousAuthServerInterceptor implements ServerInterceptor
{
  private final Authenticator authenticator;

  @Inject
  public AnonymousAuthServerInterceptor(Authenticator authenticator)
  {
    this.authenticator = authenticator;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call,
      Metadata headers,
      ServerCallHandler<ReqT, RespT> next
  )
  {
    return Contexts.interceptCall(
        Context.current().withValue(
            QueryServer.AUTH_KEY,
            authenticator.authenticateJDBCContext(ImmutableMap.of())
        ),
        call,
        headers,
        next
    );
  }
}
