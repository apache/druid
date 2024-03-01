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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.inject.Inject;

/**
 * Authorizes a Basic Auth user name and password and sets the resulting
 * {@link AuthenticationResult} on the call context.
 * <p>
 * Implements the gRPC {@link ServerInterceptor} to wrap the actual RPC
 * call with a step which pulls the "Authorization" header from the request,
 * decodes the user name and password, looks up the user using the
 * BasicHTTPAuthenticator#authenticateJDBCContext(java.util.Map)
 * method, and attaches the resulting {@link AuthenticationResult} to the call
 * {@link Context}. The gRPC service will later retrieve the auth result to pass
 * into the Driver for use in validating query resources.
 * <p>
 * Note that gRPC documentation in this area is sparse. Examples are hard to
 * find. gRPC provides exactly one (obscure) way to do things, as represented
 * here.
 * <p>
 * Auth failures can occur in many ways: missing or badly formed header, invalid
 * user name or password, etc. In each case, the code throws a
 * {@link StatusRuntimeException} with {@link Status#PERMISSION_DENIED}. No hint
 * of the problem is provided to the user.
 * <p>
 * This pattern can be replicated for other supported Druid authorizers.
 */
public class BasicAuthServerInterceptor implements ServerInterceptor
{
  public static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BASIC_PREFIX = "Basic ";
  private static final Metadata.Key<String> AUTHORIZATION_KEY =
      Metadata.Key.of(AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER);
  private static final Logger LOG = new Logger(BasicAuthServerInterceptor.class);

  // Want BasicHTTPAuthenticator, but it is not visible here.
  private final Authenticator authenticator;

  @Inject
  public BasicAuthServerInterceptor(Authenticator authenticator)
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
    // Use a gRPC method to wrap the actual call in a new context
    // that includes the auth result.
    return Contexts.interceptCall(
        Context.current().withValue(
            QueryServer.AUTH_KEY,
            authenticate(headers.get(AUTHORIZATION_KEY))
        ),
        call,
        headers,
        next
    );
  }

  // See BasicHTTPAuthenticator.Filter
  public AuthenticationResult authenticate(String encodedUserSecret)
  {
    if (encodedUserSecret == null) {
      throw new StatusRuntimeException(Status.PERMISSION_DENIED);
    }

    if (!encodedUserSecret.startsWith(BASIC_PREFIX)) {
      throw new StatusRuntimeException(Status.PERMISSION_DENIED);
    }
    encodedUserSecret = encodedUserSecret.substring(BASIC_PREFIX.length());

    // At this point, encodedUserSecret is not null, indicating that the request intends to perform
    // Basic HTTP authentication.
    // Copy of BasicAuthUtils.decodeUserSecret() which is not visible here.
    String decodedUserSecret;
    try {
      decodedUserSecret = StringUtils.fromUtf8(StringUtils.decodeBase64String(encodedUserSecret));
    }
    catch (IllegalArgumentException iae) {
      LOG.info("Malformed user secret.");
      throw new StatusRuntimeException(Status.PERMISSION_DENIED);
    }

    String[] splits = decodedUserSecret.split(":");
    if (splits.length != 2) {
      // The decoded user secret is not of the right format
      throw new StatusRuntimeException(Status.PERMISSION_DENIED);
    }

    final String user = splits[0];
    final String password = splits[1];

    // Fail fast for any authentication error. If the authentication result is null we also fail
    // as this indicates a non-existent user.
    try {
      AuthenticationResult authenticationResult = authenticator.authenticateJDBCContext(
          ImmutableMap.of("user", user, "password", password)
      );
      if (authenticationResult == null) {
        throw new StatusRuntimeException(Status.PERMISSION_DENIED);
      }
      return authenticationResult;
    }
    // Want BasicSecurityAuthenticationException, but it is not visible here.
    catch (IllegalArgumentException ex) {
      LOG.info("Exception authenticating user [%s] - [%s]", user, ex.getMessage());
      throw new StatusRuntimeException(Status.PERMISSION_DENIED);
    }
  }
}
