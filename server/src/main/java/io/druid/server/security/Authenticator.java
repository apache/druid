/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.security;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.server.initialization.jetty.ServletFilterHolder;
import org.eclipse.jetty.client.api.Request;

import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = AuthConfig.ALLOW_ALL_NAME, value = AllowAllAuthenticator.class),
})
/**
 * This interface is essentially a ServletFilterHolder with additional requirements on the getFilter() method contract, plus:
 *
 * - A method that returns a WWW-Authenticate challenge header appropriate for the
 *   authentication mechanism, getAuthChallengeHeader().
 * - A method for authenticating credentials contained in a JDBC connection context, used for authenticating Druid SQL
 *   requests received via JDBC, authenticateJDBCContext().
 */
public interface Authenticator extends ServletFilterHolder
{
  /**
   * Create a Filter that performs authentication checks on incoming HTTP requests.
   * <p>
   * If the authentication succeeds, the Filter should set the "Druid-Authentication-Result" attribute in the request,
   * containing an AuthenticationResult that represents the authenticated identity of the requester, along with
   * the name of the Authorizer instance that should authorize the request. An Authenticator may choose to
   * add a Map<String, Object> context to the authentication result, containing additional information to be
   * used by the Authorizer. The contents of this map are left for Authenticator/Authorizer implementors to decide.
   * <p>
   * If the "Druid-Authentication-Result" attribute is already set (i.e., request has been authenticated by an
   * earlier Filter), this Filter should skip any authentication checks and proceed to the next Filter.
   * <p>
   * If a filter cannot recognize a request's format (e.g., the request does not have credentials compatible
   * with a filter's authentication scheme), the filter should not send an error response, allowing other
   * filters to handle the request. A challenge response will be sent if the filter chain is exhausted.
   * <p>
   * If the authentication fails (i.e., a filter recognized the authentication scheme of a request, but the credentials
   * failed to authenticate successfully) the Filter should send an error response, without needing to proceed to
   * other filters in the chain..
   *
   * @return Filter that authenticates HTTP requests
   */
  @Override
  Filter getFilter();

  /**
   * Return a WWW-Authenticate challenge scheme string appropriate for this Authenticator's authentication mechanism.
   * <p>
   * For example, a Basic HTTP implementation should return "Basic", while a Kerberos implementation would return
   * "Negotiate". If this method returns null, no authentication scheme will be added for that Authenticator
   * implementation.
   *
   * @return Authentication scheme
   */
  @Nullable
  String getAuthChallengeHeader();

  /**
   * Given a JDBC connection context, authenticate the identity represented by the information in the context.
   * This is used to secure JDBC access for Druid SQL.
   * <p>
   * For example, a Basic HTTP auth implementation could read the "user" and "password" fields from the JDBC context.
   * <p>
   * The expected contents of the context are left to the implementation.
   *
   * @param context JDBC connection context
   *
   * @return AuthenticationResult of the identity represented by the context is successfully authenticated,
   *         null if authentication failed
   */
  @Nullable
  AuthenticationResult authenticateJDBCContext(Map<String, Object> context);


  /**
   * This is used to add some Headers or Authentication token/results that can be used by down stream target host.
   * Such token can be used to authenticate the user down stream, in cases where to original credenitals
   * are not forwardable as is and therefore the need to attach some authentication tokens by the proxy.
   *
   * @param clientRequest original client request processed by the upstream chain of authenticator
   * @param proxyResponse proxy Response
   * @param proxyRequest actual proxy request targeted to a given broker
   */
  default void decorateProxyRequest(
      final HttpServletRequest clientRequest,
      final HttpServletResponse proxyResponse,
      final Request proxyRequest
  )
  {
    //noop
  }
}
