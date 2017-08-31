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
import com.metamx.http.client.HttpClient;
import io.druid.server.initialization.jetty.ServletFilterHolder;

import javax.servlet.Filter;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "allowAll", value = AllowAllAuthenticator.class),
})
/**
 * This interface is essentially a ServletFilterHolder with additional requirements on the getFilter() method contract, plus:
 *
 * - A method that returns a WWW-Authenticate challenge header appropriate for the
 *   authentication mechanism, getAuthChallengeHeader().
 * - A method for creating a wrapped HTTP client that can authenticate using the Authenticator's authentication scheme,
 *   used for internal Druid node communications (e.g., broker -> historical messages), createEscalatedClient().
 * - A method for creating a wrapped Jetty HTTP client that can authenticate using the Authenticator's authentication scheme,
 *   used by the Druid router, createEscalatedJettyClient().
 * - A method for authenticating credentials contained in a JDBC connection context, used for authenticating Druid SQL
 *   requests received via JDBC, authenticateJDBCContext().
 */
public interface Authenticator extends ServletFilterHolder
{
  /**
   * Create a Filter that performs authentication checks on incoming HTTP requests.
   * <p>
   * If the authentication succeeds, the Filter should set the "Druid-Auth-Token" attribute in the request,
   * containing a String that represents the authenticated identity of the requester.
   * <p>
   * If the "Druid-Auth-Token" attribute is already set (i.e., request has been authenticated by an earlier Filter),
   * this Filter should skip any authentication checks and proceed to the next Filter.
   * <p>
   * If the authentication fails, the Filter should not send an error response. The error response will be sent
   * after all Filters in the authentication filter chain have been checked.
   * <p>
   * If an anonymous request is received, the Filter should continue on to the next Filter, the challenge response
   * will be sent after the filter chain is exhausted.
   *
   * @return Filter that authenticates HTTP requests
   */
  @Override
  public Filter getFilter();

  /**
   * Return a WWW-Authenticate challenge scheme string appropriate for this Authenticator's authentication mechanism.
   * <p>
   * For example, a Basic HTTP implementation should return "Basic", while a Kerberos implementation would return
   * "Negotiate".
   *
   * @return Authentication scheme
   */
  public String getAuthChallengeHeader();

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
   * @return true if the identity represented by the context is successfully authenticated
   */
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context);

  /**
   * Return a client that sends requests with the format/information necessary to authenticate successfully
   * against this Authenticator's authentication scheme using the identity of the internal system user.
   * <p>
   * This HTTP client is used for internal communications between Druid nodes, such as when a broker communicates
   * with a historical node during query processing.
   *
   * @param baseClient Base HTTP client for internal Druid communications
   *
   * @return metamx HttpClient that sends requests with the credentials of the internal system user
   */
  public HttpClient createEscalatedClient(HttpClient baseClient);

  /**
   * Return a client that sends requests with the format/information necessary to authenticate successfully
   * against this Authenticator's authentication scheme using the identity of the internal system user.
   * <p>
   * This HTTP client is used by the Druid Router node.
   *
   * @param baseClient Base Jetty HttpClient
   *
   * @return Jetty HttpClient that sends requests with the credentials of the internal system user
   */
  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient);

  /**
   * @return an AuthenticationResult representing the identity of the internal system user.
   */
  public AuthenticationResult createEscalatedAuthenticationResult();
}
