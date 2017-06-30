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
    @JsonSubTypes.Type(name = "noop", value = NoopAuthenticator.class),
})
public interface Authenticator extends ServletFilterHolder
{
  /**
   * @return The type name of this authenticator. Should be identical to the JsonTypeInfo type.
   */
  public String getTypeName();

  /**
   * @return The namespace associated with this Authenticator. This will be used for choosing the correct
   * AuthorizationManager for authorizing requests that have been authenticated by this Authenticator.
   */
  public String getNamespace();

  /**
   * Create a Filter that performs authentication checks on incoming HTTP requests.
   *
   * If the authentication succeeds, the Filter should set the "Druid-Auth-Token" attribute in the request,
   * containing a String that represents the authenticated identity of the requester.
   *
   * If the "Druid-Auth-Token" attribute is already set (i.e., request has been authenticated by an earlier Filter),
   * this Filter should skip any authentication checks and proceed to the next Filter.
   *
   * If the authentication fails, the Filter should not send an error response. The error response will be sent
   * after all Filters in the authentication filter chain have been checked.
   *
   * If an anonymous request is received, the Filter should continue on to the next Filter, the challenge response
   * will be sent after the filter chain is exhausted.
   *
   * @return Filter that authenticates HTTP requests
   */
  @Override
  public Filter getFilter();

  /**
   * Return a WWW-Authenticate challenge scheme string appropriate for this Authenticator's authentication mechanism.
   *
   * For example, a Basic HTTP implementation should return "Basic", while a Kerberos implementation would return
   * "Negotiate".
   *
   * @return Authentication scheme
   */
  public String getAuthChallengeHeader();

  /**
   * Given a JDBC connection context, authenticate the identity represented by the information in the context.
   * This is used to secure JDBC access for Druid SQL.
   *
   * For example, a Basic HTTP auth implementation could read the "user" and "password" fields from the JDBC context.
   *
   * The expected contents of the context are left to the implementation.
   *
   * @param context JDBC connection context
   * @return true if the identity represented by the context is successfully authenticated
   */
  public boolean authenticateJDBCContext(Map<String, Object> context);

  /**
   * Return a client that sends requests with the format/information necessary to authenticate successfully
   * against this Authenticator's authentication scheme using the identity of the internal system user.
   *
   * This HTTP client is used for internal communications between Druid nodes, such as when a broker communicates
   * with a historical node during query processing.
   *
   * @param baseClient Base HTTP client for internal Druid communications
   * @return HttpClient that sends requests with the credentials of the internal system user
   */
  public HttpClient createInternalClient(HttpClient baseClient);
}
