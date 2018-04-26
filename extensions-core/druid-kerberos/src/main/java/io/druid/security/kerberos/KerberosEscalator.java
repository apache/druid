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

package io.druid.security.kerberos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Escalator;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.HttpCookie;
import java.util.stream.Collectors;

@JsonTypeName("kerberos")
public class KerberosEscalator implements Escalator
{
  private static final Logger log = new Logger(KerberosAuthenticator.class);
  public static final String SIGNED_TOKEN_ATTRIBUTE = "signedToken";

  private final String internalClientPrincipal;
  private final String internalClientKeytab;
  private final String authorizerName;

  @JsonCreator
  public KerberosEscalator(
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("internalClientPrincipal") String internalClientPrincipal,
      @JsonProperty("internalClientKeytab") String internalClientKeytab
  )
  {
    this.authorizerName = authorizerName;
    this.internalClientPrincipal = internalClientPrincipal;
    this.internalClientKeytab = internalClientKeytab;

  }
  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return new KerberosHttpClient(baseClient, internalClientPrincipal, internalClientKeytab);
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return new AuthenticationResult(internalClientPrincipal, authorizerName, null);
  }

  @Override
  public void decorateProxyRequest(
      HttpServletRequest clientRequest, HttpServletResponse proxyResponse, Request proxyRequest
  )
  {
    Object cookieToken = clientRequest.getAttribute(SIGNED_TOKEN_ATTRIBUTE);
    if (cookieToken != null && cookieToken instanceof String) {
      log.debug("Found cookie token will attache it to proxyRequest as cookie");
      String authResult = (String) cookieToken;
      String existingCookies = proxyRequest.getCookies()
                                           .stream()
                                           .map(HttpCookie::toString)
                                           .collect(Collectors.joining(";"));
      proxyRequest.header(HttpHeader.COOKIE, Joiner.on(";").join(authResult, existingCookies));
    }
  }
}
