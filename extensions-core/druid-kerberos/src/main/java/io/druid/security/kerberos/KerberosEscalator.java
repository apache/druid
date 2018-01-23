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
import com.google.common.base.Throwables;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Escalator;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Attributes;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.net.URI;
import java.security.PrivilegedExceptionAction;

@JsonTypeName("kerberos")
public class KerberosEscalator implements Escalator
{
  private static final Logger log = new Logger(KerberosAuthenticator.class);

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
  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient)
  {
    baseClient.getAuthenticationStore().addAuthentication(new Authentication()
    {
      @Override
      public boolean matches(String type, URI uri, String realm)
      {
        return true;
      }

      @Override
      public Result authenticate(
          final Request request, ContentResponse response, Authentication.HeaderInfo headerInfo, Attributes context
      )
      {
        return new Result()
        {
          @Override
          public URI getURI()
          {
            return request.getURI();
          }

          @Override
          public void apply(Request request)
          {
            try {
              // No need to set cookies as they are handled by Jetty Http Client itself.
              URI uri = request.getURI();
              if (DruidKerberosUtil.needToSendCredentials(baseClient.getCookieStore(), uri)) {
                log.debug(
                    "No Auth Cookie found for URI[%s]. Existing Cookies[%s] Authenticating... ",
                    uri,
                    baseClient.getCookieStore().getCookies()
                );
                final String host = request.getHost();
                DruidKerberosUtil.authenticateIfRequired(internalClientPrincipal, internalClientKeytab);
                UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                String challenge = currentUser.doAs(new PrivilegedExceptionAction<String>()
                {
                  @Override
                  public String run() throws Exception
                  {
                    return DruidKerberosUtil.kerberosChallenge(host);
                  }
                });
                request.getHeaders().add(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challenge);
              } else {
                log.debug("Found Auth Cookie found for URI[%s].", uri);
              }
            }
            catch (Throwable e) {
              Throwables.propagate(e);
            }
          }
        };
      }
    });
    return baseClient;
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return new AuthenticationResult(internalClientPrincipal, authorizerName, null);
  }
}
