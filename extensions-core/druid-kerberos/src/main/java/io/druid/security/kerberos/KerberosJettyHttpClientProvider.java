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


import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.druid.guice.http.AbstractHttpClientProvider;
import io.druid.java.util.common.logger.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Attributes;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.net.URI;
import java.security.PrivilegedExceptionAction;

public class KerberosJettyHttpClientProvider extends AbstractHttpClientProvider<HttpClient>
{
  private static final Logger log = new Logger(KerberosJettyHttpClientProvider.class);

  private final Provider<HttpClient> delegateProvider;
  private AuthenticationKerberosConfig config;


  public KerberosJettyHttpClientProvider(
    Provider<HttpClient> delegateProvider
  )
  {
    this.delegateProvider = delegateProvider;
  }

  @Inject
  @Override
  public void configure(Injector injector)
  {
    if (delegateProvider instanceof AbstractHttpClientProvider) {
      ((AbstractHttpClientProvider) delegateProvider).configure(injector);
    }
    config = injector.getInstance(AuthenticationKerberosConfig.class);
  }


  @Override
  public HttpClient get()
  {
    final HttpClient httpClient = delegateProvider.get();
    httpClient.getAuthenticationStore().addAuthentication(new Authentication()
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
              if (DruidKerberosUtil.needToSendCredentials(httpClient.getCookieStore(), uri)) {
                log.debug(
                  "No Auth Cookie found for URI[%s]. Existing Cookies[%s] Authenticating... ",
                  uri,
                  httpClient.getCookieStore().getCookies()
                );
                final String host = request.getHost();
                DruidKerberosUtil.authenticateIfRequired(config);
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
    return httpClient;
  }
}
