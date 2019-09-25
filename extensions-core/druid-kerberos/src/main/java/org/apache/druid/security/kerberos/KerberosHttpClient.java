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

package org.apache.druid.security.kerberos;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.AbstractHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;

import java.net.CookieManager;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class KerberosHttpClient extends AbstractHttpClient
{
  private static final Logger log = new Logger(KerberosHttpClient.class);

  private final HttpClient delegate;
  private final CookieManager cookieManager;
  private final Executor exec = Execs.singleThreaded("test-%s");
  private final String internalClientPrincipal;
  private final String internalClientKeytab;

  public KerberosHttpClient(HttpClient delegate, String internalClientPrincipal, String internalClientKeytab)
  {
    this.delegate = delegate;
    this.cookieManager = new CookieManager();
    this.internalClientPrincipal = internalClientPrincipal;
    this.internalClientKeytab = internalClientKeytab;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> httpResponseHandler,
      Duration duration
  )
  {
    final SettableFuture<Final> retVal = SettableFuture.create();
    inner_go(request, httpResponseHandler, duration, retVal);
    return retVal;
  }


  private <Intermediate, Final> void inner_go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> httpResponseHandler,
      final Duration duration,
      final SettableFuture<Final> future
  )
  {
    try {
      final String host = request.getUrl().getHost();
      final URI uri = request.getUrl().toURI();


      Map<String, List<String>> cookieMap = cookieManager.get(uri, Collections.emptyMap());
      for (Map.Entry<String, List<String>> entry : cookieMap.entrySet()) {
        request.addHeaderValues(entry.getKey(), entry.getValue());
      }
      final boolean should_retry_on_unauthorized_response;

      if (DruidKerberosUtil.needToSendCredentials(cookieManager.getCookieStore(), uri)) {
        // No Cookies for requested URI, authenticate user and add authentication header
        log.debug(
            "No Auth Cookie found for URI[%s]. Existing Cookies[%s] Authenticating... ",
            uri,
            cookieManager.getCookieStore().getCookies()
        );
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
        request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challenge);
        should_retry_on_unauthorized_response = false;
      } else {
        should_retry_on_unauthorized_response = true;
        log.debug("Found Auth Cookie found for URI[%s].", uri);
      }

      ListenableFuture<RetryResponseHolder<Final>> internalFuture = delegate.go(
          request,
          new RetryIfUnauthorizedResponseHandler<Intermediate, Final>(
              new ResponseCookieHandler(request.getUrl().toURI(), cookieManager, httpResponseHandler)
          ),
          duration
      );

      Futures.addCallback(internalFuture, new FutureCallback<RetryResponseHolder<Final>>()
      {
        @Override
        public void onSuccess(RetryResponseHolder<Final> result)
        {
          if (should_retry_on_unauthorized_response && result.shouldRetry()) {
            log.info("Preparing for Retry");
            // remove Auth cookie
            DruidKerberosUtil.removeAuthCookie(cookieManager.getCookieStore(), uri);
            // clear existing cookie
            request.setHeader("Cookie", "");
            inner_go(request.copy(), httpResponseHandler, duration, future);
          } else {
            log.debug("Not retrying and returning future response");
            future.set(result.getObj());
          }
        }

        @Override
        public void onFailure(Throwable t)
        {
          future.setException(t);
        }
      }, exec);
    }
    catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

}
