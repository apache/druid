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
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.http.client.AbstractHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.java.util.common.logger.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;

import java.security.PrivilegedExceptionAction;

public class KerberosHttpClient extends AbstractHttpClient
{
  private final HttpClient delegate;
  private final DruidKerberosConfig config;


  public KerberosHttpClient(HttpClient delegate, DruidKerberosConfig config)
  {
    this.delegate = delegate;
    this.config = config;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
    Request request, HttpResponseHandler<Intermediate, Final> httpResponseHandler, Duration duration
  )
  {
    try {
      final String host = request.getUrl().getHost();
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
      request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challenge);
    }
    catch (Throwable e) {
      Throwables.propagate(e);
    }
    return delegate.go(request, httpResponseHandler, duration);
  }

}
