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

package io.druid.server.http;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.BrokerService;
import org.eclipse.jetty.proxy.ProxyServlet;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A Proxy servlet that proxies requests to the overlord.
 */
public class BrokerProxyServlet extends ProxyServlet
{
  private final ServerDiscoverySelector selector;

  private static final String BROKER_PREFIX = "/broker";

  @Inject
  BrokerProxyServlet(
      @BrokerService ServerDiscoverySelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    try {
      final Server broker = selector.pick();
      if (broker == null) {
        throw new ISE(
            "Can't find brokers. Did you configure druid.selectors.broker.serviceName same as druid.service at brokers?"
        );
      }

      String requestUri = request.getRequestURI();
      if (requestUri.startsWith(BROKER_PREFIX)) {
        requestUri = requestUri.substring(BROKER_PREFIX.length());
      } else {
        throw new ISE("illegal uri [%s], must start with /broker", requestUri);
      }

      return new URI(
          request.getScheme(),
          broker.getHost(),
          requestUri,
          request.getQueryString(),
          null
      ).toString();
    }
    catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }
}
