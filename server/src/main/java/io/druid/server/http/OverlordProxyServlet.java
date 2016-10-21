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

import io.druid.client.indexing.IndexingService;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.java.util.common.ISE;

import org.eclipse.jetty.proxy.ProxyServlet;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A Proxy servlet that proxies requests to the overlord.
 */
public class OverlordProxyServlet extends ProxyServlet
{
  private final ServerDiscoverySelector selector;

  @Inject
  OverlordProxyServlet(
      @IndexingService ServerDiscoverySelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  protected URI rewriteURI(HttpServletRequest request)
  {
    try {
      final Server indexer = selector.pick();
      if (indexer == null) {
        throw new ISE("Can't find indexingService, did you configure druid.selectors.indexing.serviceName same as druid.service at overlord?");
      }
      return new URI(
          request.getScheme(),
          indexer.getHost(),
          request.getRequestURI(),
          request.getQueryString(),
          null
      );
    }
    catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }
}
