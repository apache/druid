/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.http;

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

/**
 */
public class RedirectFilter implements Filter
{
  private static final Logger log = new Logger(RedirectFilter.class);

  private final RedirectInfo redirectInfo;

  @Inject
  public RedirectFilter(
      RedirectInfo redirectInfo
  )
  {
    this.redirectInfo = redirectInfo;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
      throws IOException, ServletException
  {
    HttpServletRequest request;
    HttpServletResponse response;

    try {
      request = (HttpServletRequest) req;
      response = (HttpServletResponse) res;
    }
    catch (ClassCastException e) {
      throw new ServletException("non-HTTP request or response");
    }

    if (redirectInfo.doLocal()) {
      chain.doFilter(request, response);
    } else {
      URL url = redirectInfo.getRedirectURL(request.getQueryString(), request.getRequestURI());
      log.debug("Forwarding request to [%s]", url);

      if (url == null) {
        // We apparently have no coordinator, so let's do a Service Unavailable
        response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        return;
      }

      response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
      response.setHeader("Location", url.toString());
    }
  }

  @Override
  public void destroy() {}
}
