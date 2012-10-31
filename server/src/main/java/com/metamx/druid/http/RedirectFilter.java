/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.http;

import java.io.IOException;
import java.net.URL;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.HttpResponseHandler;

/**
 */
public class RedirectFilter implements Filter
{
  private static final Logger log = new Logger(RedirectFilter.class);

  private final HttpClient httpClient;
  private final HttpResponseHandler<StringBuilder, String> responseHandler;
  private final RedirectInfo redirectInfo;

  public RedirectFilter(
      HttpClient httpClient,
      HttpResponseHandler<StringBuilder, String> responseHandler,
      RedirectInfo redirectInfo
  )
  {
    this.httpClient = httpClient;
    this.responseHandler = responseHandler;
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
      log.info("Forwarding request to [%s]", url);

      if (request.getMethod().equals(HttpMethod.POST)) {
        try {
          forward(request, url);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      } else {
        response.sendRedirect(url.toString());
      }
    }
  }

  @Override
  public void destroy() {}

  private void forward(HttpServletRequest req, URL url) throws Exception
  {
    byte[] requestQuery = ByteStreams.toByteArray(req.getInputStream());
    httpClient.post(url)
              .setContent("application/json", requestQuery)
              .go(responseHandler)
              .get();
  }
}
