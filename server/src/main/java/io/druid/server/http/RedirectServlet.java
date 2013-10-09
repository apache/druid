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
import org.eclipse.jetty.servlet.DefaultServlet;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

/**
 */
public class RedirectServlet extends DefaultServlet
{
  private static final Logger log = new Logger(RedirectServlet.class);

  private final RedirectInfo redirectInfo;

  @Inject
  public RedirectServlet(
      RedirectInfo redirectInfo
  )
  {
    this.redirectInfo = redirectInfo;
  }

  @Override
  public void service(ServletRequest req, ServletResponse res)
      throws ServletException, IOException
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
      super.service(request, response);
    } else {
      final URL redirectURL = redirectInfo.getRedirectURL(request.getQueryString(), request.getRequestURI());
      if (redirectURL == null) {
        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      }
      else {
        log.info("Forwarding request to [%s]", redirectURL);

        response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
        response.setHeader("Location", redirectURL.toString());
      }
    }
  }
}
