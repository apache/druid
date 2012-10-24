package com.metamx.druid.http;

import com.metamx.common.logger.Logger;
import org.mortbay.jetty.servlet.DefaultServlet;

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
      URL url = redirectInfo.getRedirectURL(request.getQueryString(), request.getRequestURI());
      log.info("Forwarding request to [%s]", url);
      response.sendRedirect(url.toString());
    }
  }
}
