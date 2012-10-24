package com.metamx.druid.http;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.HttpResponseHandler;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.URL;

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
