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

package io.druid.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.ISE;
import io.druid.query.QueryInterruptedException;
import io.druid.server.DruidNode;
import org.eclipse.jetty.server.Response;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Filter that verifies that authorization checks were applied to an HTTP request, before sending a response.
 * <p>
 * This filter is intended to help catch missing authorization checks arising from bugs/design omissions.
 */
public class PreResponseAuthorizationCheckFilter implements Filter
{
  private static final EmittingLogger log = new EmittingLogger(PreResponseAuthorizationCheckFilter.class);

  private final List<Authenticator> authenticators;
  private final ObjectMapper jsonMapper;

  public PreResponseAuthorizationCheckFilter(
      List<Authenticator> authenticators,
      ObjectMapper jsonMapper
  )
  {
    this.authenticators = authenticators;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {

  }

  @Override
  public void doFilter(
      ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain
  ) throws IOException, ServletException
  {
    final HttpServletResponse response = (HttpServletResponse) servletResponse;
    final HttpServletRequest request = (HttpServletRequest) servletRequest;

    if (servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT) == null) {
      handleUnauthenticatedRequest(response);
      return;
    }

    filterChain.doFilter(servletRequest, servletResponse);

    Boolean authInfoChecked = (Boolean) servletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED);
    if (authInfoChecked == null && statusIsSuccess(response.getStatus())) {
      // Note: rather than throwing an exception here, it would be nice to blank out the original response
      // since the request didn't have any authorization checks performed. However, this breaks proxying
      // (e.g. OverlordServletProxy), so this is not implemented for now.
      handleAuthorizationCheckError(
          "Request did not have an authorization check performed.",
          request,
          response
      );
    }

    if (authInfoChecked != null && !authInfoChecked && response.getStatus() != Response.SC_FORBIDDEN) {
      handleAuthorizationCheckError(
          "Request's authorization check failed but status code was not 403.",
          request,
          response
      );
    }
  }

  @Override
  public void destroy()
  {

  }

  private void handleUnauthenticatedRequest(
      final HttpServletResponse response
  ) throws IOException
  {
    // Since this is the last filter in the chain, some previous authentication filter
    // should have placed an authentication result in the request.
    // If not, send an authentication challenge.
    Set<String> supportedAuthSchemes = Sets.newHashSet();
    for (Authenticator authenticator : authenticators) {
      String challengeHeader = authenticator.getAuthChallengeHeader();
      if (challengeHeader != null) {
        supportedAuthSchemes.add(challengeHeader);
      }
    }
    for (String authScheme : supportedAuthSchemes) {
      response.addHeader("WWW-Authenticate", authScheme);
    }
    QueryInterruptedException unauthorizedError = new QueryInterruptedException(
        QueryInterruptedException.UNAUTHORIZED,
        null,
        null,
        DruidNode.getDefaultHost()
    );
    unauthorizedError.setStackTrace(new StackTraceElement[0]);
    OutputStream out = response.getOutputStream();
    sendJsonError(response, Response.SC_UNAUTHORIZED, jsonMapper.writeValueAsString(unauthorizedError), out);
    out.close();
    return;
  }

  private void handleAuthorizationCheckError(
      String errorMsg,
      HttpServletRequest servletRequest,
      HttpServletResponse servletResponse
  )
  {
    // Send out an alert so there's a centralized collection point for seeing errors of this nature
    log.makeAlert(errorMsg)
       .addData("uri", servletRequest.getRequestURI())
       .addData("method", servletRequest.getMethod())
       .addData("remoteAddr", servletRequest.getRemoteAddr())
       .addData("remoteHost", servletRequest.getRemoteHost())
       .emit();

    if (servletResponse.isCommitted()) {
      throw new ISE(errorMsg);
    } else {
      try {
        servletResponse.sendError(Response.SC_FORBIDDEN);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static boolean statusIsSuccess(int status)
  {
    return 200 <= status && status < 300;
  }

  public static void sendJsonError(HttpServletResponse resp, int error, String errorJson, OutputStream outputStream)
  {
    resp.setStatus(error);
    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");
    try {
      outputStream.write(errorJson.getBytes(StandardCharsets.UTF_8));
    }
    catch (IOException ioe) {
      log.error("WTF? Can't get writer from HTTP response.");
    }
  }
}
