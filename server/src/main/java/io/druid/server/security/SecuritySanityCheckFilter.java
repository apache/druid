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
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import io.druid.server.DruidNode;
import org.eclipse.jetty.server.Response;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class SecuritySanityCheckFilter implements Filter
{
  private static final Logger log = new Logger(SecuritySanityCheckFilter.class);

  private final String unauthorizedMessage;

  public SecuritySanityCheckFilter(
      ObjectMapper jsonMapper
  )
  {
    try {
      QueryInterruptedException unauthorizedError = new QueryInterruptedException(
          QueryInterruptedException.UNAUTHORIZED,
          null,
          null,
          DruidNode.getDefaultHost()
      );
      unauthorizedError.setStackTrace(new StackTraceElement[0]);
      this.unauthorizedMessage = jsonMapper.writeValueAsString(unauthorizedError);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
  }

  @Override
  public void doFilter(
      ServletRequest request, ServletResponse response, FilterChain chain
  ) throws IOException, ServletException
  {
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    OutputStream out = httpResponse.getOutputStream();

    // make sure the original request isn't trying to fake the auth token checks
    Boolean authInfoChecked = (Boolean) request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED);
    Boolean allowUnsecured = (Boolean) request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH);

    AuthenticationResult result = (AuthenticationResult) request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT);
    if (authInfoChecked != null || result != null || allowUnsecured != null) {
      sendJsonError(httpResponse, Response.SC_FORBIDDEN, unauthorizedMessage, out);
      out.close();
      return;
    }

    chain.doFilter(request, response);
  }

  @Override
  public void destroy()
  {

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
