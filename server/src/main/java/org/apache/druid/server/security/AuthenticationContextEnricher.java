/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.security;

import com.google.common.base.Strings;

import javax.annotation.Nullable;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AuthenticationContextEnricher
{
  public static final String CONTEXT_CLIENT_IPADDRESS = "clientIpAddress";
  public static final String CONTEXT_FORWARDED_ADDRESSES = "forwardedAddresses";
  public static final String CONTEXT_REMOTE_ADDRESS = "remoteAddress";

  private static final String X_FORWARDED_FOR = "X-Forwarded-For";
  private static final String X_REAL_IP = "X-Real-IP";

  @Nullable
  private static List<String> getForwardedAddresses(ServletRequest request)
  {
    if (request instanceof HttpServletRequest) {
      String forwarded_for = ((HttpServletRequest) request).getHeader(X_FORWARDED_FOR);
      if (!Strings.isNullOrEmpty(forwarded_for)) {
        return Arrays.asList(forwarded_for.split(","));
      }
    }
    return null;
  }

  /**
   * Enriches the context with information received from the servlet request like
   * remote address and proxies.
   * @param request The servlet request
   */
  public static Map<String, Object> enrichContext(ServletRequest request)
  {
    Map<String, Object> context = new HashMap<>();

    List<String> forwardedAddresses = getForwardedAddresses(request);
    if (forwardedAddresses != null) {
      context.put(CONTEXT_REMOTE_ADDRESS, forwardedAddresses.get(0));
    } else if (request instanceof HttpServletRequest) {
      String realIp = ((HttpServletRequest) request).getHeader(X_REAL_IP);
      if (!Strings.isNullOrEmpty(realIp)) {
        context.put(CONTEXT_REMOTE_ADDRESS, realIp);
      }
    }
    context.put(CONTEXT_FORWARDED_ADDRESSES, forwardedAddresses);
    context.put(CONTEXT_CLIENT_IPADDRESS, request.getRemoteAddr());

    return context;
  }
}
