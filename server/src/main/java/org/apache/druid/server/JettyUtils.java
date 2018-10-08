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

package org.apache.druid.server;

import javax.annotation.Nullable;

public class JettyUtils
{
  /**
   * Concatenate URI parts, in a way that is useful for proxy servlets.
   *
   * @param base               base part of the uri, like http://example.com (no trailing slash)
   * @param encodedPath        encoded path, like you would get from HttpServletRequest's getRequestURI
   * @param encodedQueryString encoded query string, like you would get from HttpServletRequest's getQueryString
   */
  public static String concatenateForRewrite(
      final String base,
      final String encodedPath,
      @Nullable final String encodedQueryString
  )
  {
    // Query string and path are already encoded, no need for anything fancy beyond string concatenation.

    final StringBuilder url = new StringBuilder(base).append(encodedPath);

    if (encodedQueryString != null) {
      url.append("?").append(encodedQueryString);
    }

    return url.toString();
  }
}
