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

import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

public class JettyUtils
{
  private static final Logger log = new Logger(JettyUtils.class);

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

  /**
   * Returns the value of the query parameter of the given name. If not found, but there is a value corresponding to
   * the parameter of the given compatiblityName it is returned instead and a warning is logged suggestion to make
   * queries using the new parameter name.
   *
   * This method is useful for renaming query parameters (from name to compatiblityName) while preserving backward
   * compatibility of the REST API.
   */
  @Nullable
  public static String getQueryParam(UriInfo uriInfo, String name, String compatiblityName)
  {
    MultivaluedMap<String, String> queryParameters = uriInfo.getQueryParameters();
    // Returning the first value, according to the @QueryParam spec:
    // https://docs.oracle.com/javaee/7/api/javax/ws/rs/QueryParam.html:
    // "If the type is not one of the collection types listed in 5 above and the query parameter is represented by
    //  multiple values then the first value (lexically) of the parameter is used."
    String paramValue = queryParameters.getFirst(name);
    if (paramValue != null) {
      return paramValue;
    }
    String compatibilityParamValue = queryParameters.getFirst(compatiblityName);
    if (compatibilityParamValue != null) {
      log.warn(
          "Parameter %s in %s query has been renamed to %s. Use the new parameter name.",
          compatiblityName,
          uriInfo.getPath(),
          name
      );
      return compatibilityParamValue;
    }
    // Not found neither name nor compatiblityName
    return null;
  }
}
