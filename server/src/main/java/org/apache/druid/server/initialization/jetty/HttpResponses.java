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

package org.apache.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableMap;
import io.netty.util.SuppressForbidden;
import org.apache.druid.common.utils.ServletResourceUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public enum HttpResponses
{
  // 2xx
  OK(Response.Status.OK),
  ACCEPTED(Response.Status.ACCEPTED),

  // 4xx
  NO_CONTENT(Response.Status.NO_CONTENT),
  BAD_REQUEST(Response.Status.BAD_REQUEST),
  FORBIDDEN(Response.Status.FORBIDDEN),
  NOT_FOUND(Response.Status.NOT_FOUND),
  GONE(Response.Status.GONE),

  // 5xx
  SERVER_ERROR(Response.Status.INTERNAL_SERVER_ERROR),
  SERVICE_UNAVAILABLE(Response.Status.SERVICE_UNAVAILABLE);

  private final int statusCode;

  HttpResponses(Response.Status status)
  {
    this.statusCode = status.getStatusCode();
  }

  /**
   * This method is only for backward compatibility for some old interfaces.
   * SHOULD NOT be usded in new code
   *
   * @return an HTTP response whose Content-Type is text/plain
   */
  @SuppressForbidden(reason = "Response#status")
  public Response text(String message)
  {
    return Response.status(statusCode)
                   .entity(message)
                   .type(MediaType.TEXT_PLAIN_TYPE)
                   .build();
  }

  /**
   * @return an HTTP response with JSON formatted message. The message is as:
   * <code>
   * {"error": "error message"}
   * </code>
   */
  public Response error(String message)
  {
    return json(ImmutableMap.of("error", message));
  }

  /**
   * @return an HTTP response with JSON formatted message. The message is as:
   * <code>
   * {"error": "error message"}
   * </code>
   */
  public Response exception(@Nullable Throwable t)
  {
    return error(ServletResourceUtils.sanitizeExceptionMessage(t));
  }

  /**
   * @return an HTTP response with JSON formatted message. The message is as:
   * <code>
   * {"error": "error message"}
   * </code>
   */
  public Response error(String messageFormat, Object... formatArgs)
  {
    return error(StringUtils.format(messageFormat, formatArgs));
  }

  /**
   * @param entity object
   * @return an HTTP response with JSON formatted body
   */
  @SuppressForbidden(reason = "Response#status")
  public Response json(@Nonnull Object entity)
  {
    return Response.status(statusCode)
                   .type(MediaType.APPLICATION_JSON)
                   .entity(entity)
                   .build();
  }

  /**
   * @return an HTTP response without body
   */
  @SuppressForbidden(reason = "Response#status")
  public Response empty()
  {
    return Response.status(statusCode)
                   .build();
  }
}
