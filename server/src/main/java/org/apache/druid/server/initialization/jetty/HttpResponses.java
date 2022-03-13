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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public enum HttpResponses
{
  // 2xx
  OK(Response.Status.OK),
  ACCEPTED(Response.Status.ACCEPTED),
  NO_CONTENT(Response.Status.NO_CONTENT),

  // 4xx
  BAD_REQUEST(Response.Status.BAD_REQUEST),
  FORBIDDEN(Response.Status.FORBIDDEN),
  NOT_FOUND(Response.Status.NOT_FOUND),
  GONE(Response.Status.GONE),

  // 5xx
  SERVER_ERROR(Response.Status.INTERNAL_SERVER_ERROR),
  NOT_IMPLEMENTED(501),
  SERVICE_UNAVAILABLE(Response.Status.SERVICE_UNAVAILABLE);

  private final int statusCode;

  HttpResponses(Response.Status status)
  {
    this.statusCode = status.getStatusCode();
  }

  HttpResponses(int statusCode)
  {
    this.statusCode = statusCode;
  }

  @SuppressForbidden(reason = "Response#status")
  public static Response.ResponseBuilder builder(int statusCode, MediaType mediaType)
  {
    return Response.status(statusCode)
                   .type(mediaType);
  }

  /**
   * @param mediaType Content-Type in the HTTP response.
   *                  NOTE: It's dangerous to set the type to text/html which might cause an XSS problem.
   * @param text      text
   * @return an HTTP response
   */
  @SuppressForbidden(reason = "Response#status")
  public Response text(MediaType mediaType, String text)
  {
    return Response.status(statusCode)
                   .entity(text)
                   .type(mediaType)
                   .build();
  }

  public Response text(MediaType mediaType, String format, Object...args)
  {
    return text(mediaType, StringUtils.format(format, args));
  }

  /**
   * Returns an {@link WebApplicationException} exception which will be handled by the servlet framework automatically.
   * <p>
   * This method is usually called in a {@link com.sun.jersey.spi.container.ContainerRequestFilter}.
   * Since the servlet handles the exceptions thrown from the filter differently from the {@link Response} returned by an endpoint,
   * the ContentType is explictly set to application/json.
   * <p>
   * If this method is called within an endpoint instead of filter,
   * and the endpoint declares Content-Type by {@link javax.ws.rs.Produces}, the declaration should contain application/json.
   */
  public RuntimeException exception(String messageFormat, Object... args)
  {
    Response response = error(messageFormat, args);
    response.getMetadata().putSingle("Content-Type", MediaType.APPLICATION_JSON);
    return new WebApplicationException(response);
  }

  /**
   * @return an HTTP response with JSON formatted message. The message is as:
   * <code>
   * {"error": "error message"}
   * </code>
   */
  public Response error(String message)
  {
    return entity(ImmutableMap.of("error", message));
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
   * @return an HTTP response with JSON formatted message. The message is as:
   * <code>
   * {"error": "error message"}
   * </code>
   */
  public Response error(@Nullable Throwable t)
  {
    return error(ServletResourceUtils.sanitizeExceptionMessage(t));
  }

  /**
   * NOTE: the Content-Type is not set on the returned Response object.
   * For any http endpoint, it MUST declare a {@link javax.ws.rs.Produces} annotation,
   * so that the servlet framework knows how to serialize this entity.
   * <p>
   * If the {@link javax.ws.rs.Produces} annotation on an endpoint misses,
   * the servlet will infer the returned Content-Type by the input 'Accept' header and the entity type,
   * which might cause an XSS problem.
   *
   * @param entity object
   * @return an HTTP response
   */
  @SuppressForbidden(reason = "Response#status")
  public Response entity(@Nonnull Object entity)
  {
    return Response.status(statusCode)
                   .entity(entity)
                   .build();
  }

  /**
   * An overrided version of {@link #entity(Object)}
   * <p>
   * It's a little dangerous to return String-based entity if it lacks of explicit Content-Type.
   * So it's designed to be prohibitted in the forbidden-apis.txt
   * By this way, the forbidden checks helps developers to know if a String is really needed to return.
   */
  public Response entity(String entity)
  {
    throw new ISE("Not Supported API. Call text() function instead.");
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
