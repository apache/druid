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

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public enum HttpResponses
{
  BAD_REQUEST(Response.Status.BAD_REQUEST),
  FORBIDDEN(Response.Status.FORBIDDEN),
  NOT_FOUND(Response.Status.NOT_FOUND),
  SERVER_ERROR(Response.Status.INTERNAL_SERVER_ERROR),
  SERVICE_UNAVAILABLE(Response.Status.SERVICE_UNAVAILABLE);

  private final int statusCode;

  HttpResponses(Response.Status status)
  {
    this.statusCode = status.getStatusCode();
  }

  public Response error(String message)
  {
    return object(ImmutableMap.of("error", message));
  }

  public Response error(@Nullable Throwable t)
  {
    return error(ServletResourceUtils.sanitizeExceptionMessage(t));
  }

  public Response error(String messageFormat, Object... formatArgs)
  {
    return error(StringUtils.format(messageFormat, formatArgs));
  }

  @SuppressForbidden(reason = "Response#status")
  public Response object(Object entity)
  {
    return Response.status(statusCode)
                   .type(MediaType.APPLICATION_JSON)
                   .entity(entity)
                   .build();
  }
}
