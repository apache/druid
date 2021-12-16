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
import org.apache.druid.common.utils.ServletResourceUtils;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class ResponseStatusException extends RuntimeException
{
  private final Response.Status statusCode;

  public ResponseStatusException(Response.Status statusCode, String message)
  {
    super(message);
    this.statusCode = statusCode;
  }

  public Response.Status getStatusCode()
  {
    return statusCode;
  }

  public static Response toResponse(Response.Status statusCode, @Nullable Throwable t)
  {
    return toResponse(statusCode, ServletResourceUtils.sanitizeExceptionMessage(t));
  }

  /**
   * For Response.Status.BAD_REQUEST, it's suggested to use {@link BadRequestException#toResponse(String)} to simply the code.
   * For Response.Status.NOT_FOUND, it's suggested to use {@link NotFoundException#toResponse(String)} to simply the code.
   */
  public static Response toResponse(Response.Status status, String messageFormat, Object... formatArgs)
  {
    return toResponse(status, StringUtils.format(messageFormat, formatArgs));
  }

  public static Response toResponse(Response.Status status, String message)
  {
    return Response.status(status.getStatusCode())
                   .type(MediaType.APPLICATION_JSON)
                   .entity(ImmutableMap.of("error", message))
                   .build();
  }
}
