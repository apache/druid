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

package org.apache.druid.error;

import com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import java.util.HashMap;
import java.util.Map;

public class StandardRestExceptionEncoder implements RestExceptionEncoder
{
  private static final RestExceptionEncoder INSTANCE = new StandardRestExceptionEncoder();

  public static RestExceptionEncoder instance()
  {
    return INSTANCE;
  }

  @Override
  public ResponseBuilder builder(DruidException e)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("type", errorCode(e));
    builder.put("message", e.message());
    builder.put("errorMessage", e.getMessage());
    if (e.context() != null && !e.context().isEmpty()) {
      Map<String, String> context = new HashMap<>(e.context());
      String errorCode = context.remove(DruidException.ERROR_CODE);
      if (errorCode != null) {
        builder.put("errorCode", errorCode);
      }
      String host = context.remove(DruidException.HOST);
      if (host != null) {
        builder.put("host", host);
      }
      if (!context.isEmpty()) {
        builder.put("context", context);
      }
    }
    return Response
      .status(status(e))
      .entity(builder.build());
  }

  @Override
  public Response encode(DruidException e)
  {
    return builder(e).build();
  }

  private Object errorCode(DruidException e)
  {
    return e.type().name();
  }

  // Temporary status mapping
  private Status status(DruidException e)
  {
    switch (e.type()) {
      case CONFIG:
      case SYSTEM:
      case NETWORK:
        return Response.Status.INTERNAL_SERVER_ERROR;
      case TIMEOUT:
        return Response.Status.fromStatusCode(504); // No predefined status name
      case NOT_FOUND:
        return Response.Status.NOT_FOUND;
      case RESOURCE:
        return Response.Status.fromStatusCode(429); // No predefined status name
      case USER:
        return Response.Status.BAD_REQUEST;
      default:
        // Should never occur
        return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }
}
