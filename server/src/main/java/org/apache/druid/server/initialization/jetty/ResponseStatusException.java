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

  public static Response toResponse(Response.Status status, String message)
  {
    return HttpResponses.builder(status.getStatusCode(), MediaType.APPLICATION_JSON_TYPE)
                        .entity(ImmutableMap.of("error", message))
                        .build();
  }
}
