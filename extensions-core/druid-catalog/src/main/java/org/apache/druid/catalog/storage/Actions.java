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

package org.apache.druid.catalog.storage;

import com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.Response;

import java.util.Map;

/**
 * Helper functions for the catalog REST API actions.
 */
public class Actions
{
  public static final String DUPLICATE_ERROR = "Already exists";
  public static final String FAILED_ERROR = "Failed";
  public static final String INVALID = "Invalid";
  public static final String FORBIDDEN = "Forbidden";
  public static final String NOT_FOUND = "Not found";

  public static final String ERROR_KEY = "error";
  public static final String ERR_MSG_KEY = "errorMessage";

  public static Map<String, String> error(String code, String msg)
  {
    return ImmutableMap.of(ERROR_KEY, code, ERR_MSG_KEY, msg);
  }

  public static Response exception(Exception e)
  {
    return Response
        .serverError()
        .entity(error(FAILED_ERROR, e.getMessage()))
        .build();
  }

  public static Response badRequest(String code, String msg)
  {
    return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(error(code, msg))
        .build();
  }

  public static Response notFound(String msg)
  {
    return Response
        .status(Response.Status.NOT_FOUND)
        .entity(error(NOT_FOUND, msg))
        .build();
  }

  public static Response ok()
  {
    return Response.ok().build();
  }

  public static Response okWithVersion(long version)
  {
    return Response
        .ok()
        .entity(ImmutableMap.of("version", version))
        .build();

  }
}
