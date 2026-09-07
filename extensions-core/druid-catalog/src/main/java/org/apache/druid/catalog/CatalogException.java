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

package org.apache.druid.catalog;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import javax.ws.rs.core.Response;

public class CatalogException extends Exception
{
  public static final String DUPLICATE_ERROR = "Already exists";
  public static final String FAILED_ERROR = "Failed";
  public static final String INVALID_ERROR = "Invalid";
  public static final String NOT_FOUND_ERROR = "Not found";
  public static final String BAD_STATE = "Invalid table spec";

  public static final String ERROR_KEY = "error";
  public static final String ERR_MSG_KEY = "errorMessage";

  /**
   * Thrown when a record does not exist in the database. Allows
   * the caller to check for this specific case in a generic way.
   */
  public static class NotFoundException extends CatalogException
  {
    public NotFoundException(String msg, Object... args)
    {
      super(NOT_FOUND_ERROR, Response.Status.NOT_FOUND, msg, args);
    }
  }

  /**
   * Indicates an attempt to insert a duplicate key into a table.
   * This could indicate a logic error, or a race condition. It is
   * generally not retryable: it us unrealistic to expect the other
   * thread to helpfully delete the record it just added.
   */
  public static class DuplicateKeyException extends CatalogException
  {
    public DuplicateKeyException(String msg, Object... args)
    {
      super(DUPLICATE_ERROR, Response.Status.BAD_REQUEST, msg, args);
    }
  }

  private final String errorCode;
  private final Response.Status responseCode;

  public CatalogException(
      final String errorCode,
      final Response.Status responseCode,
      final String message,
      final Object... args
  )
  {
    super(StringUtils.format(message, args));
    this.errorCode = errorCode;
    this.responseCode = responseCode;
  }

  public static CatalogException badRequest(String msg, Object... args)
  {
    return new CatalogException(
        CatalogException.INVALID_ERROR,
        Response.Status.BAD_REQUEST,
        msg,
        args
    );
  }

  /**
   * Converts a table validation failure into a bad request: {@link IAE} (the traditional catalog validation
   * exception) and invalid-input {@link DruidException}s become bad requests; a {@link DruidException} of any other
   * category is a genuine server error, and is rethrown rather than being misreported as a problem with the request.
   */
  public static CatalogException validationError(RuntimeException e)
  {
    if (e instanceof DruidException && ((DruidException) e).getCategory() != DruidException.Category.INVALID_INPUT) {
      throw e;
    }
    return badRequest(e.getMessage());
  }

  public Response toResponse()
  {
    return Response
        .status(responseCode)
        .entity(ImmutableMap.of(ERROR_KEY, errorCode, ERR_MSG_KEY, getMessage()))
        .build();
  }
}
