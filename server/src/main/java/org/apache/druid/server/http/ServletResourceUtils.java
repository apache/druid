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

package org.apache.druid.server.http;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.HttpResponseException;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.function.Supplier;

public class ServletResourceUtils
{
  private static final Logger log = new Logger(ServletResourceUtils.class);

  /**
   * Sanitize the exception as a map of "error" to information about the exception.
   *
   * This method explicitly suppresses the stack trace and any other logging. Any logging should be handled by the caller.
   * @param t The exception to sanitize
   * @return An immutable Map with a single entry which maps "error" to information about the error suitable for passing as an entity in a servlet error response.
   */
  public static Map<String, String> sanitizeException(@Nullable Throwable t)
  {
    return ImmutableMap.of(
        "error",
        t == null ? "null" : (t.getMessage() == null ? t.toString() : t.getMessage())
    );
  }

  /**
   * Converts String errorMsg into a Map so that it produces valid json on serialization into response.
   */
  public static Map<String, String> jsonize(String msgFormat, Object... args)
  {
    return ImmutableMap.of("error", StringUtils.nonStrictFormat(msgFormat, args));
  }

  public static Response buildErrorResponseFrom(DruidException e)
  {
    return Response.status(e.getStatusCode())
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(new ErrorResponse(e))
                   .build();
  }

  public static Response buildUpdateResponse(Supplier<Boolean> updateOperation)
  {
    return buildReadResponse(() -> Map.of("success", updateOperation.get()));
  }

  public static <T> Response buildReadResponse(Supplier<T> readOperation)
  {
    try {
      return Response.ok(readOperation.get()).build();
    }
    catch (DruidException e) {
      log.error(e, "Error executing HTTP request");
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      log.error(e, "Error executing HTTP request");
      return ServletResourceUtils.buildErrorResponseFrom(
          InternalServerError.exception(Throwables.getRootCause(e), "Unknown error occurred")
      );
    }
  }

  /**
   * Returns the given default value if the root cause of the exception is an
   * {@link HttpResponseException} with status code {@link HttpResponseStatus#NOT_FOUND}.
   * Otherwise, re-throws the given exception wrapped in a {@link RuntimeException}.
   */
  public static <T> T getDefaultValueIfCauseIs404ElseThrow(
      Exception e,
      Supplier<T> defaultValueSupplier
  )
  {
    Throwable rootCause = Throwables.getRootCause(e);
    if (rootCause instanceof HttpResponseException) {
      final HttpResponseException httpException = (HttpResponseException) rootCause;
      if (httpException.getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
        return defaultValueSupplier.get();
      }
    }

    throw new RuntimeException(e);
  }
}
