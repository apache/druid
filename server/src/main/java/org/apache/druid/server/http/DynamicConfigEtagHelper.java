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
import org.apache.druid.common.config.ConfigEtag;
import org.apache.druid.common.config.ConfigManager.SetResult;

import java.util.Map;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * HTTP-layer helpers for {@code ETag} / {@code If-Match} on dynamic-config endpoints.
 */
public final class DynamicConfigEtagHelper
{
  private DynamicConfigEtagHelper()
  {
  }

  /**
   * Read the {@code If-Match} header. Returns {@code null} when absent. Throws
   * 400 if present but blank (RFC 7232 §3.1 requires a non-empty value).
   */
  @Nullable
  public static String getIfMatch(HttpServletRequest req)
  {
    if (req == null) {
      return null;
    }
    final String header = req.getHeader(HttpHeaders.IF_MATCH);
    if (header == null) {
      return null;
    }
    if (header.trim().isEmpty()) {
      throw InvalidInput.exception("If-Match header must not be blank");
    }
    return header;
  }

  /** Attach an {@code ETag} header derived from {@code bytes}; no-op if {@code bytes} is null. */
  public static Response.ResponseBuilder withEtag(Response.ResponseBuilder builder, @Nullable byte[] bytes)
  {
    final String etag = ConfigEtag.compute(bytes);
    if (etag != null) {
      builder.header(HttpHeaders.ETAG, etag);
    }
    return builder;
  }

  /**
   * Build a read response whose entity and ETag are both derived from one read
   * of the stored config bytes.
   */
  public static <T> Response buildReadResponseWithEtag(
      Supplier<byte[]> bytesSupplier,
      Function<byte[], T> entitySupplier
  )
  {
    try {
      final byte[] currentBytes = bytesSupplier.get();
      return withEtag(Response.ok(entitySupplier.apply(currentBytes)), currentBytes).build();
    }
    catch (DruidException e) {
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      return ServletResourceUtils.buildErrorResponseFrom(
          InternalServerError.exception(Throwables.getRootCause(e), "Unknown error occurred")
      );
    }
  }

  /**
   * Map a failed {@link SetResult} to 412 (precondition failed) or 400.
   * Callers handle the success case themselves.
   */
  public static Response toErrorResponse(SetResult result)
  {
    final Response.Status status = result.isPreconditionFailed()
        ? Response.Status.PRECONDITION_FAILED
        : Response.Status.BAD_REQUEST;
    return Response.status(status)
                   .entity(ServletResourceUtils.sanitizeException(result.getException()))
                   .build();
  }

  /**
   * Build a write response from a {@link SetResult}, using the same success and
   * error shapes as other dynamic-config endpoints.
   */
  public static Response buildSetResultUpdateResponse(SetResult result)
  {
    if (result.isOk()) {
      return Response.ok(Map.of("success", true)).build();
    }
    return toErrorResponse(result);
  }
}
