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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps the {@link AsyncContext} of a {@link HttpServletRequest} and ensures that the
 * response is completed at most once, guarding against writing to a timed-out or already-completed request.
 */
public class AsyncRequest
{
  private static final Logger log = new Logger(AsyncRequest.class);

  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final AsyncContext context;
  private final ObjectMapper objectMapper;
  private final String contentType;

  /**
   * Starts async processing on the given request using {@link HttpServletRequest#startAsync()},
   * writing any response entity as JSON using the given {@link ObjectMapper}.
   */
  public AsyncRequest(HttpServletRequest request, ObjectMapper jsonMapper)
  {
    this(request, jsonMapper, MediaType.APPLICATION_JSON);
  }

  /**
   * Starts async processing on the given request using {@link HttpServletRequest#startAsync()},
   * writing any response entity using the given {@link ObjectMapper} and content type.
   */
  public AsyncRequest(HttpServletRequest request, ObjectMapper objectMapper, String contentType)
  {
    this.context = request.startAsync();
    this.objectMapper = objectMapper;
    this.contentType = contentType;
  }

  /**
   * Sets the timeout, in milliseconds, for this async request.
   */
  public void setTimeout(long timeoutMillis)
  {
    context.setTimeout(timeoutMillis);
  }

  /**
   * Completes the request with the given status code and writes the given result, if non-null.
   */
  public void complete(int statusCode, @Nullable Object result)
  {
    complete(statusCode, result, (JavaType) null);
  }

  /**
   * Completes the request with the given status code and writes the given result, if non-null, using the
   * given {@link TypeReference} so that generic type information erased at runtime is preserved.
   */
  public void complete(int statusCode, @Nullable Object result, @Nullable TypeReference<?> resultType)
  {
    complete(statusCode, result, resultType == null ? null : objectMapper.getTypeFactory().constructType(resultType));
  }

  /**
   * Completes the request with the given status code and writes the given result, if non-null, using the
   * given {@link JavaType} so that generic type information erased at runtime is preserved.
   */
  public void complete(int statusCode, @Nullable Object result, @Nullable JavaType resultType)
  {
    if (!completed.compareAndSet(false, true)) {
      log.warn("Request has already been completed");
      return;
    }

    try {
      final HttpServletResponse response = (HttpServletResponse) context.getResponse();
      response.setStatus(statusCode);
      if (result != null) {
        response.setContentType(contentType);
        final ObjectWriter writer = resultType == null ? objectMapper.writer() : objectMapper.writerFor(resultType);
        writer.writeValue(response.getOutputStream(), result);
      }
      context.complete();
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Request timed out or is closed already");
    }
  }

  /**
   * Completes the request with the given error status code and message, using {@link HttpServletResponse#sendError}.
   */
  public void completeWithError(int statusCode, @Nullable String message)
  {
    if (!completed.compareAndSet(false, true)) {
      log.warn("Request has already been completed");
      return;
    }

    try {
      final HttpServletResponse response = (HttpServletResponse) context.getResponse();
      if (message == null) {
        response.sendError(statusCode);
      } else {
        response.sendError(statusCode, message);
      }
      context.complete();
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Request timed out or is closed already");
    }
  }

  /**
   * Completes the request with the given status code and no response body.
   */
  public void completeWithStatus(int statusCode)
  {
    if (!completed.compareAndSet(false, true)) {
      log.warn("Request has already been completed");
      return;
    }

    try {
      final HttpServletResponse response = (HttpServletResponse) context.getResponse();
      response.setStatus(statusCode);
      context.complete();
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Request timed out or is closed already");
    }
  }

  /**
   * Completes this request with the given status code and cancels the given future if it times out before
   * being completed otherwise. The request is completed before the future is cancelled, since cancellation
   * may synchronously invoke callbacks on the future that attempt to complete this same request.
   */
  public void completeWithStatusOnTimeout(Future<?> future, int statusCode)
  {
    context.addListener(
        ServletResourceUtils.createAsyncTimeoutListener(event -> {
          completeWithStatus(statusCode);
          future.cancel(true);
        })
    );
  }
}
