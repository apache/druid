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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Wraps the {@link AsyncContext} of a {@link HttpServletRequest} and ensures that the
 * response is completed at most once, guarding against writing to a timed-out or already-completed request.
 * Use {@link #completed(HttpServletRequest, ObjectMapper, AsyncResponse)} to complete a request immediately, or
 * {@link #create(HttpServletRequest, ListenableFuture, long)} to obtain a {@link Builder} that responds based on
 * the outcome of the future; call {@link Builder#contentType} to specify how to write the response entity.
 */
public class AsyncRequest
{
  private static final Logger log = new Logger(AsyncRequest.class);

  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final AsyncContext context;
  private final ObjectMapper objectMapper;
  private final String contentType;

  /**
   * Starts async processing of the request and immediately completes it with the given response, writing any
   * response entity as JSON using the given {@link ObjectMapper}.
   */
  public static <T> AsyncRequest completed(HttpServletRequest request, ObjectMapper jsonMapper, AsyncResponse<T> response)
  {
    return completed(request, jsonMapper, MediaType.APPLICATION_JSON, response);
  }

  /**
   * Starts async processing of the request and immediately completes it with the given response, writing any
   * response entity using the given {@link ObjectMapper} and content type.
   */
  public static <T> AsyncRequest completed(HttpServletRequest request, ObjectMapper objectMapper, String contentType, AsyncResponse<T> response)
  {
    final AsyncRequest asyncRequest = new AsyncRequest(request, objectMapper, contentType);
    asyncRequest.complete(response);
    return asyncRequest;
  }

  /**
   * Creates a {@link Builder} that waits on the given future. Call {@link Builder#contentType} to specify the
   * {@link ObjectMapper} and content type to use for writing the response entity.
   */
  public static <T> Builder<T> create(HttpServletRequest request, ListenableFuture<T> future, long timeoutMillis)
  {
    Preconditions.checkArgument(timeoutMillis > 0, "timeoutMillis[%s] must be greater than zero", timeoutMillis);
    return new Builder<>(request, future, timeoutMillis);
  }

  private AsyncRequest(HttpServletRequest request, ObjectMapper objectMapper, String contentType)
  {
    this.context = request.startAsync();
    this.objectMapper = objectMapper;
    this.contentType = contentType;
  }

  /**
   * Completes the request as described by the given {@link AsyncResponse}, if it hasn't already been completed.
   */
  private <T> void complete(AsyncResponse<T> response)
  {
    if (!completed.compareAndSet(false, true)) {
      // Request has already been completed
      return;
    }

    try {
      final HttpServletResponse servletResponse = (HttpServletResponse) context.getResponse();
      if (response.isError) {
        if (response.errorMessage == null) {
          servletResponse.sendError(response.statusCode);
        } else {
          servletResponse.sendError(response.statusCode, response.errorMessage);
        }
      } else {
        servletResponse.setStatus(response.statusCode);
        if (response.result != null) {
          servletResponse.setContentType(contentType);
          final ObjectWriter writer = response.resultType == null
                                       ? objectMapper.writer()
                                       : objectMapper.writerFor(objectMapper.getTypeFactory().constructType(response.resultType));
          writer.writeValue(servletResponse.getOutputStream(), response.result);
        }
      }
      context.complete();
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Request timed out or is closed already");
    }
  }

  /**
   * Describes how an {@link AsyncRequest} should be completed: a status code, with either a result of type
   * {@code T} (serialized using the {@link AsyncRequest}'s {@link ObjectMapper}) or an error message (sent using
   * {@link HttpServletResponse#sendError}).
   */
  public static final class AsyncResponse<T>
  {
    private final int statusCode;
    private final boolean isError;
    @Nullable
    private final T result;
    @Nullable
    private final Type resultType;
    @Nullable
    private final String errorMessage;

    private AsyncResponse(int statusCode, boolean isError, @Nullable T result, @Nullable Type resultType, @Nullable String errorMessage)
    {
      this.statusCode = statusCode;
      this.isError = isError;
      this.result = result;
      this.resultType = resultType;
      this.errorMessage = errorMessage;
    }

    /**
     * Responds with the given status code and no body.
     */
    public static AsyncResponse<Void> status(int statusCode)
    {
      return new AsyncResponse<>(statusCode, false, null, null, null);
    }

    /**
     * Responds with the given status code, writing the given result, if non-null.
     */
    public static <T> AsyncResponse<T> of(int statusCode, @Nullable T result)
    {
      return new AsyncResponse<>(statusCode, false, result, null, null);
    }

    /**
     * Responds with the given status code, writing the given result using the given {@link TypeReference} so
     * that generic type information erased at runtime is preserved.
     */
    public static <T> AsyncResponse<T> of(int statusCode, @Nullable T result, TypeReference<T> resultType)
    {
      return new AsyncResponse<>(statusCode, false, result, resultType.getType(), null);
    }

    /**
     * Responds with the given status code, writing the given result using the given {@link JavaType} so that
     * generic type information erased at runtime is preserved.
     */
    public static <T> AsyncResponse<T> of(int statusCode, @Nullable T result, @Nullable JavaType resultType)
    {
      return new AsyncResponse<>(statusCode, false, result, resultType, null);
    }

    /**
     * Responds with the given error status code and message, using {@link HttpServletResponse#sendError}.
     */
    public static AsyncResponse<Void> error(int statusCode, @Nullable String message)
    {
      return new AsyncResponse<>(statusCode, true, null, null, message);
    }
  }

  /**
   * Builds and starts an {@link AsyncRequest} that responds based on the outcome of a {@link ListenableFuture}
   * of type {@code T}, obtained from {@link #create(HttpServletRequest, ListenableFuture, long)}.
   */
  public static final class Builder<T>
  {
    private final HttpServletRequest request;
    private final ListenableFuture<T> future;
    private final long timeoutMillis;

    private ObjectMapper objectMapper;
    private String contentType;
    private int onTimeoutStatusCode = HttpServletResponse.SC_NO_CONTENT;
    private Function<T, AsyncResponse<?>> onSuccess;
    private Function<Throwable, AsyncResponse<?>> onFailure;

    private Builder(HttpServletRequest request, ListenableFuture<T> future, long timeoutMillis)
    {
      this.request = request;
      this.future = future;
      this.timeoutMillis = timeoutMillis;
    }

    /**
     * The {@link ObjectMapper} and content type to use for writing the response entity. Required.
     */
    public Builder<T> contentType(ObjectMapper objectMapper, String contentType)
    {
      this.objectMapper = objectMapper;
      this.contentType = contentType;
      return this;
    }

    /**
     * Status code to respond with, with no body, if the future times out before completing.
     */
    public Builder<T> onTimeout(int statusCode)
    {
      this.onTimeoutStatusCode = statusCode;
      return this;
    }

    /**
     * Function invoked with the result of the future on success, to determine how to respond. Required.
     */
    public Builder<T> onSuccess(Function<T, AsyncResponse<?>> onSuccess)
    {
      this.onSuccess = onSuccess;
      return this;
    }

    /**
     * Function invoked with the failure cause of the future, to determine how to respond. Required.
     */
    public Builder<T> onFailure(Function<Throwable, AsyncResponse<?>> onFailure)
    {
      this.onFailure = onFailure;
      return this;
    }

    /**
     * Starts async processing of the request using {@link HttpServletRequest#startAsync()}, wiring up the
     * configured future, timeout and success/failure handling.
     */
    public AsyncRequest start()
    {
      Preconditions.checkArgument(objectMapper != null, "contentType must be specified");
      Preconditions.checkArgument(contentType != null, "contentType must be specified");
      Preconditions.checkArgument(onSuccess != null, "onSuccess must be specified");
      Preconditions.checkArgument(onFailure != null, "onFailure must be specified");

      final AsyncRequest asyncRequest = new AsyncRequest(request, objectMapper, contentType);

      asyncRequest.context.setTimeout(timeoutMillis);
      asyncRequest.context.addListener(
          ServletResourceUtils.createAsyncTimeoutListener(event -> {
            asyncRequest.complete(AsyncResponse.status(onTimeoutStatusCode));
            future.cancel(true);
          })
      );

      Futures.addCallback(
          future,
          new FutureCallback<T>()
          {
            @Override
            public void onSuccess(T result)
            {
              asyncRequest.complete(onSuccess.apply(result));
            }

            @Override
            public void onFailure(Throwable t)
            {
              asyncRequest.complete(onFailure.apply(t));
            }
          },
          MoreExecutors.directExecutor()
      );

      return asyncRequest;
    }
  }
}
