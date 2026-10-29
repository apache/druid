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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Wraps the {@link AsyncContext} of a {@link HttpServletRequest} and ensures that the
 * response is completed at most once, guarding against writing to a timed-out or already-completed request.
 * Use {@link #create(HttpServletRequest, ObjectMapper)} to obtain a {@link Builder}.
 */
public class AsyncRequest
{
  private static final Logger log = new Logger(AsyncRequest.class);

  private final AtomicBoolean completed = new AtomicBoolean(false);
  private final AsyncContext context;
  private final ObjectMapper objectMapper;
  private final String contentType;

  /**
   * Creates a {@link Builder} that writes any response entity as JSON using the given {@link ObjectMapper}.
   */
  public static Builder create(HttpServletRequest request, ObjectMapper jsonMapper)
  {
    return new Builder(request, jsonMapper, MediaType.APPLICATION_JSON);
  }

  /**
   * Creates a {@link Builder} that writes any response entity using the given {@link ObjectMapper} and content type.
   */
  public static Builder create(HttpServletRequest request, ObjectMapper objectMapper, String contentType)
  {
    return new Builder(request, objectMapper, contentType);
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
  private void complete(AsyncResponse response)
  {
    if (!completed.compareAndSet(false, true)) {
      log.warn("Request has already been completed");
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
                                       : objectMapper.writerFor(response.resultType);
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
   * Describes how an {@link AsyncRequest} should be completed: a status code, with either a result (serialized
   * using the {@link AsyncRequest}'s {@link ObjectMapper}) or an error message (sent using
   * {@link HttpServletResponse#sendError}).
   */
  public static final class AsyncResponse
  {
    private final int statusCode;
    private final boolean isError;
    @Nullable
    private final Object result;
    @Nullable
    private final JavaType resultType;
    @Nullable
    private final String errorMessage;

    private AsyncResponse(int statusCode, boolean isError, @Nullable Object result, @Nullable JavaType resultType, @Nullable String errorMessage)
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
    public static AsyncResponse status(int statusCode)
    {
      return new AsyncResponse(statusCode, false, null, null, null);
    }

    /**
     * Responds with the given status code, writing the given result, if non-null.
     */
    public static AsyncResponse of(int statusCode, @Nullable Object result)
    {
      return new AsyncResponse(statusCode, false, result, null, null);
    }

    /**
     * Responds with the given status code, writing the given result using the given {@link TypeReference} so
     * that generic type information erased at runtime is preserved.
     */
    public static AsyncResponse of(int statusCode, @Nullable Object result, ObjectMapper objectMapper, TypeReference<?> resultType)
    {
      return of(statusCode, result, objectMapper.getTypeFactory().constructType(resultType));
    }

    /**
     * Responds with the given status code, writing the given result using the given {@link JavaType} so that
     * generic type information erased at runtime is preserved.
     */
    public static AsyncResponse of(int statusCode, @Nullable Object result, @Nullable JavaType resultType)
    {
      return new AsyncResponse(statusCode, false, result, resultType, null);
    }

    /**
     * Responds with the given error status code and message, using {@link HttpServletResponse#sendError}.
     */
    public static AsyncResponse error(int statusCode, @Nullable String message)
    {
      return new AsyncResponse(statusCode, true, null, null, message);
    }
  }

  /**
   * Builds and starts an {@link AsyncRequest}, fluently specifying how it should respond on success, failure
   * and timeout of an optional {@link ListenableFuture} to {@link #waitOn}.
   */
  public static final class Builder
  {
    private final HttpServletRequest request;
    private final ObjectMapper objectMapper;
    private final String contentType;

    @Nullable
    private ListenableFuture<?> future;
    private long timeoutMillis = -1;
    private int onTimeoutStatusCode = HttpServletResponse.SC_NO_CONTENT;
    @Nullable
    private Function<Object, AsyncResponse> onSuccess;
    @Nullable
    private Function<Throwable, AsyncResponse> onFailure;

    private Builder(HttpServletRequest request, ObjectMapper objectMapper, String contentType)
    {
      this.request = request;
      this.objectMapper = objectMapper;
      this.contentType = contentType;
    }

    /**
     * The future to wait on, cancelling it and completing the request with {@link #onTimeout}'s status code if
     * it does not finish within the given timeout, in milliseconds. If given, {@link #onSuccess} and
     * {@link #onFailure} must also be specified.
     */
    @SuppressWarnings("unchecked")
    public <T> Builder waitOn(ListenableFuture<T> future, long timeoutMillis)
    {
      this.future = (ListenableFuture<Object>) future;
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    /**
     * Status code to respond with, with no body, if {@link #future} times out before completing.
     */
    public Builder onTimeout(int statusCode)
    {
      this.onTimeoutStatusCode = statusCode;
      return this;
    }

    /**
     * Function invoked with the result of {@link #future} on success, to determine how to respond.
     */
    public <T> Builder onSuccess(Function<T, AsyncResponse> onSuccess)
    {
      this.onSuccess = (Function<Object, AsyncResponse>) onSuccess;
      return this;
    }

    /**
     * Function invoked with the failure cause of {@link #future}, to determine how to respond.
     */
    public Builder onFailure(Function<Throwable, AsyncResponse> onFailure)
    {
      this.onFailure = onFailure;
      return this;
    }

    /**
     * Starts async processing of the request using {@link HttpServletRequest#startAsync()}, wiring up the
     * configured future, timeout and success/failure handling, if any.
     */
    public AsyncRequest start()
    {
      final AsyncRequest asyncRequest = new AsyncRequest(request, objectMapper, contentType);

      if (future != null) {
        Preconditions.checkArgument(onSuccess != null, "onSuccess must be specified when a future is given");
        Preconditions.checkArgument(onFailure != null, "onFailure must be specified when a future is given");

        if (timeoutMillis >= 0) {
          asyncRequest.context.setTimeout(timeoutMillis);
          asyncRequest.context.addListener(
              ServletResourceUtils.createAsyncTimeoutListener(event -> {
                asyncRequest.complete(AsyncResponse.status(onTimeoutStatusCode));
                future.cancel(true);
              })
          );
        }

        Futures.addCallback(
            future,
            new FutureCallback<Object>()
            {
              @Override
              public void onSuccess(Object result)
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
      }

      return asyncRequest;
    }

    /**
     * Starts async processing of the request and immediately completes it with the given response, without
     * waiting on a future. Mutually exclusive with {@link #future}.
     */
    public AsyncRequest completeNow(AsyncResponse response)
    {
      Preconditions.checkArgument(future == null, "completeNow() cannot be used together with future()");
      final AsyncRequest asyncRequest = new AsyncRequest(request, objectMapper, contentType);
      asyncRequest.complete(response);
      return asyncRequest;
    }
  }
}
