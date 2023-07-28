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

package org.apache.druid.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, T>
{
  private final RequestBuilder requestBuilder;
  private final List<Function<Throwable, Either<Throwable, T>>> exceptionMappers = new ArrayList<>();

  private HttpResponseHandler<HandlerIntermediateType, HandlerFinalType> responseHandler;
  private Function<HandlerFinalType, T> responseTransformer;

  protected ServiceCallBuilder(
      RequestBuilder requestBuilder,
      HttpResponseHandler<HandlerIntermediateType, HandlerFinalType> responseHandler,
      Function<HandlerFinalType, T> responseTransformer
  )
  {
    this.requestBuilder = requestBuilder;
    this.responseHandler = responseHandler;
    this.responseTransformer = responseTransformer;
  }

  /**
   * Create a new call builder.
   *
   * The initial handler is {@link IgnoreHttpResponseHandler} and the initial response transformer is
   * {@link Function#identity()}. Use methods on the resulting builder to customize behavior, then call
   * {@link #build()} to get a {@link ServiceCall}.
   */
  public static ServiceCallBuilder<Void, Void, Void> forRequest(final RequestBuilder requestBuilder)
  {
    return new ServiceCallBuilder<>(requestBuilder, IgnoreHttpResponseHandler.INSTANCE, Function.identity());
  }

  /**
   * Handler for requests. The result from this handler is fed into the transformer provided by {@link #onSuccess}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <NewIntermediateType, NewFinalType> ServiceCallBuilder<NewIntermediateType, NewFinalType, T> handler(
      final HttpResponseHandler<NewIntermediateType, NewFinalType> handler
  )
  {
    this.responseHandler = (HttpResponseHandler) handler;
    return (ServiceCallBuilder) this;
  }

  /**
   * Response mapping for successful requests.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <NewT> ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, NewT> onSuccess(
      final Function<HandlerFinalType, NewT> responseTransformer
  )
  {
    this.responseTransformer = (Function) responseTransformer;
    return (ServiceCallBuilder) this;
  }

  /**
   * Error mapping for all exceptions.
   */
  public ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, T> onException(final Function<Throwable, Either<Throwable, T>> fn)
  {
    exceptionMappers.add(fn);
    return this;
  }

  /**
   * Error mapping for {@link HttpResponseException}, which occurs when a task returns a non-2xx HTTP code.
   */
  public ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, T> onHttpError(final Function<HttpResponseException, Either<Throwable, T>> fn)
  {
    return onException(e -> {
      if (e instanceof HttpResponseException) {
        return fn.apply((HttpResponseException) e);
      } else {
        return Either.error(e);
      }
    });
  }

  /**
   * Error mapping for {@link ServiceNotAvailableException}, which occurs when a task is not available.
   */
  public ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, T> onNotAvailable(final Function<ServiceNotAvailableException, Either<Throwable, T>> fn)
  {
    return onException(e -> {
      if (e instanceof ServiceNotAvailableException) {
        return fn.apply((ServiceNotAvailableException) e);
      } else {
        return Either.error(e);
      }
    });
  }

  /**
   * Error mapping for {@link ServiceClosedException}, which occurs when a task is not running.
   */
  public ServiceCallBuilder<HandlerIntermediateType, HandlerFinalType, T> onClosed(final Function<ServiceClosedException, Either<Throwable, T>> fn)
  {
    return onException(e -> {
      if (e instanceof ServiceClosedException) {
        return fn.apply((ServiceClosedException) e);
      } else {
        return Either.error(e);
      }
    });
  }

  /**
   * Create a {@link ServiceClient} for this builder.
   */
  public ServiceCall<T> build()
  {
    return new ServiceCallImpl<>(
        requestBuilder,
        responseHandler,
        responseTransformer,
        exceptionMappers
    );
  }

  /**
   * Shortcut for {@link #build()} followed by {@link ServiceCall#go(ServiceClient)}.
   */
  public ListenableFuture<T> go(final ServiceClient client)
  {
    return build().go(client);
  }
}
