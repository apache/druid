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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * Helper for setting up each request's desired response, error handling, and retry behavior.
 */
public class ServiceCallImpl<HandlerIntermediateType, HandlerFinalType, T> implements ServiceCall<T>
{
  private static final Logger log = new Logger(ServiceCallImpl.class);

  private final RequestBuilder requestBuilder;
  private final HttpResponseHandler<HandlerIntermediateType, HandlerFinalType> responseHandler;
  private final Function<HandlerFinalType, T> responseTransformer;
  private final List<Function<Throwable, Either<Throwable, T>>> exceptionMappers;

  ServiceCallImpl(
      RequestBuilder requestBuilder,
      HttpResponseHandler<HandlerIntermediateType, HandlerFinalType> responseHandler,
      Function<HandlerFinalType, T> responseTransformer,
      List<Function<Throwable, Either<Throwable, T>>> exceptionMappers
  )
  {
    this.requestBuilder = requestBuilder;
    this.responseHandler = responseHandler;
    this.responseTransformer = responseTransformer;
    this.exceptionMappers = exceptionMappers;
  }

  @Override
  public ListenableFuture<T> go(final ServiceClient client)
  {
    final SettableFuture<T> retVal = SettableFuture.create();

    Futures.addCallback(
        FutureUtils.transform(
            client.asyncRequest(requestBuilder, responseHandler),
            responseTransformer
        ),
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(@Nullable T result)
          {
            retVal.set(result);
          }

          @Override
          public void onFailure(Throwable t)
          {
            Either<Throwable, T> either = Either.error(t);

            for (final Function<Throwable, Either<Throwable, T>> exceptionMapper : exceptionMappers) {
              if (!either.isError()) {
                break;
              }

              try {
                final Either<Throwable, T> nextEither = exceptionMapper.apply(either.error());
                if (nextEither != null) {
                  either = nextEither;
                }
              }
              catch (Throwable e) {
                // Not expected: on-error function should never throw exceptions. Continue mapping.
                log.warn(e, "Failed to map exception encountered while issuing request[%s]", requestBuilder);
              }
            }

            if (either.isError()) {
              retVal.setException(either.error());
            } else {
              retVal.set(either.valueOrThrow());
            }
          }
        }
    );

    return retVal;
  }
}
