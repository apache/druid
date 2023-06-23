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

package org.apache.druid.java.util.http.client.response;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import org.apache.druid.java.util.common.Either;

import java.nio.charset.StandardCharsets;

/**
 * Response handler that delegates successful responses (2xx response codes) to some other handler, but returns
 * errors (non-2xx response codes) as Strings. The return value is an {@link Either}.
 */
public class ObjectOrErrorResponseHandler<IntermediateType, FinalType>
    implements HttpResponseHandler<Either<BytesFullResponseHolder, IntermediateType>, Either<StringFullResponseHolder, FinalType>>
{
  private final HttpResponseHandler<IntermediateType, FinalType> okHandler;
  private final StringFullResponseHandler errorHandler;

  public ObjectOrErrorResponseHandler(HttpResponseHandler<IntermediateType, FinalType> okHandler)
  {
    this.okHandler = okHandler;
    this.errorHandler = new StringFullResponseHandler(StandardCharsets.UTF_8);
  }

  @Override
  public ClientResponse<Either<BytesFullResponseHolder, IntermediateType>> handleResponse(
      final HttpResponse response,
      final TrafficCop trafficCop
  )
  {
    if (response.status().code() / 100 == 2) {
      final ClientResponse<IntermediateType> delegateResponse = okHandler.handleResponse(response, trafficCop);

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.value(delegateResponse.getObj())
      );
    } else {
      final ClientResponse<BytesFullResponseHolder> delegateResponse =
          errorHandler.handleResponse(response, trafficCop);

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.error(delegateResponse.getObj())
      );
    }
  }

  @Override
  public ClientResponse<Either<BytesFullResponseHolder, IntermediateType>> handleChunk(
      final ClientResponse<Either<BytesFullResponseHolder, IntermediateType>> clientResponse,
      final HttpContent chunk,
      final long chunkNum
  )
  {
    final Either<BytesFullResponseHolder, IntermediateType> prevHolder = clientResponse.getObj();

    if (prevHolder.isValue()) {
      final ClientResponse<IntermediateType> delegateResponse = okHandler.handleChunk(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.valueOrThrow()
          ),
          chunk,
          chunkNum
      );

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.value(delegateResponse.getObj())
      );
    } else {
      final ClientResponse<BytesFullResponseHolder> delegateResponse = errorHandler.handleChunk(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.error()
          ),
          chunk,
          chunkNum
      );

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.error(delegateResponse.getObj())
      );
    }
  }

  @Override
  public ClientResponse<Either<StringFullResponseHolder, FinalType>> done(
      final ClientResponse<Either<BytesFullResponseHolder, IntermediateType>> clientResponse
  )
  {
    final Either<BytesFullResponseHolder, IntermediateType> prevHolder = clientResponse.getObj();

    if (prevHolder.isValue()) {
      final ClientResponse<FinalType> delegateResponse = okHandler.done(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.valueOrThrow()
          )
      );

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.value(delegateResponse.getObj())
      );
    } else {
      final ClientResponse<StringFullResponseHolder> delegateResponse = errorHandler.done(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.error()
          )
      );

      return new ClientResponse<>(
          delegateResponse.isFinished(),
          delegateResponse.isContinueReading(),
          Either.error(delegateResponse.getObj())
      );
    }
  }

  @Override
  public void exceptionCaught(
      final ClientResponse<Either<BytesFullResponseHolder, IntermediateType>> clientResponse,
      final Throwable e
  )
  {
    final Either<BytesFullResponseHolder, IntermediateType> prevHolder = clientResponse.getObj();

    if (prevHolder.isValue()) {
      okHandler.exceptionCaught(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.valueOrThrow()
          ),
          e
      );
    } else {
      errorHandler.exceptionCaught(
          new ClientResponse<>(
              clientResponse.isFinished(),
              clientResponse.isContinueReading(),
              prevHolder.error()
          ),
          e
      );
    }
  }
}
