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

package org.apache.druid.discovery;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.io.InputStream;

/**
 * Client to query data servers given a query.
 */
public class DataServerClient
{
  private static final Logger log = new Logger(DataServerClient.class);
  private static final String BASE_PATH = "/druid/v2/";
  private static final Duration CANCELLATION_TIMEOUT = Duration.standardSeconds(5);

  private final ServiceClient serviceClient;
  private final ObjectMapper objectMapper;
  private final ServiceLocation serviceLocation;

  public DataServerClient(
      ServiceClientFactory serviceClientFactory,
      ServiceLocation serviceLocation,
      ObjectMapper objectMapper,
      ServiceRetryPolicy retryPolicy
  )
  {
    this.serviceClient = serviceClientFactory.makeClient(
        serviceLocation.getHost(),
        new FixedServiceLocator(serviceLocation),
        retryPolicy
    );
    this.serviceLocation = serviceLocation;
    this.objectMapper = objectMapper;
  }

  /**
   * Issue a query. Returns a future that resolves when the server starts sending its response.
   *
   * @param query           query to run
   * @param responseContext response context to populate
   * @param queryResultType type of result object
   * @param closer          closer; this call will register a query canceler with this closer
   */
  public <T> ListenableFuture<Sequence<T>> run(
      final Query<T> query,
      final ResponseContext responseContext,
      final JavaType queryResultType,
      final Closer closer
  )
  {
    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, BASE_PATH);
    final boolean isSmile = objectMapper.getFactory() instanceof SmileFactory;
    if (isSmile) {
      requestBuilder = requestBuilder.smileContent(objectMapper, query);
    } else {
      requestBuilder = requestBuilder.jsonContent(objectMapper, query);
    }

    if (log.isDebugEnabled()) {
      log.debug("Sending request to servers for query[%s], request[%s]", query.getId(), requestBuilder);
    }
    ListenableFuture<InputStream> resultStreamFuture = serviceClient.asyncRequest(
        requestBuilder,
        new DataServerResponseHandler(query, responseContext, objectMapper)
    );

    closer.register(() -> resultStreamFuture.cancel(true));
    Futures.addCallback(
        resultStreamFuture,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(InputStream result)
          {
            // Do nothing
          }

          @Override
          public void onFailure(Throwable t)
          {
            if (resultStreamFuture.isCancelled()) {
              cancelQuery(query.getId());
            }
          }
        },
        Execs.directExecutor()
    );

    return FutureUtils.transform(
        resultStreamFuture,
        resultStream -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
            {
              @Override
              public JsonParserIterator<T> make()
              {
                return new JsonParserIterator<>(
                    queryResultType,
                    Futures.immediateFuture(resultStream),
                    BASE_PATH,
                    query,
                    serviceLocation.getHost(),
                    objectMapper
                );
              }

              @Override
              public void cleanup(JsonParserIterator<T> iterFromMake)
              {
                CloseableUtils.closeAndWrapExceptions(iterFromMake);
              }
            }
        )
    );
  }

  private void cancelQuery(final String queryId)
  {
    if (queryId == null) {
      throw DruidException.defensive("Null queryId");
    }

    final String cancelPath = BASE_PATH + queryId;

    final ListenableFuture<Void> cancelFuture = serviceClient.asyncRequest(
        new RequestBuilder(HttpMethod.DELETE, cancelPath).timeout(CANCELLATION_TIMEOUT),
        IgnoreHttpResponseHandler.INSTANCE
    );

    Futures.addCallback(
        cancelFuture,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(final Void result)
          {
            // Do nothing on successful cancellation.
          }

          @Override
          public void onFailure(final Throwable t)
          {
            log.noStackTrace()
               .warn(t, "Failed to cancel query[%s] on server[%s]", queryId, serviceLocation.getHostAndPort());
          }
        },
        Execs.directExecutor()
    );
  }
}
