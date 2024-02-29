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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client to query data servers given a query.
 */
public class DataServerClient
{
  private static final String BASE_PATH = "/druid/v2/";
  private static final Logger log = new Logger(DataServerClient.class);
  private final ServiceClient serviceClient;
  private final ObjectMapper objectMapper;
  private final ServiceLocation serviceLocation;
  private final ScheduledExecutorService queryCancellationExecutor;

  public DataServerClient(
      ServiceClientFactory serviceClientFactory,
      ServiceLocation serviceLocation,
      ObjectMapper objectMapper,
      ScheduledExecutorService queryCancellationExecutor
  )
  {
    this.serviceClient = serviceClientFactory.makeClient(
        serviceLocation.getHost(),
        FixedSetServiceLocator.forServiceLocation(serviceLocation),
        StandardRetryPolicy.noRetries()
    );
    this.serviceLocation = serviceLocation;
    this.objectMapper = objectMapper;
    this.queryCancellationExecutor = queryCancellationExecutor;
  }

  public <T> Sequence<T> run(Query<T> query, ResponseContext responseContext, JavaType queryResultType, Closer closer)
  {
    final String cancelPath = BASE_PATH + query.getId();

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
        new FutureCallback<InputStream>()
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
              cancelQuery(query, cancelPath);
            }
          }
        },
        Execs.directExecutor()
    );

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<>(
                queryResultType,
                resultStreamFuture,
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
    );
  }

  private void cancelQuery(Query<?> query, String cancelPath)
  {
    Runnable cancelRunnable = () -> {
      Future<StatusResponseHolder> cancelFuture = serviceClient.asyncRequest(
          new RequestBuilder(HttpMethod.DELETE, cancelPath),
          StatusResponseHandler.getInstance());

      Runnable checkRunnable = () -> {
        try {
          if (!cancelFuture.isDone()) {
            log.error("Error cancelling query[%s]", query);
          }
          StatusResponseHolder response = cancelFuture.get();
          if (response.getStatus().getCode() >= 500) {
            log.error("Error cancelling query[%s]: queryable node returned status[%d] [%s].",
                      query,
                      response.getStatus().getCode(),
                      response.getStatus().getReasonPhrase());
          }
        }
        catch (ExecutionException | InterruptedException e) {
          log.error(e, "Error cancelling query[%s]", query);
        }
      };
      queryCancellationExecutor.schedule(checkRunnable, 5, TimeUnit.SECONDS);
    };
    queryCancellationExecutor.submit(cancelRunnable);
  }
}
