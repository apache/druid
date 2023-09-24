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
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.rpc.FixedSetServiceLocator;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
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
public class DataServerClient<T>
{
  private static final Logger log = new Logger(DataServerClient.class);
  private static final String SERVED_SEGMENT_CLIENT_NAME = "ServedSegmentClient";
  private final ServiceClient serviceClient;
  private final ObjectMapper objectMapper;
  private final ObjectMapper smileMapper;
  private final ScheduledExecutorService queryCancellationExecutor;

  public DataServerClient(
      ServiceClientFactory serviceClientFactory,
      FixedSetServiceLocator fixedSetServiceLocator,
      ObjectMapper objectMapper,
      ObjectMapper smileMapper
  )
  {
    serviceClient = serviceClientFactory.makeClient(
        SERVED_SEGMENT_CLIENT_NAME,
        fixedSetServiceLocator,
        StandardRetryPolicy.aboutAnHour()
    );
    this.objectMapper = objectMapper;
    this.smileMapper = smileMapper;
    this.queryCancellationExecutor = Execs.scheduledSingleThreaded("query-cancellation-executor");
  }


  public ResourceHolder<Sequence<T>> run(Query<T> query, ResponseContext context)
  {
    final String basePath = "/druid/v2/";
    final String cancelPath = basePath + query.getId();
    JavaType queryResultType;
    if (query instanceof ScanQuery) {
      queryResultType = objectMapper.getTypeFactory().constructType(ScanResultValue.class);
    } else {
      queryResultType = objectMapper.getTypeFactory().constructType(ResultRow.class);
    }

    Future<InputStream> resultStreamFuture;
    try {
      resultStreamFuture = Futures.immediateFuture(serviceClient.request(
          new RequestBuilder(HttpMethod.POST, basePath)
              .smileContent(smileMapper, query),
          new DataServerResponseHandler(query, context, smileMapper)
      ));

      BaseSequence<T, JsonParserIterator<T>> baseSequence = new BaseSequence<>(
          new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
          {
            @Override
            public JsonParserIterator<T> make()
            {
              return new JsonParserIterator<>(
                  queryResultType,
                  resultStreamFuture,
                  basePath,
                  query,
                  "",
                  smileMapper
              );
            }

            @Override
            public void cleanup(JsonParserIterator<T> iterFromMake)
            {
              CloseableUtils.closeAndWrapExceptions(iterFromMake);
            }
          }
      );

      return new ResourceHolder<Sequence<T>>()
      {
        @Override
        public Sequence<T> get()
        {
          return baseSequence;
        }

        @Override
        public void close()
        {
          if (!resultStreamFuture.isDone()) {
            cancelQuery(query, cancelPath);
          }
        }
      };
    }
    catch (Exception e) {
      cancelQuery(query, cancelPath);
      throw new RuntimeException(e);
    }
  }

  private void cancelQuery(Query<T> query, String cancelPath)
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
