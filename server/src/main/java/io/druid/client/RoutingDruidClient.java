/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.guice.annotations.Client;
import io.druid.query.Query;
import io.druid.server.router.Router;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class RoutingDruidClient<IntermediateType, FinalType>
{
  private static final Logger log = new Logger(RoutingDruidClient.class);

  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;

  private final AtomicInteger openConnections;
  private final boolean isSmile;

  @Inject
  public RoutingDruidClient(
      ObjectMapper objectMapper,
      @Router HttpClient httpClient
  )
  {
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;

    this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    this.openConnections = new AtomicInteger();
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  public ListenableFuture<FinalType> post(
      String url,
      Query query,
      HttpResponseHandler<IntermediateType, FinalType> responseHandler
  )
  {
    final ListenableFuture<FinalType> future;

    try {
      log.debug("Querying url[%s]", url);
      future = httpClient
          .post(new URL(url))
          .setContent(objectMapper.writeValueAsBytes(query))
          .setHeader(HttpHeaders.Names.CONTENT_TYPE, isSmile ? "application/smile" : "application/json")
          .go(responseHandler);

      openConnections.getAndIncrement();

      Futures.addCallback(
          future,
          new FutureCallback<FinalType>()
          {
            @Override
            public void onSuccess(FinalType result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
            }
          }
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return future;
  }

  public ListenableFuture<FinalType> get(
      String url,
      HttpResponseHandler<IntermediateType, FinalType> responseHandler
  )
  {
    try {
      return httpClient
          .get(new URL(url))
          .go(responseHandler);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
