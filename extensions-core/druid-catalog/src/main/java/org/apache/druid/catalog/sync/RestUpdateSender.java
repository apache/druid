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

package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Sends updates to a set of Druid nodes provided by the given supplier.
 * The update is provided by the caller in serialized form. The class sends
 * updates concurrently, and returns futures for all the requests.
 *
 * Updates are processed one by one, but each updates is sent concurrently.
 * All responses from all receivers must arrive (or a timeout must occur)
 * before the next updates can be sent. As a result, this class is suitable for
 * low-frequency updates: where the worst-case send times are less than
 * the worst-case update frequency. If updates are faster, they will back
 * up, and the class should be redesigned to allow healthy receivers to
 * continue to get updates while laggards block only themselves.
 *
 * Defined by composition so it can be tested and reused in other
 * contexts.
 */
public class RestUpdateSender implements Consumer<byte[]>
{
  private static final EmittingLogger LOG = new EmittingLogger(RestUpdateSender.class);

  public interface RestSender
  {
    ListenableFuture<StatusResponseHolder> send(URL listenerURL, byte[] serializedEntity);
  }

  private static class HttpClientSender implements RestSender
  {
    private final HttpClient httpClient;
    private final Duration cacheNotificationsTimeout;

    private HttpClientSender(
        HttpClient httpClient,
        Duration cacheNotificationsTimeout)
    {
      this.httpClient = httpClient;
      this.cacheNotificationsTimeout = cacheNotificationsTimeout;
    }

    @Override
    public ListenableFuture<StatusResponseHolder> send(URL listenerURL, byte[] serializedEntity)
    {
      // Best effort, if this fails, remote node will poll
      // and pick up the update eventually.
      return httpClient.go(
          new Request(HttpMethod.POST, listenerURL)
              .setContent(SmileMediaTypes.APPLICATION_JACKSON_SMILE, serializedEntity),
          StatusResponseHandler.getInstance(),
          cacheNotificationsTimeout
      );
    }
  }

  private final String callerName;
  private final Supplier<Iterable<DruidNode>> destinationSupplier;
  private final String baseUrl;
  private final RestSender sender;
  private final long cacheNotificationsTimeoutMs;

  public RestUpdateSender(
      final String callerName,
      final Supplier<Iterable<DruidNode>> destinationSupplier,
      final RestSender sender,
      final String baseUrl,
      final long cacheNotificationsTimeoutMs
  )
  {
    this.callerName = callerName;
    this.destinationSupplier = destinationSupplier;
    this.sender = sender;
    this.baseUrl = baseUrl;
    this.cacheNotificationsTimeoutMs = cacheNotificationsTimeoutMs;
  }

  public static RestSender httpClientSender(HttpClient httpClient, Duration cacheNotificationsTimeou)
  {
    return new HttpClientSender(httpClient, cacheNotificationsTimeou);
  }

  @Override
  public void accept(byte[] serializedEntity)
  {
    LOG.debug(callerName + ": Sending update notifications");

    // Best effort, if a notification fails, the remote node will eventually poll to update its state
    // We wait for responses however, to avoid flooding remote nodes with notifications.
    List<ListenableFuture<StatusResponseHolder>> futures = new ArrayList<>();
    for (DruidNode node : destinationSupplier.get()) {
      futures.add(
          sender.send(
              getListenerURL(node, baseUrl),
              serializedEntity));
    }

    try {
      List<StatusResponseHolder> responses = getResponsesFromFutures(futures);

      for (StatusResponseHolder response : responses) {
        if (response == null) {
          LOG.error("Got null future response from update request.");
          continue;
        }
        HttpResponseStatus status = response.getStatus();
        if (HttpResponseStatus.OK.equals(status) ||
            HttpResponseStatus.ACCEPTED.equals(status)) {
          LOG.debug("Got status [%s]", status);
        } else {
          LOG.error("Got error status [%s], content [%s]", status, response.getContent());
        }
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, callerName + ": Failed to get response for cache notification.").emit();
    }

    LOG.debug(callerName + ": Received responses for cache update notifications.");
  }

  @VisibleForTesting
  List<StatusResponseHolder> getResponsesFromFutures(
      List<ListenableFuture<StatusResponseHolder>> futures
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    return Futures.successfulAsList(futures)
                  .get(
                      cacheNotificationsTimeoutMs,
                      TimeUnit.MILLISECONDS
                  );
  }

  private URL getListenerURL(DruidNode druidNode, String baseUrl)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          baseUrl
      );
    }
    catch (MalformedURLException mue) {
      String msg = StringUtils.format(callerName + ": Malformed url for DruidNode [%s] and baseUrl [%s]", druidNode, baseUrl);
      LOG.error(msg);
      throw new RE(mue, msg);
    }
  }
}
