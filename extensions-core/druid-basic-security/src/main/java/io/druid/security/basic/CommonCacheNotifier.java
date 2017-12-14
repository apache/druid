/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CommonCacheNotifier
{
  private static final EmittingLogger LOG = new EmittingLogger(CommonCacheNotifier.class);

  private static final List<String> NODE_TYPES = Arrays.asList(
      DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
      DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
      DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL,
      DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
      DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER,
      DruidNodeDiscoveryProvider.NODE_TYPE_MM
  );

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpClient httpClient;
  private final BlockingQueue<Pair<String, byte[]>> updateQueue;
  private final Map<String, BasicAuthDBConfig> itemConfigMap;
  private final String baseUrl;
  private final String callerName;
  private final ExecutorService exec;

  public CommonCacheNotifier(
      Map<String, BasicAuthDBConfig> itemConfigMap,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpClient httpClient,
      String baseUrl,
      String callerName
  )
  {
    this.exec = Execs.scheduledSingleThreaded(StringUtils.format("%s-notifierThread-", callerName) + "%d");
    this.callerName = callerName;
    this.updateQueue = new LinkedBlockingQueue<>();
    this.itemConfigMap = itemConfigMap;
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.baseUrl = baseUrl;
  }

  public void start()
  {
    exec.submit(
        () -> {
          while (!Thread.interrupted()) {
            try {
              LOG.debug(callerName + ":Waiting for cache update notification");
              Pair<String, byte[]> update = updateQueue.take();
              String authorizer = update.lhs;
              byte[] serializedMap = update.rhs;
              BasicAuthDBConfig authorizerConfig = itemConfigMap.get(update.lhs);
              if (!authorizerConfig.isEnableCacheNotifications()) {
                continue;
              }

              LOG.debug(callerName + ":Sending cache update notifications");
              // Best effort, if a notification fails, the remote node will eventually poll to update its state
              // We wait for responses however, to avoid flooding remote nodes with notifications.
              List<ListenableFuture<StatusResponseHolder>> futures = sendUpdate(
                  authorizer,
                  serializedMap
              );

              try {
                List<StatusResponseHolder> responses = Futures.allAsList(futures)
                                                              .get(
                                                                  authorizerConfig.getCacheNotificationTimeout(),
                                                                  TimeUnit.MILLISECONDS
                                                              );

                for (StatusResponseHolder response : responses) {
                  LOG.debug(callerName + ":Got status: " + response.getStatus());
                }
              }
              catch (Exception e) {
                LOG.makeAlert(e, callerName + ":Failed to get response for cache notification.").emit();
              }

              LOG.debug(callerName + ":Received responses for cache update notifications.");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, callerName + ":Error occured while handling updates for cachedUserMaps.").emit();
            }
          }
        }
    );
  }

  public void stop()
  {
    exec.shutdownNow();
  }

  public void addUpdate(String updatedItemName, byte[] updatedItemData)
  {
    updateQueue.add(
        new Pair<>(updatedItemName, updatedItemData)
    );
  }

  private List<ListenableFuture<StatusResponseHolder>> sendUpdate(String updatedAuthorizerPrefix, byte[] serializedUserMap)
  {
    List<ListenableFuture<StatusResponseHolder>> futures = new ArrayList<>();
    for (String nodeType : NODE_TYPES) {
      DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeType(nodeType);
      Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
      for (DiscoveryDruidNode node : nodes) {
        URL listenerURL = getListenerURL(node.getDruidNode(), baseUrl, updatedAuthorizerPrefix);

        // best effort, if this fails, remote node will poll and pick up the update eventually
        Request req = new Request(HttpMethod.POST, listenerURL);
        req.setContent(MediaType.APPLICATION_JSON, serializedUserMap);

        BasicAuthDBConfig itemConfig = itemConfigMap.get(updatedAuthorizerPrefix);

        ListenableFuture<StatusResponseHolder> future = httpClient.go(
            req,
            new ResponseHandler(),
            Duration.millis(itemConfig.getCacheNotificationTimeout())
        );
        futures.add(future);
      }
    }
    return futures;
  }

  private URL getListenerURL(DruidNode druidNode, String baseUrl, String itemName)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          StringUtils.format(baseUrl, itemName)
      );
    }
    catch (MalformedURLException mue) {
      LOG.error(callerName + ":WTF? Malformed url for DruidNode[%s] and itemName[%s]", druidNode, itemName);
      throw new RuntimeException(mue);
    }
  }

  // Based off StatusResponseHandler, but with response content ignored
  private static class ResponseHandler implements HttpResponseHandler<StatusResponseHolder, StatusResponseHolder>
  {
    protected static final Logger log = new Logger(ResponseHandler.class);

    @Override
    public ClientResponse<StatusResponseHolder> handleResponse(HttpResponse response)
    {
      return ClientResponse.unfinished(
          new StatusResponseHolder(
              response.getStatus(),
              null
          )
      );
    }

    @Override
    public ClientResponse<StatusResponseHolder> handleChunk(
        ClientResponse<StatusResponseHolder> response,
        HttpChunk chunk
    )
    {
      return response;
    }

    @Override
    public ClientResponse<StatusResponseHolder> done(ClientResponse<StatusResponseHolder> response)
    {
      return ClientResponse.finished(response.getObj());
    }

    @Override
    public void exceptionCaught(
        ClientResponse<StatusResponseHolder> clientResponse, Throwable e
    )
    {
      // Its safe to Ignore as the ClientResponse returned in handleChunk were unfinished
      log.error(e, "exceptionCaught in CommonCacheNotifier ResponseHandler.");
    }
  }
}
