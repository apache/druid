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
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CommonCacheNotifier
{
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
  private final Set<String> itemsToUpdate;
  private final Map<String, byte[]> serializedMaps;
  private final Map<String, BasicAuthDBConfig> itemConfigMap;
  private final String baseUrl;
  private final EmittingLogger logger;
  private final String threadName;

  private Thread notifierThread;

  public CommonCacheNotifier(
      Map<String, BasicAuthDBConfig> itemConfigMap,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpClient httpClient,
      String baseUrl,
      String threadName,
      EmittingLogger logger
  )
  {
    this.threadName = threadName;
    this.logger = logger;
    this.itemsToUpdate = new HashSet<>();
    this.itemConfigMap = itemConfigMap;
    this.serializedMaps = new HashMap<>();
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.baseUrl = baseUrl;
  }

  public void start()
  {
    notifierThread = Execs.makeThread(
        threadName,
        () -> {
          while (!Thread.interrupted()) {
            try {
              logger.debug("Waiting for cache update notification");
              Set<String> itemsToUpdateSnapshot;
              HashMap<String, byte[]> serializedMapsSnapshot;
              synchronized (itemsToUpdate) {
                if (itemsToUpdate.isEmpty()) {
                  itemsToUpdate.wait();
                }
                itemsToUpdateSnapshot = new HashSet<>(itemsToUpdate);
                serializedMapsSnapshot = new HashMap<String, byte[]>(serializedMaps);
                itemsToUpdate.clear();
                serializedMaps.clear();
              }
              logger.debug("Sending cache update notifications");
              for (String authorizer : itemsToUpdateSnapshot) {
                BasicAuthDBConfig authorizerConfig = itemConfigMap.get(authorizer);
                if (!authorizerConfig.isEnableCacheNotifications()) {
                  continue;
                }

                // Best effort, if a notification fails, the remote node will eventually poll to update its state
                // We wait for responses however, to avoid flooding remote nodes with notifications.
                List<ListenableFuture<StatusResponseHolder>> futures = sendUpdate(
                    authorizer,
                    serializedMapsSnapshot.get(authorizer)
                );
                for (ListenableFuture<StatusResponseHolder> future : futures) {
                  try {
                    StatusResponseHolder srh = future.get(
                        authorizerConfig.getCacheNotificationTimeout(), TimeUnit.MILLISECONDS
                    );
                    logger.debug("Got status: " + srh.getStatus());
                  }
                  catch (Exception e) {
                    logger.makeAlert(e, "Failed to get response for cache notification.").emit();
                  }
                }
              }
              logger.debug("Received responses for cache update notifications.");
            }
            catch (Throwable t) {
              logger.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
            }
          }
        },
        true
    );
    notifierThread.start();
  }

  public void addUpdate(String updatedItemName, byte[] updatedItemData)
  {
    synchronized (itemsToUpdate) {
      itemsToUpdate.add(updatedItemName);
      serializedMaps.put(updatedItemName, updatedItemData);
      itemsToUpdate.notify();
    }
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
      logger.error("WTF? Malformed url for DruidNode[%s] and itemName[%s]", druidNode, itemName);
      throw new RuntimeException(mue);
    }
  }

  // Based off StatusResponseHandler, but with response content ignored
  private static class ResponseHandler implements HttpResponseHandler<StatusResponseHolder, StatusResponseHolder>
  {
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
    }
  }
}
