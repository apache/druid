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

package io.druid.security.basic.authentication.db.cache;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.BasicAuthDBConfig;
import io.druid.server.DruidNode;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
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

@ManageLifecycle
public class CoordinatorBasicAuthenticatorCacheNotifier implements BasicAuthenticatorCacheNotifier
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorBasicAuthenticatorCacheNotifier.class);

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
  private final Set<String> authenticatorsToUpdate;
  private final Map<String, byte[]> serializedMaps;
  private final Map<String, BasicAuthDBConfig> authenticatorConfigMap;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private Thread notifierThread;

  @Inject
  public CoordinatorBasicAuthenticatorCacheNotifier(
      AuthenticatorMapper authenticatorMapper,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.authenticatorsToUpdate = new HashSet<>();
    this.serializedMaps = new HashMap<>();
    this.authenticatorConfigMap = new HashMap<>();
    initAuthenticatorConfigMap(authenticatorMapper);
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      notifierThread = Execs.makeThread(
          "CoordinatorBasicAuthenticatorCacheNotifier-notifierThread",
          () -> {
            while (!Thread.interrupted()) {
              try {
                LOG.debug("Waiting for cache update notification");
                Set<String> authenticatorsToUpdateSnapshot;
                HashMap<String, byte[]> serializedUserMapsSnapshot;
                synchronized (authenticatorsToUpdate) {
                  if (authenticatorsToUpdate.isEmpty()) {
                    authenticatorsToUpdate.wait();
                  }
                  authenticatorsToUpdateSnapshot = new HashSet<>(authenticatorsToUpdate);
                  serializedUserMapsSnapshot = new HashMap<String, byte[]>(serializedMaps);
                  authenticatorsToUpdate.clear();
                  serializedMaps.clear();
                }
                LOG.debug("Sending cache update notifications");
                for (String authenticator : authenticatorsToUpdateSnapshot) {
                  BasicAuthDBConfig authenticatorConfig = authenticatorConfigMap.get(authenticator);
                  if (!authenticatorConfig.isEnableCacheNotifications()) {
                    continue;
                  }

                  // Best effort, if a notification fails, the remote node will eventually poll to update its state
                  // We wait for responses however, to avoid flooding remote nodes with notifications.
                  List<ListenableFuture<StatusResponseHolder>> futures = sendUpdate(
                      authenticator,
                      serializedUserMapsSnapshot.get(authenticator)
                  );
                  for (ListenableFuture<StatusResponseHolder> future : futures) {
                    try {
                      StatusResponseHolder srh = future.get(
                          authenticatorConfig.getCacheNotificationTimeout(), TimeUnit.MILLISECONDS
                      );
                      LOG.debug("Got status: " + srh.getStatus());
                    }
                    catch (Exception e) {
                      LOG.makeAlert("Failed to get response for cache notification.").emit();
                    }
                  }
                }
                LOG.debug("Received responses for cache update notifications.");
              }
              catch (Throwable t) {
                LOG.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
              }
            }
          },
          true
      );
      notifierThread.start();
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void addUpdate(String updatedAuthenticatorPrefix, byte[] userAndRoleMap)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    synchronized (authenticatorsToUpdate) {
      authenticatorsToUpdate.add(updatedAuthenticatorPrefix);
      serializedMaps.put(updatedAuthenticatorPrefix, userAndRoleMap);
      authenticatorsToUpdate.notify();
    }
  }

  private List<ListenableFuture<StatusResponseHolder>> sendUpdate(
      String updatedAuthenticatorPrefix,
      byte[] serializedUserAndRoleMap
  )
  {
    List<ListenableFuture<StatusResponseHolder>> futures = new ArrayList<>();
    for (String nodeType : NODE_TYPES) {
      DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeType(nodeType);
      Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
      for (DiscoveryDruidNode node : nodes) {
        URL listenerURL = getListenerURL(node.getDruidNode(), updatedAuthenticatorPrefix);

        // best effort, if this fails, remote node will poll and pick up the update eventually
        Request req = new Request(HttpMethod.POST, listenerURL);
        req.setContent(MediaType.APPLICATION_JSON, serializedUserAndRoleMap);

        BasicAuthDBConfig authenticatorConfig = authenticatorConfigMap.get(updatedAuthenticatorPrefix);

        ListenableFuture<StatusResponseHolder> future = httpClient.go(
            req,
            new ResponseHandler(),
            Duration.millis(authenticatorConfig.getCacheNotificationTimeout())
        );
        futures.add(future);
      }
    }
    return futures;
  }

  private void initAuthenticatorConfigMap(AuthenticatorMapper mapper)
  {
    if (mapper == null || mapper.getAuthenticatorMap() == null) {
      return;
    }

    for (Map.Entry<String, Authenticator> entry : mapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
        authenticatorConfigMap.put(authenticatorName, dbConfig);
      }
    }
  }

  private static URL getListenerURL(DruidNode druidNode, String authPrefix)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          StringUtils.format("/druid-ext/basic-security/authentication/listen/%s", authPrefix)
      );
    }
    catch (MalformedURLException mue) {
      LOG.error("WTF? Malformed url for DruidNode[%s] and authPrefix[%s]", druidNode, authPrefix);
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
