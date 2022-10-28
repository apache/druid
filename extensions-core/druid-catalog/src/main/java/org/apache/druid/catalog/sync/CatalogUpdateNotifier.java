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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.http.CatalogListenerResource;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.sync.RestUpdateSender.RestSender;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;

import javax.inject.Inject;

import java.util.Collections;
import java.util.function.Supplier;

/**
 * Global update notifier for the catalog. Registers itself as a catalog
 * listener, then uses the common cache notifier to send Smile-encoded JSON
 * updates to broker nodes discovered from node discovery (typically ZooKeeper.)
 * <p>
 * Deletes are encoded as a table update with a table definition of a special
 * tombstone type. This saves having the need for two endpoints, or having
 * a wrapper class to handle deletes.
 */
@ManageLifecycle
public class CatalogUpdateNotifier implements CatalogUpdateListener
{
  private static final String CALLER_NAME = "Catalog Sync";
  private static final long TIMEOUT_MS = 5000;

  private final CacheNotifier notifier;
  private final ObjectMapper smileMapper;

  @Inject
  public CatalogUpdateNotifier(
      CatalogStorage catalog,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient,
      @Smile ObjectMapper smileMapper
  )
  {
    long timeoutMs = TIMEOUT_MS;
    this.smileMapper = smileMapper;
    Supplier<Iterable<DruidNode>> nodeSupplier = new ListeningNodeSupplier(
        Collections.singletonList(NodeRole.BROKER),
        discoveryProvider);
    RestSender restSender = RestUpdateSender.httpClientSender(httpClient, Duration.millis(timeoutMs));
    RestUpdateSender sender = new RestUpdateSender(
        CALLER_NAME,
        nodeSupplier,
        restSender,
        CatalogListenerResource.BASE_URL + CatalogListenerResource.SYNC_URL,
        timeoutMs);
    this.notifier = new CacheNotifier(
        CALLER_NAME,
        sender);
    catalog.register(this);
  }

  @LifecycleStart
  public void start()
  {
    notifier.start();
  }

  @LifecycleStop
  public void stop()
  {
    notifier.stop();
  }

  @Override
  public void updated(UpdateEvent event)
  {
    try {
      notifier.send(smileMapper.writeValueAsBytes(event));
    }
    catch (JsonProcessingException e) {
      throw new ISE("Failed to serialize " + event.getClass().getSimpleName());
    }
  }
}
