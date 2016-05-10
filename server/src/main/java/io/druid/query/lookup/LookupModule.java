/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.RE;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.DruidModule;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.listener.announcer.ListenerResourceAnnouncer;
import io.druid.server.listener.announcer.ListeningAnnouncerConfig;
import io.druid.server.listener.resource.AbstractListenerHandler;
import io.druid.server.listener.resource.ListenerResource;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.curator.utils.ZKPaths;

import javax.ws.rs.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LookupModule implements DruidModule
{
  private static final String PROPERTY_BASE = "druid.lookup";
  public static final String FAILED_UPDATES_KEY = "failedUpdates";

  public static String getTierListenerPath(String tier)
  {
    return ZKPaths.makePath(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY, tier);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidLookupModule").registerSubtypes(MapLookupExtractorFactory.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, PROPERTY_BASE, LookupConfig.class);
    LifecycleModule.register(binder, LookupReferencesManager.class);
    JsonConfigProvider.bind(binder, PROPERTY_BASE, LookupListeningAnnouncerConfig.class);
    Jerseys.addResource(binder, LookupListeningResource.class);
    Jerseys.addResource(binder, LookupIntrospectionResource.class);
    LifecycleModule.register(binder, LookupResourceListenerAnnouncer.class);
    // Nothing else starts this, so we bind it to get it to start
    binder.bind(LookupResourceListenerAnnouncer.class).in(ManageLifecycle.class);
  }
}

@Path(ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)
class LookupListeningResource extends ListenerResource
{
  private static final Logger LOG = new Logger(LookupListeningResource.class);

  @Inject
  public LookupListeningResource(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final LookupReferencesManager manager
  )
  {
    super(
        jsonMapper,
        smileMapper,
        new AbstractListenerHandler<LookupExtractorFactory>(new TypeReference<LookupExtractorFactory>()
        {
        })
        {
          private final Object deleteLock = new Object();

          @Override
          public synchronized Object post(final Map<String, LookupExtractorFactory> lookups)
              throws Exception
          {
            final Map<String, LookupExtractorFactory> failedUpdates = new HashMap<>();
            for (final String name : lookups.keySet()) {
              final LookupExtractorFactory factory = lookups.get(name);
              try {
                if (!manager.updateIfNew(name, factory)) {
                  failedUpdates.put(name, factory);
                }
              }
              catch (ISE ise) {
                LOG.error(ise, "Error starting [%s]: [%s]", name, factory);
                failedUpdates.put(name, factory);
              }
            }
            return ImmutableMap.of("status", "accepted", LookupModule.FAILED_UPDATES_KEY, failedUpdates);
          }

          @Override
          public Object get(String id)
          {
            return manager.get(id);
          }

          @Override
          public Map<String, LookupExtractorFactory> getAll()
          {
            return manager.getAll();
          }

          @Override
          public Object delete(String id)
          {
            // Prevent races to 404 vs 500 between concurrent delete requests
            synchronized (deleteLock) {
              if (manager.get(id) == null) {
                return null;
              }
              if (!manager.remove(id)) {
                // We don't have more information at this point.
                throw new RE("Could not remove lookup [%s]", id);
              }
              return id;
            }
          }
        }
    );
  }
}

class LookupResourceListenerAnnouncer extends ListenerResourceAnnouncer
{
  @Inject
  public LookupResourceListenerAnnouncer(
      Announcer announcer,
      LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig,
      @Self DruidNode node
  )
  {
    super(
        announcer,
        lookupListeningAnnouncerConfig,
        lookupListeningAnnouncerConfig.getLookupKey(),
        HostAndPort.fromString(node.getHostAndPort())
    );
  }
}


class LookupListeningAnnouncerConfig extends ListeningAnnouncerConfig
{
  public static final String DEFAULT_TIER = "__default";
  @JsonProperty("lookupTier")
  private String lookupTier = null;

  @JsonCreator
  public static LookupListeningAnnouncerConfig createLookupListeningAnnouncerConfig(
      @JacksonInject ZkPathsConfig zkPathsConfig,
      @JsonProperty("lookupTier") String lookupTier
  )
  {
    final LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig = new LookupListeningAnnouncerConfig(
        zkPathsConfig);
    lookupListeningAnnouncerConfig.lookupTier = lookupTier;
    return lookupListeningAnnouncerConfig;
  }

  @Inject
  public LookupListeningAnnouncerConfig(ZkPathsConfig zkPathsConfig)
  {
    super(zkPathsConfig);
  }

  public String getLookupTier()
  {
    return lookupTier == null ? DEFAULT_TIER : lookupTier;
  }

  public String getLookupKey()
  {
    return LookupModule.getTierListenerPath(getLookupTier());
  }
}
