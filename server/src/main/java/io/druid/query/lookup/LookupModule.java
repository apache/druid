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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.ExpressionModule;
import io.druid.discovery.LookupNodeService;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.LookupDimensionSpec;
import io.druid.query.expression.LookupExprMacro;
import io.druid.server.DruidNode;
import io.druid.server.http.HostAndPortWithScheme;
import io.druid.server.http.security.ConfigResourceFilter;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.initialization.jetty.JettyBindings;
import io.druid.server.listener.announcer.ListenerResourceAnnouncer;
import io.druid.server.listener.announcer.ListeningAnnouncerConfig;
import io.druid.server.listener.resource.AbstractListenerHandler;
import io.druid.server.listener.resource.ListenerResource;
import io.druid.server.lookup.cache.LookupCoordinatorManager;
import io.druid.server.metrics.DataSourceTaskIdHolder;
import org.apache.curator.utils.ZKPaths;

import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LookupModule implements DruidModule
{
  static final String PROPERTY_BASE = "druid.lookup";
  public static final String FAILED_UPDATES_KEY = "failedUpdates";

  public static String getTierListenerPath(String tier)
  {
    return ZKPaths.makePath(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY, tier);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidLookupModule").registerSubtypes(MapLookupExtractorFactory.class),
        new SimpleModule().registerSubtypes(
            new NamedType(LookupDimensionSpec.class, "lookup"),
            new NamedType(RegisteredLookupExtractionFn.class, "registeredLookup")
        )
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
    ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
    LifecycleModule.register(binder, LookupResourceListenerAnnouncer.class);
    // Nothing else starts this, so we bind it to get it to start
    binder.bind(LookupResourceListenerAnnouncer.class).in(ManageLifecycle.class);
    JettyBindings.addQosFilter(
        binder,
        ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY,
        2 // 1 for "normal" operation and 1 for "emergency" or other
    );
  }

  @Provides
  @LazySingleton
  public LookupNodeService getLookupNodeService(LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig)
  {
    return new LookupNodeService(lookupListeningAnnouncerConfig.getLookupTier());
  }
}

@Path(ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY)
@ResourceFilters(ConfigResourceFilter.class)
class LookupListeningResource extends ListenerResource
{
  private static final Logger LOG = new Logger(LookupListeningResource.class);

  private static final TypeReference<LookupsState<LookupExtractorFactoryContainer>> LOOKUPS_STATE_TYPE_REFERENCE =
      new TypeReference<LookupsState<LookupExtractorFactoryContainer>>()
      {
      };

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
          @Override
          public Response handleUpdates(
              InputStream inputStream, ObjectMapper mapper
          )
          {
            final LookupsState<LookupExtractorFactoryContainer> state;
            try {
              state = mapper.readValue(inputStream, LOOKUPS_STATE_TYPE_REFERENCE);
            }
            catch (final IOException ex) {
              LOG.debug(ex, "Bad request");
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ServletResourceUtils.sanitizeException(ex))
                             .build();
            }

            try {
              state.getToLoad().forEach(manager::add);
              state.getToDrop().forEach(manager::remove);

              return Response.status(Response.Status.ACCEPTED).entity(manager.getAllLookupsState()).build();
            }
            catch (Exception e) {
              LOG.error(e, "Error handling request");
              return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
            }
          }

          @Override
          public Object post(final Map<String, LookupExtractorFactory> lookups)
              throws Exception
          {
            final Map<String, LookupExtractorFactory> failedUpdates = new HashMap<>();
            for (final String name : lookups.keySet()) {

              final LookupExtractorFactoryContainer factoryContainer = new LookupExtractorFactoryContainer(
                  null,
                  lookups.get(name)
              );

              manager.add(name, factoryContainer);
            }
            return ImmutableMap.of("status", "accepted", LookupModule.FAILED_UPDATES_KEY, failedUpdates);
          }

          @Override
          public Object get(String id)
          {
            return manager.get(id);
          }

          @Override
          public LookupsState<LookupExtractorFactoryContainer> getAll()
          {
            return manager.getAllLookupsState();
          }

          @Override
          public Object delete(String id)
          {
            manager.remove(id);
            return id;
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
        HostAndPortWithScheme.fromString(node.getServiceScheme(), node.getHostAndPortToUse())
    );
  }
}


class LookupListeningAnnouncerConfig extends ListeningAnnouncerConfig
{
  public static final String DEFAULT_TIER = "__default";
  private final DataSourceTaskIdHolder dataSourceTaskIdHolder;
  @JsonProperty("lookupTier")
  private String lookupTier = null;
  @JsonProperty("lookupTierIsDatasource")
  private boolean lookupTierIsDatasource = false;

  @JsonCreator
  public LookupListeningAnnouncerConfig(
      @JacksonInject ZkPathsConfig zkPathsConfig,
      @JacksonInject DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    super(zkPathsConfig);
    this.dataSourceTaskIdHolder = dataSourceTaskIdHolder;
  }

  public String getLookupTier()
  {
    Preconditions.checkArgument(
        !(lookupTierIsDatasource && null != lookupTier),
        "Cannot specify both `lookupTier` and `lookupTierIsDatasource`"
    );
    final String lookupTier = lookupTierIsDatasource ? dataSourceTaskIdHolder.getDataSource() : this.lookupTier;
    return Preconditions.checkNotNull(
        lookupTier == null ? DEFAULT_TIER : Strings.emptyToNull(lookupTier),
        "Cannot have empty lookup tier from %s",
        lookupTierIsDatasource ? "bound value" : LookupModule.PROPERTY_BASE
    );
  }

  public String getLookupKey()
  {
    return LookupModule.getTierListenerPath(getLookupTier());
  }
}
