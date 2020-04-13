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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.dimension.LookupDimensionSpec;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.server.initialization.jetty.JettyBindings;
import org.apache.druid.server.listener.resource.ListenerResource;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;

import java.util.List;

public class LookupModule implements DruidModule
{
  static final String PROPERTY_BASE = "druid.lookup";
  public static final String FAILED_UPDATES_KEY = "failedUpdates";
  public static final int LOOKUP_LISTENER_QOS_MAX_REQUESTS = 2;

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
    binder.bind(LookupExtractorFactoryContainerProvider.class).to(LookupReferencesManager.class);
    LifecycleModule.register(binder, LookupReferencesManager.class);
    JsonConfigProvider.bind(binder, PROPERTY_BASE, LookupListeningAnnouncerConfig.class);
    Jerseys.addResource(binder, LookupListeningResource.class);
    Jerseys.addResource(binder, LookupIntrospectionResource.class);
    ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
    JettyBindings.addQosFilter(
        binder,
        ListenerResource.BASE_PATH + "/" + LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY,
        LOOKUP_LISTENER_QOS_MAX_REQUESTS // 1 for "normal" operation and 1 for "emergency" or other
    );
  }

  @Provides
  @LazySingleton
  public LookupNodeService getLookupNodeService(LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig)
  {
    return new LookupNodeService(lookupListeningAnnouncerConfig.getLookupTier());
  }
}

