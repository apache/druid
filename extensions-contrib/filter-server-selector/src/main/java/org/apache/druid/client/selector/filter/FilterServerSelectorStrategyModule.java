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

package org.apache.druid.client.selector.filter;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.filter.lookup.LookupAwareFilterStrategy;
import org.apache.druid.client.selector.filter.lookup.LookupStatusView;
import org.apache.druid.client.selector.filter.lookup.LookupStatusViewConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

public class FilterServerSelectorStrategyModule implements DruidModule
{

  @Override
  public void configure(Binder binder)
  {
    LifecycleModule.register(binder, LookupStatusView.class);
    JsonConfigProvider.bind(binder, "druid.broker.balancer.filter", ServerFilterStrategyConfig.class);
    JsonConfigProvider.bind(binder, "druid.broker.balancer.filter.lookups.view", LookupStatusViewConfig.class);
    JsonConfigProvider.bind(binder, "druid.broker.balancer.filter.after", ServerSelectorStrategy.class,
        DelegateServerSelectorStrategy.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(new SimpleModule("FilterServerSelectorModule")
        .registerSubtypes(new NamedType(FilterServerSelectorStrategy.class, "filter")));
  }

  @Provides
  @LazySingleton
  @Named("no-op")
  public ServerFilterStrategy makeNoOpFilter()
  {
    return new NoOpServerFilterStrategy();
  }

  @Provides
  @LazySingleton
  @Named("lookup-aware")
  public ServerFilterStrategy makeLookupAwareFilter(LookupStatusView lookupStatusView)
  {
    return new LookupAwareFilterStrategy(lookupStatusView);
  }

  @Provides
  @LazySingleton
  public ComposingServerFilterStrategy makeCompsingServerFilterStrategy(ServerFilterStrategyConfig config,
      final Injector injector)
  {
    return new ComposingServerFilterStrategy(
        Lists.transform(config.getFilters(), new Function<String, ServerFilterStrategy>()
        {
          @Override
          public ServerFilterStrategy apply(String s)
          {
            return injector.getInstance(Key.get(ServerFilterStrategy.class, Names.named(s)));
          }
        }));
  }
}
