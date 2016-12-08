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
package io.druid.indexing.overlord.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TierRouteConfigTest
{
  private static final String TIER = "tier";
  private static final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.of(new Module()
      {
        @Override
        public void configure(Binder binder)
        {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
        }
      })
  );
  private final TierRouteConfig routeConfig = new TierRouteConfig();
  private final TierTaskRunnerFactory factory = new UnknownRouteFactory();

  @Before
  public void setUp()
  {
    routeConfig.mapper = injector.getInstance(ObjectMapper.class);
    routeConfig.tierMap = new HashMap<>();
  }

  @Test
  public void testGetRouteFactory() throws Exception
  {
    populateConfig(TIER, factory);
    Assert.assertTrue(routeConfig.getRouteFactory(TIER) instanceof UnknownRouteFactory);
  }

  @Test
  public void testGetTiers() throws Exception
  {
    Assert.assertEquals(ImmutableSet.<String>of(), routeConfig.getTiers());
    populateConfig(TIER, factory);
    Assert.assertEquals(ImmutableSet.of(TIER), routeConfig.getTiers());
  }

  private void populateConfig(final String tier, final TierTaskRunnerFactory factory)
  {
    routeConfig.tierMap.put(
        tier,
        routeConfig.mapper.<Map<String, Object>>convertValue(
            factory,
            new TypeReference<Map<String, Object>>()
            {
            }
        )
    );
  }
}

