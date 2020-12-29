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

package org.apache.druid.client.selector.filter.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelectorStrategy;
import org.apache.druid.client.selector.filter.ComposingServerFilterStrategy;
import org.apache.druid.client.selector.filter.FilterServerSelectorStrategyModule;
import org.apache.druid.client.selector.filter.NoOpServerFilterStrategy;
import org.apache.druid.client.selector.filter.ServerFilterStrategyConfig;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.JacksonModule;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterServerSelectorStrategyModuleTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  private final String FILTERING_SELECTOR_TYPE = "filter";

  private final FilterServerSelectorStrategyModule module = new FilterServerSelectorStrategyModule();

  private Injector injector;

  @Before
  public void setUp()
  {
    for (Module jacksonModule : module.getJacksonModules()) {
      mapper.registerModule(jacksonModule);
    }
    final Properties props = new Properties();
    props.put("druid.broker.balancer.type", FILTERING_SELECTOR_TYPE);
    props.put("druid.broker.balancer.filter.after.type", "connectionCount");
    props.put("druid.broker.balancer.filter.lookups.view.pollFrequencySeconds", "70");
    props.put("druid.broker.balancer.filter.filters", "[\"lookup-aware\",\"no-op\"]");

    final ImmutableList<com.google.inject.Module> modules = ImmutableList.of(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new ServerModule(),
        new JacksonModule(),
        module,
        new DruidModule()
        {

          @Override
          public void configure(Binder binder)
          {
            binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
            // binder.bind(JsonConfigurator.class).in(LazySingleton.class);

            LookupsCoordinatorClient client = EasyMock.createMock(LookupsCoordinatorClient.class);
            EasyMock.expect(client.fetchLookupNodeStatus()).andReturn(LookupAwareFilterStrategyTest.constructLookups()).anyTimes();
            EasyMock.replay(client);

            LookupStatusView view = new LookupStatusView(client, new LookupStatusViewConfig());
            try {
              view.start();
            }
            catch (InterruptedException e) {
              e.printStackTrace();
              Assert.fail();
            }
            view.stop();
            binder.bind(LookupStatusView.class).toInstance(view);
            binder.bind(Properties.class).toInstance(props);
            JsonConfigProvider.bind(binder, "druid.broker.balancer", ServerSelectorStrategy.class);

          }

          @Override
          public List<? extends Module> getJacksonModules()
          {
            return ImmutableList.of();
          }
        });

    injector = Guice.createInjector(modules);
  }

  
  @Test
  public void testComposingFilter()
  {
    ComposingServerFilterStrategy composingServerFilter = injector.getInstance(ComposingServerFilterStrategy.class);
    Set<QueryableDruidServer> servers = LookupAwareFilterStrategyTest.createServerTopology();
    Set<QueryableDruidServer> filteredServers = composingServerFilter.filter(LookupAwareFilterStrategyTest.constructVirtualColumnQuery("lookup0"), servers);
    Assert.assertEquals(2, filteredServers.size());
  }
  
  @Test
  public void testBinding()
  {
    ServerSelectorStrategy selector = injector.getInstance(ServerSelectorStrategy.class);
    ServerFilterStrategyConfig filterConfig = injector.getInstance(ServerFilterStrategyConfig.class);
    Assert.assertEquals(filterConfig.getFilters().size(), 2);
  }
  
  @Test
  public void testNoOp()
  {
    NoOpServerFilterStrategy strategy = new NoOpServerFilterStrategy();
    Set<QueryableDruidServer> servers = new HashSet<QueryableDruidServer>();
    Set<QueryableDruidServer> filteredServers = strategy.filter(null, servers);
    Assert.assertEquals(servers, servers);
  }

  @Test
  public void testSubTypeRegistration()
  {
    checkSubtypeRegistered(ServerSelectorStrategy.class, FILTERING_SELECTOR_TYPE);
  }

  private void checkSubtypeRegistered(Class clazz, String subType)
  {
    MapperConfig config = mapper.getDeserializationConfig();
    AnnotatedClass annotatedClass = AnnotatedClassResolver.resolveWithoutSuperTypes(config, clazz);
    List<String> subtypes = mapper.getSubtypeResolver()
        .collectAndResolveSubtypesByClass(config, annotatedClass)
        .stream()
        .map(NamedType::getName)
        .collect(Collectors.toList());
    Assert.assertNotNull(subtypes);
    Assert.assertTrue(subtypes.contains(subType));
  }
}
