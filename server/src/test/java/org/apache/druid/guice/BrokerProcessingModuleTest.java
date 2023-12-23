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

package org.apache.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.config.Config;
import org.apache.druid.query.BrokerParallelMergeConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.LegacyBrokerParallelMergeConfig;
import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;


@RunWith(MockitoJUnitRunner.class)
public class BrokerProcessingModuleTest
{
  private Injector injector;
  private BrokerProcessingModule target;
  @Mock
  private CacheConfig cacheConfig;
  @Mock
  private CachePopulatorStats cachePopulatorStats;

  @Before
  public void setUp()
  {
    target = new BrokerProcessingModule();
    injector = makeInjector(new Properties());
  }

  @Test
  public void testIntermediateResultsPool()
  {
    DruidProcessingConfig druidProcessingConfig = injector.getInstance(DruidProcessingConfig.class);
    target.getIntermediateResultsPool(druidProcessingConfig);
  }


  @Test
  public void testMergeBufferPool()
  {
    DruidProcessingConfig druidProcessingConfig = injector.getInstance(DruidProcessingConfig.class);
    target.getMergeBufferPool(druidProcessingConfig);
  }

  @Test
  public void testMergeProcessingPool()
  {
    BrokerParallelMergeConfig config = injector.getInstance(BrokerParallelMergeConfig.class);
    BrokerProcessingModule module = new BrokerProcessingModule();
    module.getMergeProcessingPoolProvider(config);
  }

  @Test
  public void testMergeProcessingPoolLegacyConfigs()
  {
    Properties props = new Properties();
    props.put("druid.processing.merge.pool.parallelism", "10");
    props.put("druid.processing.merge.pool.defaultMaxQueryParallelism", "10");
    props.put("druid.processing.merge.task.targetRunTimeMillis", "1000");
    Injector gadget = makeInjector(props);
    BrokerParallelMergeConfig config = gadget.getInstance(BrokerParallelMergeConfig.class);
    Assert.assertEquals(10, config.getParallelism());
    Assert.assertEquals(10, config.getDefaultMaxQueryParallelism());
    Assert.assertEquals(1000, config.getTargetRunTimeMillis());
  }

  @Test
  public void testCachePopulatorAsSingleton()
  {
    CachePopulator cachePopulator = injector.getInstance(CachePopulator.class);
    Assert.assertNotNull(cachePopulator);
  }

  @Test(expected = ProvisionException.class)
  public void testMemoryCheckThrowsException()
  {
    // JDK 9 and above do not support checking for direct memory size
    // so this test only validates functionality for Java 8.
    try {
      JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
    }
    catch (UnsupportedOperationException e) {
      Assume.assumeNoException(e);
    }
    Properties props = new Properties();
    props.setProperty("druid.processing.buffer.sizeBytes", "3GiB");
    Injector injector1 = makeInjector(props);

    DruidProcessingConfig processingBufferConfig = injector1.getInstance(DruidProcessingConfig.class);
    BrokerProcessingModule module = new BrokerProcessingModule();
    module.getMergeBufferPool(processingBufferConfig);
  }

  private Injector makeInjector(Properties props)
  {

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            Modules.override(
                (binder) -> {
                  binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                  binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                  binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
                  binder.bind(Properties.class).toInstance(props);
                  ConfigurationObjectFactory factory = Config.createFactory(props);
                  LegacyBrokerParallelMergeConfig legacyConfig = factory.build(LegacyBrokerParallelMergeConfig.class);
                  binder.bind(ConfigurationObjectFactory.class).toInstance(factory);
                  binder.bind(LegacyBrokerParallelMergeConfig.class).toInstance(legacyConfig);
                },
                target
            ).with((binder) -> {
              binder.bind(CachePopulatorStats.class).toInstance(cachePopulatorStats);
              binder.bind(CacheConfig.class).toInstance(cacheConfig);
            })
        )
    );
    return injector;
  }
}

