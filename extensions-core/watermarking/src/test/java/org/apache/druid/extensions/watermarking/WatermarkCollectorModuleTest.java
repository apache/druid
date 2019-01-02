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

package org.apache.druid.extensions.watermarking;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.cache.WatermarkCache;
import org.apache.druid.extensions.watermarking.storage.composite.CompositeWatermarkSink;
import org.apache.druid.extensions.watermarking.storage.google.DatastoreWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.google.PubsubWatermarkSink;
import org.apache.druid.extensions.watermarking.storage.sql.MySqlWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.sql.SqlWatermarkStoreConfig;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermarkFactory;
import org.apache.druid.extensions.watermarking.watermarks.MintimeWatermarkFactory;
import org.apache.druid.guice.DruidProcessingModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Properties;

public class WatermarkCollectorModuleTest extends WatermarkingModuleTestBase
{
  @Test
  public void defaultStore()
  {
    Properties props = getBaseProperties();
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    // todo: explode or default to memory store without config?
    assertMemorySink(sink);
    assertMemorySource(source);
  }

  @Test
  public void customCursors()
  {
    Properties props = getBaseProperties();
    props.setProperty(
        "druid.watermarking.collector.cursors",
        StringUtils.format(
            "[\"%s\", \"%s\"]",
            MintimeWatermarkFactory.class.getName(),
            DataCompletenessLowWatermarkFactory.class.getName()
        )
    );

    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);
    WatermarkCollectorConfig moduleConfig = injector.getInstance(WatermarkCollectorConfig.class);

    // todo: explode or default to memory store without config?
    assertMemorySink(sink);
    assertMemorySource(source);

    Assert.assertTrue("Expected only 2 cursors", moduleConfig.getCursors().size() == 2);
  }

  @Test
  public void memoryStore()
  {
    Properties props = getBaseProperties();
    setMemorySink(props);
    setMemorySource(props);
    setMemoryStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertMemorySink(sink);
    assertMemorySource(source);
  }

  @Test
  public void mysqlStore()
  {

    Properties props = getBaseProperties();
    setMysqlSink(props);
    setMysqlSource(props);
    setMysqlStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertMysqlSink(sink);
    assertMysqlSource(source);
    assertMysqlConfig((SqlWatermarkStoreConfig) ((MySqlWatermarkStore) sink).getConfig());
    assertMysqlConfig((SqlWatermarkStoreConfig) ((MySqlWatermarkStore) source).getConfig());
  }

  @Ignore
  @Test
  public void datastoreStore()
  {
    Properties props = getBaseProperties();
    setDatastoreSink(props);
    setDatastoreSource(props);
    setDatastoreStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertDatastoreSink(sink);
    assertDatastoreSource(source);
    assertDatastoreConfig(((DatastoreWatermarkStore) sink).getConfig());
    assertDatastoreConfig(((DatastoreWatermarkStore) source).getConfig());
  }

  @Ignore
  @Test
  public void pubsubSinkDatastoreSourceTest()
  {
    Properties props = getBaseProperties();
    setPubsubSink(props);
    setDatastoreSource(props);
    setPubsubStore(props);
    setDatastoreStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertPubsubSink(sink);
    assertDatastoreSource(source);
    assertPubsubConfig(((PubsubWatermarkSink) sink).getConfig());
    assertDatastoreConfig(((DatastoreWatermarkStore) source).getConfig());
  }

  @Ignore
  @Test
  public void compositeSinkTest()
  {
    Properties props = getBaseProperties();
    setCompositePubsubSink(props);
    setMemorySource(props);
    setCompositePubsubStore(props);
    setMemoryStore(props);

    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertCompositeSink(sink);
    assertMemorySource(source);
    assertCompositePubsubConfig((CompositeWatermarkSink) sink);
  }

  @Test
  public void testCache()
  {
    Properties props = getBaseProperties();
    setCache(props);

    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    WatermarkCache cache = injector.getInstance(WatermarkCache.class);

    assertMemorySink(sink);
    assertMemorySource(source);

    assertMemorySource(cache.getSource());

    Assert.assertTrue("The cache should be enabled", cache.getConfig().getEnable());
    Assert.assertEquals(
        "The config value expireMinutes should be correct",
        1, cache.getConfig().getExpireMinutes()
    );
    Assert.assertEquals(
        "The config value maxSize should be correct",
        10_000, cache.getConfig().getMaxSize()
    );
  }

  @Test
  public void testCacheConfig()
  {
    Properties props = getBaseProperties();
    setCache(props);
    setCacheOptionals(props);

    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSink sink = injector.getInstance(WatermarkSink.class);
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    WatermarkCache cache = injector.getInstance(WatermarkCache.class);

    assertMemorySink(sink);
    assertMemorySource(source);

    assertMemorySource(cache.getSource());

    Assert.assertTrue("The cache should be enabled", cache.getConfig().getEnable());
    Assert.assertEquals(
        "The config value expireMinutes should be correct",
        10, cache.getConfig().getExpireMinutes()
    );
    Assert.assertEquals(
        "The config value maxSize should be correct",
        1_000, cache.getConfig().getMaxSize()
    );
  }

  @Override
  public java.lang.Class getType()
  {
    return WatermarkCollector.class;
  }

  @Override
  public Injector newInjector(final Properties props)
  {
    return Initialization.makeInjectorWithModules(
        Guice.createInjector(
            Modules.override(GuiceInjectors.makeDefaultStartupModules())
                   .with((Module) binder -> binder.bind(Properties.class).toInstance(props))
        ),
        ImmutableList.of(
            new DruidProcessingModule(),
            new QueryableModule(),
            new QueryRunnerFactoryModule(),
            new WatermarkCollectorModule()
        )
    );
  }
}
