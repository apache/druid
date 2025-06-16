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

package org.apache.druid.testing.simulate.embedded;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliIndexer;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.utils.RuntimeInfo;
import org.jetbrains.annotations.Nullable;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Embeddded mode of {@link CliIndexer} used in simulation tests.
 */
public class EmbeddedIndexer extends EmbeddedDruidServer
{
  private static final Map<String, String> DEFAULT_PROPERTIES = Map.of(
      // Don't sync lookups as cluster might not have a Coordinator
      "druid.lookup.enableLookupSyncOnStartup", "false"
  );

  private final Map<String, String> overrideProperties;

  public static EmbeddedIndexer create()
  {
    return new EmbeddedIndexer(Map.of());
  }

  public static EmbeddedIndexer withProps(
      Map<String, String> overrideProps
  )
  {
    return new EmbeddedIndexer(overrideProps);
  }

  private EmbeddedIndexer(Map<String, String> overrideProperties)
  {
    this.overrideProperties = overrideProperties;
  }

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Indexer(handler);
  }

  @Override
  RuntimeInfo getRuntimeInfo()
  {
    final long mem100Mb = 100_000_000;
    return new DruidProcessingConfigTest.MockRuntimeInfo(4, mem100Mb, mem100Mb);
  }

  @Override
  Properties buildStartupProperties(
      TemporaryFolder tempDir,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  ) throws IOException
  {
    final Properties properties = super.buildStartupProperties(tempDir, zk, dbRule);
    properties.putAll(DEFAULT_PROPERTIES);
    properties.putAll(overrideProperties);
    return properties;
  }

  /**
   * Extends {@link CliIndexer} to allow handling the lifecycle.
   */
  private static class Indexer extends CliIndexer
  {
    private final LifecycleInitHandler handler;

    private Indexer(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(handler.getInitModules());
      modules.addAll(super.getModules());
      return modules;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }
  }
}
