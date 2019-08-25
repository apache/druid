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

package org.apache.druid.curator;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.LifecycleModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

/**
 */
public final class CuratorModuleTest
{

  private static final String CURATOR_HOST_KEY = CuratorModule.CURATOR_CONFIG_PREFIX + ".host";

  private static final String EXHIBITOR_HOSTS_KEY = CuratorModule.EXHIBITOR_CONFIG_PREFIX + ".hosts";

  @Test
  public void defaultEnsembleProvider()
  {
    Injector injector = newInjector(new Properties());
    injector.getInstance(CuratorFramework.class); // initialize related components
    EnsembleProvider ensembleProvider = injector.getInstance(EnsembleProvider.class);
    Assert.assertTrue(
        "EnsembleProvider should be FixedEnsembleProvider",
        ensembleProvider instanceof FixedEnsembleProvider
    );
    Assert.assertEquals(
        "The connectionString should be 'localhost'",
        "localhost", ensembleProvider.getConnectionString()
    );
  }

  @Test
  public void fixedZkHosts()
  {
    Properties props = new Properties();
    props.put(CURATOR_HOST_KEY, "hostA");
    Injector injector = newInjector(props);

    injector.getInstance(CuratorFramework.class); // initialize related components
    EnsembleProvider ensembleProvider = injector.getInstance(EnsembleProvider.class);
    Assert.assertTrue(
        "EnsembleProvider should be FixedEnsembleProvider",
        ensembleProvider instanceof FixedEnsembleProvider
    );
    Assert.assertEquals(
        "The connectionString should be 'hostA'",
        "hostA", ensembleProvider.getConnectionString()
    );
  }

  @Test
  public void exhibitorEnsembleProvider()
  {
    Properties props = new Properties();
    props.put(CURATOR_HOST_KEY, "hostA");
    props.put(EXHIBITOR_HOSTS_KEY, "[\"hostB\"]");
    Injector injector = newInjector(props);

    injector.getInstance(CuratorFramework.class); // initialize related components
    EnsembleProvider ensembleProvider = injector.getInstance(EnsembleProvider.class);
    Assert.assertTrue(
        "EnsembleProvider should be ExhibitorEnsembleProvider",
        ensembleProvider instanceof ExhibitorEnsembleProvider
    );
  }

  @Test
  public void emptyExhibitorHosts()
  {
    Properties props = new Properties();
    props.put(CURATOR_HOST_KEY, "hostB");
    props.put(EXHIBITOR_HOSTS_KEY, "[]");
    Injector injector = newInjector(props);

    injector.getInstance(CuratorFramework.class); // initialize related components
    EnsembleProvider ensembleProvider = injector.getInstance(EnsembleProvider.class);
    Assert.assertTrue(
        "EnsembleProvider should be FixedEnsembleProvider",
        ensembleProvider instanceof FixedEnsembleProvider
    );
    Assert.assertEquals(
        "The connectionString should be 'hostB'",
        "hostB", ensembleProvider.getConnectionString()
    );
  }

  private Injector newInjector(final Properties props)
  {
    List<Module> modules = ImmutableList.<Module>builder()
        .addAll(GuiceInjectors.makeDefaultStartupModules())
        .add(new LifecycleModule()).add(new CuratorModule()).build();
    return Guice.createInjector(
        Modules.override(modules).with(new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(Properties.class).toInstance(props);
          }
        })
    );
  }

}
