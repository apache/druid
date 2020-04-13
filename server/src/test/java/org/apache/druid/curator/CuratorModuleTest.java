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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.util.List;
import java.util.Properties;

public final class CuratorModuleTest
{
  private static final String CURATOR_HOST_KEY = CuratorModule.CURATOR_CONFIG_PREFIX + "." + CuratorConfig.HOST;
  private static final String CURATOR_CONNECTION_TIMEOUT_MS_KEY =
      CuratorModule.CURATOR_CONFIG_PREFIX + "." + CuratorConfig.CONNECTION_TIMEOUT_MS;
  private static final String EXHIBITOR_HOSTS_KEY = CuratorModule.EXHIBITOR_CONFIG_PREFIX + ".hosts";

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Rule
  public final LoggerCaptureRule logger = new LoggerCaptureRule(CuratorModule.class);

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
    props.setProperty(CURATOR_HOST_KEY, "hostA");
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
    props.setProperty(CURATOR_HOST_KEY, "hostA");
    props.setProperty(EXHIBITOR_HOSTS_KEY, "[\"hostB\"]");
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
    props.setProperty(CURATOR_HOST_KEY, "hostB");
    props.setProperty(EXHIBITOR_HOSTS_KEY, "[]");
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

  @Test
  public void exitsJvmWhenMaxRetriesExceeded() throws Exception
  {
    Properties props = new Properties();
    props.setProperty(CURATOR_CONNECTION_TIMEOUT_MS_KEY, "0");
    Injector injector = newInjector(props);
    CuratorFramework curatorFramework = createCuratorFramework(injector, 0);
    curatorFramework.start();

    exit.expectSystemExitWithStatus(1);
    logger.clearLogEvents();

    // This will result in a curator unhandled error since the connection timeout is 0 and retries are disabled
    curatorFramework.create().inBackground().forPath("/foo");

    // org.apache.curator.framework.impl.CuratorFrameworkImpl logs "Background retry gave up" unhandled error twice
    List<LogEvent> loggingEvents = logger.getLogEvents();
    Assert.assertTrue(
        "Logging events: " + loggingEvents,
        loggingEvents.stream()
                     .anyMatch(l ->
                                   l.getLevel().equals(Level.ERROR)
                                   && l.getMessage()
                                       .getFormattedMessage()
                                       .equals("Unhandled error in Curator, stopping server.")
                     )
    );
  }

  @Ignore("Verifies changes in https://github.com/apache/druid/pull/8458, but overkill for regular testing")
  @Test
  public void ignoresDeprecatedCuratorConfigProperties()
  {
    Properties props = new Properties();
    String deprecatedPropName = CuratorModule.CURATOR_CONFIG_PREFIX + ".terminateDruidProcessOnConnectFail";
    props.setProperty(deprecatedPropName, "true");
    Injector injector = newInjector(props);

    try {
      injector.getInstance(CuratorFramework.class);
    }
    catch (Exception e) {
      Assert.fail("Deprecated curator config was not ignored:\n" + e);
    }
  }

  private Injector newInjector(final Properties props)
  {
    List<Module> modules = ImmutableList.<Module>builder()
        .addAll(GuiceInjectors.makeDefaultStartupModules())
        .add(new LifecycleModule())
        .add(new CuratorModule())
        .build();
    return Guice.createInjector(
        Modules.override(modules).with(binder -> binder.bind(Properties.class).toInstance(props))
    );
  }

  private static CuratorFramework createCuratorFramework(Injector injector, int maxRetries)
  {
    CuratorFramework curatorFramework = injector.getInstance(CuratorFramework.class);
    RetryPolicy retryPolicy = curatorFramework.getZookeeperClient().getRetryPolicy();
    Assert.assertThat(retryPolicy, CoreMatchers.instanceOf(ExponentialBackoffRetry.class));
    RetryPolicy adjustedRetryPolicy = adjustRetryPolicy((BoundedExponentialBackoffRetry) retryPolicy, maxRetries);
    curatorFramework.getZookeeperClient().setRetryPolicy(adjustedRetryPolicy);
    return curatorFramework;
  }

  private static RetryPolicy adjustRetryPolicy(BoundedExponentialBackoffRetry origRetryPolicy, int maxRetries)
  {
    return new BoundedExponentialBackoffRetry(
        origRetryPolicy.getBaseSleepTimeMs(),
        origRetryPolicy.getMaxSleepTimeMs(),
        maxRetries
    );
  }
}
