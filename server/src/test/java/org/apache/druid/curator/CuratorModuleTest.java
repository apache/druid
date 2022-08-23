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

import com.google.inject.Injector;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.util.List;
import java.util.Properties;

public final class CuratorModuleTest
{
  private static final String CURATOR_CONNECTION_TIMEOUT_MS_KEY =
      CuratorConfig.CONFIG_PREFIX + "." + CuratorConfig.CONNECTION_TIMEOUT_MS;

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Rule
  public final LoggerCaptureRule logger = new LoggerCaptureRule(CuratorModule.class);

  @Test
  public void createsCuratorFrameworkAsConfigured()
  {
    CuratorConfig config = CuratorConfig.create("myhost1:2888,myhost2:2888");
    CuratorFramework curatorFramework = CuratorModule.createCurator(config);
    CuratorZookeeperClient client = curatorFramework.getZookeeperClient();

    Assert.assertEquals(config.getZkHosts(), client.getCurrentConnectionString());
    Assert.assertEquals(config.getZkConnectionTimeoutMs(), client.getConnectionTimeoutMs());

    MatcherAssert.assertThat(client.getRetryPolicy(), Matchers.instanceOf(BoundedExponentialBackoffRetry.class));
    BoundedExponentialBackoffRetry retryPolicy = (BoundedExponentialBackoffRetry) client.getRetryPolicy();
    Assert.assertEquals(CuratorModule.BASE_SLEEP_TIME_MS, retryPolicy.getBaseSleepTimeMs());
    Assert.assertEquals(CuratorModule.MAX_SLEEP_TIME_MS, retryPolicy.getMaxSleepTimeMs());
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
    String deprecatedPropName = CuratorConfig.CONFIG_PREFIX + ".terminateDruidProcessOnConnectFail";
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
    return new StartupInjectorBuilder()
        .add(
            new LifecycleModule(),
            new CuratorModule(),
            binder -> binder.bind(Properties.class).toInstance(props)
         )
        .build();
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
