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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.netty.util.SuppressForbidden;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.base.Strings;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CuratorModule implements Module
{
  static final String CURATOR_CONFIG_PREFIX = "druid.zk.service";

  static final String EXHIBITOR_CONFIG_PREFIX = "druid.exhibitor.service";

  private static final int BASE_SLEEP_TIME_MS = 1000;

  private static final int MAX_SLEEP_TIME_MS = 45000;

  private static final int MAX_RETRIES = 29;

  private static final Logger log = new Logger(CuratorModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, CURATOR_CONFIG_PREFIX, CuratorConfig.class);
    JsonConfigProvider.bind(binder, EXHIBITOR_CONFIG_PREFIX, ExhibitorConfig.class);
  }

  @Provides
  @LazySingleton
  @SuppressForbidden(reason = "System#err")
  public CuratorFramework makeCurator(CuratorConfig config, EnsembleProvider ensembleProvider, Lifecycle lifecycle)
  {
    final CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
    if (!Strings.isNullOrEmpty(config.getZkUser()) && !Strings.isNullOrEmpty(config.getZkPwd())) {
      builder.authorization(
          config.getAuthScheme(),
          StringUtils.format("%s:%s", config.getZkUser(), config.getZkPwd()).getBytes(StandardCharsets.UTF_8)
      );
    }

    RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES);

    final CuratorFramework framework = builder
        .ensembleProvider(ensembleProvider)
        .sessionTimeoutMs(config.getZkSessionTimeoutMs())
        .connectionTimeoutMs(config.getZkConnectionTimeoutMs())
        .retryPolicy(retryPolicy)
        .compressionProvider(new PotentiallyGzippedCompressionProvider(config.getEnableCompression()))
        .aclProvider(config.getEnableAcl() ? new SecuredACLProvider() : new DefaultACLProvider())
        .build();

    framework.getUnhandledErrorListenable().addListener((message, e) -> {
      log.error(e, "Unhandled error in Curator, stopping server.");
      shutdown(lifecycle);
    });

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {
            log.debug("Starting Curator");
            framework.start();
          }

          @Override
          public void stop()
          {
            log.debug("Stopping Curator");
            framework.close();
          }
        }
    );

    return framework;
  }

  @Provides
  @LazySingleton
  public EnsembleProvider makeEnsembleProvider(CuratorConfig config, ExhibitorConfig exConfig)
  {
    if (exConfig.getHosts().isEmpty()) {
      return new FixedEnsembleProvider(config.getZkHosts());
    }

    RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES);

    return new ExhibitorEnsembleProvider(
        new Exhibitors(
            exConfig.getHosts(),
            exConfig.getRestPort(),
            newBackupProvider(config.getZkHosts())
        ),
        new DefaultExhibitorRestClient(exConfig.getUseSsl()),
        exConfig.getRestUriPath(),
        exConfig.getPollingMs(),
        retryPolicy
    )
    {
      @Override
      public void start() throws Exception
      {
        log.debug("Polling the list of ZooKeeper servers for the initial ensemble");
        this.pollForInitialEnsemble();
        super.start();
      }
    };
  }

  private Exhibitors.BackupConnectionStringProvider newBackupProvider(final String zkHosts)
  {
    return () -> zkHosts;
  }

  static class SecuredACLProvider implements ACLProvider
  {
    @Override
    public List<ACL> getDefaultAcl()
    {
      return ZooDefs.Ids.CREATOR_ALL_ACL;
    }

    @Override
    public List<ACL> getAclForPath(String path)
    {
      return ZooDefs.Ids.CREATOR_ALL_ACL;
    }
  }

  private void shutdown(Lifecycle lifecycle)
  {
    //noinspection finally (not completing the 'finally' block normally is intentional)
    try {
      lifecycle.stop();
    }
    catch (Throwable t) {
      log.error(t, "Exception when stopping server after unhandled Curator error.");
    }
    finally {
      System.exit(1);
    }
  }
}
