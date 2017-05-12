/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.curator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

/**
 */
public class CuratorModule implements Module
{
  static final String CURATOR_CONFIG_PREFIX = "druid.zk.service";

  static final String EXHIBITOR_CONFIG_PREFIX = "druid.exhibitor.service";

  private static final int BASE_SLEEP_TIME_MS = 1000;

  private static final int MAX_SLEEP_TIME_MS = 45000;

  private static final int MAX_RETRIES = 30;

  private static final Logger log = new Logger(CuratorModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, CURATOR_CONFIG_PREFIX, CuratorConfig.class);
    JsonConfigProvider.bind(binder, EXHIBITOR_CONFIG_PREFIX, ExhibitorConfig.class);
  }

  @Provides
  @LazySingleton
  public CuratorFramework makeCurator(
      CuratorConfig config, EnsembleProvider ensembleProvider, Lifecycle lifecycle
  ) throws IOException
  {
    final CuratorFramework framework =
        CuratorFrameworkFactory.builder()
                               .ensembleProvider(ensembleProvider)
                               .sessionTimeoutMs(config.getZkSessionTimeoutMs())
                               .retryPolicy(new BoundedExponentialBackoffRetry(
                                   BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES))
                               .compressionProvider(new PotentiallyGzippedCompressionProvider(config.getEnableCompression()))
                               .aclProvider(config.getEnableAcl() ? new SecuredACLProvider() : new DefaultACLProvider())
                               .build();

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            log.info("Starting Curator");
            framework.start();
          }

          @Override
          public void stop()
          {
            log.info("Stopping Curator");
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

    return new ExhibitorEnsembleProvider(
        new Exhibitors(
            exConfig.getHosts(),
            exConfig.getRestPort(),
            newBackupProvider(config.getZkHosts())
        ),
        new DefaultExhibitorRestClient(exConfig.getUseSsl()),
        exConfig.getRestUriPath(),
        exConfig.getPollingMs(),
        new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES)
    )
    {
      @Override
      public void start() throws Exception
      {
        log.info("Poll the list of zookeeper servers for initial ensemble");
        this.pollForInitialEnsemble();
        super.start();
      }
    };
  }

  private Exhibitors.BackupConnectionStringProvider newBackupProvider(final String zkHosts)
  {
    return new Exhibitors.BackupConnectionStringProvider()
    {
      @Override
      public String getBackupConnectionString() throws Exception
      {
        return zkHosts;
      }
    };
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
}
