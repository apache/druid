package org.apache.druid.indexing.overlord;

import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RemoteTaskRunnerFactoryTest
{
  @Test
  public void testBuildWithAutoScale()
  {
    ProvisioningSchedulerConfig provisioningSchedulerConfig = Mockito.mock(ProvisioningSchedulerConfig.class);
    Mockito.when(provisioningSchedulerConfig.isDoAutoscale()).thenReturn(true);

    RemoteTaskRunnerFactory remoteTaskRunnerFactory = getTestRemoteTaskRunnerFactory(provisioningSchedulerConfig);

    Assert.assertNull(remoteTaskRunnerFactory.build().getProvisioningStrategy());
  }

  @Test
  public void testBuildWithoutAutoScale()
  {
    ProvisioningSchedulerConfig provisioningSchedulerConfig = Mockito.mock(ProvisioningSchedulerConfig.class);
    Mockito.when(provisioningSchedulerConfig.isDoAutoscale()).thenReturn(false);

    RemoteTaskRunnerFactory remoteTaskRunnerFactory = getTestRemoteTaskRunnerFactory(provisioningSchedulerConfig);

    Assert.assertTrue(remoteTaskRunnerFactory.build().getProvisioningStrategy() instanceof NoopProvisioningStrategy);
  }

  private RemoteTaskRunnerFactory getTestRemoteTaskRunnerFactory(ProvisioningSchedulerConfig provisioningSchedulerConfig)
  {
    CuratorFramework curator = Mockito.mock(CuratorFramework.class);
    Mockito.when(curator.newWatcherRemoveCuratorFramework()).thenReturn(null);
    return new RemoteTaskRunnerFactory(
        curator,
        new RemoteTaskRunnerConfig(),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null),
        null,
        null,
        null,
        provisioningSchedulerConfig,
        null,
        null
    );
  }
}
