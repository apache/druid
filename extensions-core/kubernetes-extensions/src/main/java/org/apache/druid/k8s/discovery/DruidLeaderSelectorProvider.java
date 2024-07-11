package org.apache.druid.k8s.discovery;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.kubernetes.client.openapi.ApiClient;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;

abstract public class DruidLeaderSelectorProvider implements Provider<DruidLeaderSelector>
{
  @Inject
  @Self
  private DruidNode druidNode;

  @Inject
  private PodInfo podInfo;

  @Inject
  private K8sDiscoveryConfig discoveryConfig;

  @Inject
  private Provider<ApiClient> k8sApiClientProvider;

  private boolean isCoordinator;

  DruidLeaderSelectorProvider(boolean isCoordinator)
  {
    this.isCoordinator = isCoordinator;
  }

  @Override
  public DruidLeaderSelector get()
  {
    // Note: these can not be setup in the constructor because injected K8sDiscoveryConfig and PodInfo
    // are not available at that time.
    String lockResourceName;
    String lockResourceNamespace;

    if (isCoordinator) {
      lockResourceName = discoveryConfig.getClusterIdentifier() + "-leaderelection-coordinator";
      lockResourceNamespace = discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace() == null ?
                              podInfo.getPodNamespace() : discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace();
    } else {
      lockResourceName = discoveryConfig.getClusterIdentifier() + "-leaderelection-overlord";
      lockResourceNamespace = discoveryConfig.getOverlordLeaderElectionConfigMapNamespace() == null ?
                              podInfo.getPodNamespace() : discoveryConfig.getOverlordLeaderElectionConfigMapNamespace();
    }

    return new K8sDruidLeaderSelector(
        druidNode,
        lockResourceName,
        lockResourceNamespace,
        discoveryConfig,
        new DefaultK8sLeaderElectorFactory(k8sApiClientProvider.get(), discoveryConfig)
    );
  }

  static class CoordinatorDruidLeaderSelectorProvider extends DruidLeaderSelectorProvider {
    @Inject
    public CoordinatorDruidLeaderSelectorProvider() {
      super(true);
    }
  }

  static class IndexingServiceDruidLeaderSelectorProvider extends DruidLeaderSelectorProvider {
    @Inject
    public IndexingServiceDruidLeaderSelectorProvider() {
      super(false);
    }
  }
}