package simulate;

import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A K3s container for use in embedded tests.
 */
public class KubernetesResource extends TestcontainerResource<K3sContainer>
{

  private static final String K3S_IMAGE = "rancher/k3s:v1.21.3-k3s1";

  private EmbeddedDruidCluster cluster;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }


  @Override
  protected K3sContainer createContainer()
  {
    return new K3sContainer(DockerImageName.parse(K3S_IMAGE));
  }
}
