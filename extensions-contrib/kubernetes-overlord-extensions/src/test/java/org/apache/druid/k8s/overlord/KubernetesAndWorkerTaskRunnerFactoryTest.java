package org.apache.druid.k8s.overlord;

import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(EasyMockRunner.class)
public class KubernetesAndWorkerTaskRunnerFactoryTest extends EasyMockSupport {

  @Mock KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory;
  @Mock HttpRemoteTaskRunnerFactory httpRemoteTaskRunnerFactory;
  @Mock RemoteTaskRunnerFactory remoteTaskRunnerFactory;

  @Test
  public void test_useHttpTaskRunner_asDefault()
  {
    KubernetesAndWorkerTaskRunnerFactory factory = new KubernetesAndWorkerTaskRunnerFactory(
      kubernetesTaskRunnerFactory, httpRemoteTaskRunnerFactory, remoteTaskRunnerFactory, new KubernetesAndWorkerTaskRunnerConfig(null, null)
    );

    EasyMock.expect(httpRemoteTaskRunnerFactory.build()).andReturn(null);
    EasyMock.expect(kubernetesTaskRunnerFactory.build()).andReturn(null);

    replayAll();
    factory.build();
    verifyAll();
  }

  @Test
  public void test_specifyRemoteTaskRunner()
  {
    KubernetesAndWorkerTaskRunnerFactory factory = new KubernetesAndWorkerTaskRunnerFactory(
        kubernetesTaskRunnerFactory, httpRemoteTaskRunnerFactory, remoteTaskRunnerFactory, new KubernetesAndWorkerTaskRunnerConfig("remote", null)
    );

    EasyMock.expect(remoteTaskRunnerFactory.build()).andReturn(null);
    EasyMock.expect(kubernetesTaskRunnerFactory.build()).andReturn(null);

    replayAll();
    factory.build();
    verifyAll();
  }
}
