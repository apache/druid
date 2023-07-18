package org.apache.druid.k8s.overlord;

import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.k8s.overlord.KubernetesAndWorkerTaskRunner;

public class KubernetesAndWorkerTaskRunnerFactory implements TaskRunnerFactory<KubernetesAndWorkerTaskRunner> {
  public static final String TYPE_NAME = "k8sAndWorker";

  private final KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory;
  private final TaskRunnerFactory<WorkerTaskRunner> workerTaskRunnerFactory;

  private KubernetesAndWorkerTaskRunner runner;

  @Inject
  public KubernetesAndWorkerTaskRunnerFactory(
      KubernetesTaskRunnerFactory kubernetesTaskRunnerFactory,
      TaskRunnerFactory<WorkerTaskRunner> workerTaskRunnerFactory
  )
  {
   this.kubernetesTaskRunnerFactory = kubernetesTaskRunnerFactory;
   this.workerTaskRunnerFactory = workerTaskRunnerFactory;
  }
  @Override
  public KubernetesAndWorkerTaskRunner build() {
    runner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunnerFactory.build(),
        workerTaskRunnerFactory.build(),
        false
    );
    return runner;
  }

  @Override
  public KubernetesAndWorkerTaskRunner get() {
    return runner;
  }
}
