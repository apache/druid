package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.ObjectUtils;

import javax.validation.constraints.NotNull;
import java.util.List;

public class KubernetesAndWorkerTaskRunnerConfig {

  // Select which worker task runner to use in addition to the Kubernetes runner, options are httpRemote or remote.
  @JsonProperty
  @NotNull
  private String workerTaskRunnerType = "httpRemote";

  @JsonProperty
  @NotNull
  private List<String> workerTaskRunnerTaskTypes = ImmutableList.of();

  @JsonProperty
  @NotNull
  private List<String> workerTaskRunnerDataSources = ImmutableList.of();

  @JsonProperty
  @NotNull
  private Boolean sendAllTasksToWorkerTaskRunner = Boolean.FALSE;

  public KubernetesAndWorkerTaskRunnerConfig()
  {
  }

  private KubernetesAndWorkerTaskRunnerConfig(
      String workerTaskRunnerType,
      List<String> workerTaskRunnerTaskTypes,
      List<String> workerTaskRunnerDataSources,
      Boolean sendAllTasksToWorkerTaskRunner
  )
  {
    this.workerTaskRunnerType = ObjectUtils.defaultIfNull(
        workerTaskRunnerType,
        this.workerTaskRunnerType
    );
    this.workerTaskRunnerTaskTypes = ObjectUtils.defaultIfNull(
        workerTaskRunnerTaskTypes,
        this.workerTaskRunnerTaskTypes
    );
    this.workerTaskRunnerDataSources = ObjectUtils.defaultIfNull(
        workerTaskRunnerDataSources,
        this.workerTaskRunnerDataSources
    );
    this.sendAllTasksToWorkerTaskRunner = ObjectUtils.defaultIfNull(
        sendAllTasksToWorkerTaskRunner,
        false
    );
  }

  public String getWorkerTaskRunnerType()
  {
    return workerTaskRunnerType;
  }

  public List<String> getWorkerTaskRunnerTaskTypes()
  {
    return workerTaskRunnerTaskTypes;
  }

  public List<String> getWorkerTaskRunnerDataSources()
  {
    return workerTaskRunnerDataSources;
  }
  public Boolean isSendAllTasksToWorkerTaskRunner()
  {
    return sendAllTasksToWorkerTaskRunner;
  }

}
