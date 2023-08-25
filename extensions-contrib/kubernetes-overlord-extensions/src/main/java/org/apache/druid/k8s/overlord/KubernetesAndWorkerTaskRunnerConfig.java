package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
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
  private Boolean sendAllTasksToWorkerTaskRunner = Boolean.FALSE;

  @JsonCreator
  public KubernetesAndWorkerTaskRunnerConfig(
      @JsonProperty("workerTaskRunnerType") String workerTaskRunnerType,
      @JsonProperty("sendAllTasksToWorkerTaskRunner") Boolean sendAllTasksToWorkerTaskRunner
  )
  {
    this.workerTaskRunnerType = ObjectUtils.defaultIfNull(
        workerTaskRunnerType,
        this.workerTaskRunnerType
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

  public Boolean isSendAllTasksToWorkerTaskRunner()
  {
    return sendAllTasksToWorkerTaskRunner;
  }

}
