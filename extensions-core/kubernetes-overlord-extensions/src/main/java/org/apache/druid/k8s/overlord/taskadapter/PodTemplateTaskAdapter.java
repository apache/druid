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

package org.apache.druid.k8s.overlord.taskadapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplate;
import io.fabric8.kubernetes.api.model.PodTemplateBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.common.Base64Compression;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesOverlordUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.TaskLogs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A PodTemplate {@link TaskAdapter} to transform tasks to kubernetes jobs and kubernetes pods to tasks
 *
 * Pod Templates
 * This TaskAdapter allows the user to provide a pod template per druid task.  If a pod template has
 * not been provided for a task, then the provided base template will be used.
 *
 * Providing Pod Templates per Task
 * Pod templates are provided as files, each pod template file path must be specified as a runtime property
 * druid.indexer.runner.k8s.podTemplate.{task_name}=/path/to/podTemplate.yaml.
 *
 * Note that the base pod template must be specified as the runtime property
 * druid.indexer.runner.k8s.podTemplate.base=/path/to/podTemplate.yaml
 */
public class PodTemplateTaskAdapter implements TaskAdapter
{
  public static final String TYPE = "customTemplateAdapter";
  private static final Logger log = new Logger(PodTemplateTaskAdapter.class);

  /**
   * Valid image tag, as defined by the OCI distribution spec.
   */
  private static final Pattern IMAGE_TAG_PATTERN = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}");

  private final KubernetesTaskRunnerConfig taskRunnerConfig;
  private final TaskConfig taskConfig;
  private final DruidNode node;
  private final ObjectMapper mapper;
  private final TaskLogs taskLogs;
  private final PodTemplateSelector podTemplateSelector;

  public PodTemplateTaskAdapter(
      KubernetesTaskRunnerConfig taskRunnerConfig,
      TaskConfig taskConfig,
      DruidNode node,
      ObjectMapper mapper,
      TaskLogs taskLogs,
      PodTemplateSelector podTemplateSelector
  )
  {
    this.taskRunnerConfig = taskRunnerConfig;
    this.taskConfig = taskConfig;
    this.node = node;
    this.mapper = mapper;
    this.taskLogs = taskLogs;
    this.podTemplateSelector = podTemplateSelector;
  }

  @Override
  public String getAdapterType()
  {
    return TYPE;
  }

  public PodTemplateSelector getPodTemplateSelector()
  {
    return podTemplateSelector;
  }

  /**
   * Create a {@link Job} from a {@link Task}
   *
   * 1. Select pod template based on task type, then apply the image tag from the task context
   *    (see {@link #applyImageTagOverride})
   * 2. Add labels and annotations to the pod template including the task as a compressed and base64 encoded string
   * 3. Add labels and annotations to the job
   * 4. Add user specified active deadline seconds and job ttl
   * 5. Set backoff limit to zero since druid does not support external systems retrying failed tasks
   *
   * @param task
   * @return {@link Job}
   * @throws IOException
   */
  @Override
  public Job fromTask(Task task) throws IOException
  {
    Optional<PodTemplateWithName> selectedPodTemplate = podTemplateSelector.getPodTemplateForTask(task);
    if (selectedPodTemplate == null || !selectedPodTemplate.isPresent()) {
      throw InternalServerError.exception(
          "Could not find pod template for task [%s]."
              + " Check the overlord logs for errors selecting the pod template",
          task.getId()
      );
    }
    final PodTemplateWithName podTemplateWithName = applyImageTagOverride(task, selectedPodTemplate.get());

    return new JobBuilder()
        .withNewMetadata()
        .withName(new K8sTaskId(taskRunnerConfig.getK8sTaskPodNamePrefix(), task).getK8sJobName())
        .addToLabels(getJobLabels(taskRunnerConfig, task))
        .addToAnnotations(getJobAnnotations(taskRunnerConfig, task))
        .addToAnnotations(DruidK8sConstants.TASK_JOB_TEMPLATE, podTemplateWithName.getName())
        .endMetadata()
        .withNewSpec()
        .withTemplate(podTemplateWithName.getPodTemplate().getTemplate())
        .editTemplate()
        .editOrNewMetadata()
        .addToAnnotations(getPodTemplateAnnotations(task))
        .addToAnnotations(DruidK8sConstants.TASK_JOB_TEMPLATE, podTemplateWithName.getName())
        .addToLabels(getPodLabels(taskRunnerConfig, task))
        .endMetadata()
        .editSpec()
        .editFirstContainer()
        .addAllToEnv(getEnv(task))
        .endContainer()
        .endSpec()
        .endTemplate()
        .withActiveDeadlineSeconds(taskRunnerConfig.getTaskTimeout().toStandardDuration().getStandardSeconds())
        .withBackoffLimit(0)  // druid does not support an external system retrying failed tasks
        .withTtlSecondsAfterFinished((int) taskRunnerConfig.getTaskCleanupDelay()
                                                           .toStandardDuration()
                                                           .getStandardSeconds())
        .endSpec()
        .build();
  }

  /**
   * Replaces the image tag of the first container with the value of the
   * {@link DruidK8sConstants#TASK_CONTEXT_MIN_POD_IMAGE_TAG} task context key, if the value is newer than the tag in
   * the pod template. {@link StringComparators#VERSION} compares the tags.
   * <p>
   * Returns {@code selected} if the key is absent or not applicable, new {@code PodTemplateWithName} otherwise.
   *
   * @throws DruidException if the value is not a valid image tag
   * @throws org.apache.druid.query.BadQueryContextException if the value is not a string
   */
  protected PodTemplateWithName applyImageTagOverride(final Task task, final PodTemplateWithName selected)
  {
    final Object contextValue = task.getContextValue(DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG);
    if (contextValue == null) {
      return selected;
    }
    if (!taskRunnerConfig.isAllowTaskImageTagOverride()) {
      log.warn(
          "Task[%s] sets context key[%s], but the image tag override is disabled. Ignoring the key.",
          task.getId(),
          DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG
      );
      return selected;
    }
    final String requestedTag = QueryContexts.getAsString(DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG, contextValue, null);
    if (!IMAGE_TAG_PATTERN.matcher(requestedTag).matches()) {
      throw InvalidInput.exception(
          "Task[%s] sets context key[%s] to value[%s], which is not a valid image tag. A valid tag matches[%s].",
          task.getId(),
          DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG,
          requestedTag,
          IMAGE_TAG_PATTERN.pattern()
      );
    }

    final List<Container> containers = selected.getPodTemplate().getTemplate().getSpec().getContainers();
    final String oldImage = containers.isEmpty() ? null : containers.get(0).getImage();
    final int tagSeparator = getTagSeparatorIndex(oldImage);
    if (tagSeparator < 0) {
      log.warn(
          "Task[%s] sets context key[%s], but image[%s] from pod template[%s] has a digest or no tag. Ignoring the key.",
          task.getId(),
          DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG,
          oldImage,
          selected.getName()
      );
      return selected;
    }

    final String oldTag = oldImage.substring(tagSeparator + 1);
    if (StringComparators.VERSION.compare(requestedTag, oldTag) <= 0) {
      log.info(
          "Task[%s] sets context key[%s] to tag[%s], which is not newer than tag[%s] of image[%s] from pod template[%s]."
          + " Using the image from the pod template.",
          task.getId(),
          DruidK8sConstants.TASK_CONTEXT_MIN_POD_IMAGE_TAG,
          requestedTag,
          oldTag,
          oldImage,
          selected.getName()
      );
      return selected;
    }

    final String newImage = oldImage.substring(0, tagSeparator + 1) + requestedTag;
    final PodTemplate podTemplate = new PodTemplateBuilder(selected.getPodTemplate())
        .editTemplate()
        .editSpec()
        .editFirstContainer()
        .withImage(newImage)
        .endContainer()
        .endSpec()
        .endTemplate()
        .build();

    log.info(
        "Task[%s] uses image[%s] instead of image[%s] from pod template[%s].",
        task.getId(),
        newImage,
        oldImage,
        selected.getName()
    );
    return new PodTemplateWithName(selected.getName(), podTemplate);
  }

  /**
   * Returns the index of the {@code :} that comes before the image tag, or -1 if the image is null, has a digest or
   * has no tag.
   * <p>
   * An image with a digest ({@code repository@sha256:...} or {@code repository:tag@sha256:...}) returns -1, because
   * the container runtime pulls the image by the digest and ignores the tag. Thus, a new tag has no effect. To use a
   * new tag, Druid must remove the digest, but the operator can set the digest to pin the image intentionally.
   */
  private static int getTagSeparatorIndex(@Nullable String image)
  {
    if (image == null || image.indexOf('@') >= 0) {
      return -1;
    }
    final int tagSeparator = image.lastIndexOf(':');
    if (tagSeparator <= image.lastIndexOf('/')) {
      return -1;
    }
    return tagSeparator;
  }

  /**
   * Transform a {@link Pod} to a {@link Task}
   *
   * 1. Find task annotation on the pod
   * 2. Base 64 decode and decompress task, read into {@link Task}
   *
   * @param from
   * @return {@link Task}
   * @throws IOException
   */
  @Override
  public Task toTask(Job from) throws IOException
  {
    Map<String, String> annotations = from.getSpec().getTemplate().getMetadata().getAnnotations();
    if (annotations == null) {
      log.info("No annotations found on pod spec for job [%s]. Trying to load task payload from deep storage.", from.getMetadata().getName());
      return toTaskUsingDeepStorage(from);
    }
    String task = annotations.get(DruidK8sConstants.TASK);
    if (task == null) {
      log.info("No task annotation found on pod spec for job [%s]. Trying to load task payload from deep storage.", from.getMetadata().getName());
      return toTaskUsingDeepStorage(from);
    }
    return mapper.readValue(Base64Compression.decompressBase64(task), Task.class);
  }

  private Task toTaskUsingDeepStorage(Job from) throws IOException
  {
    Optional<InputStream> taskBody = taskLogs.streamTaskPayload(getTaskId(from).getOriginalTaskId());
    if (!taskBody.isPresent()) {
      throw InternalServerError.exception(
          "Could not load task payload from deep storage for job [%s]."
          + " Check the overlord logs for errors uploading task payloads to deep storage.",
          from.getMetadata().getName()
      );
    }
    String task = IOUtils.toString(taskBody.get(), Charset.defaultCharset());
    return mapper.readValue(task, Task.class);
  }

  @Override
  public K8sTaskId getTaskId(Job from)
  {
    Map<String, String> annotations = from.getSpec().getTemplate().getMetadata().getAnnotations();
    if (annotations == null) {
      throw DruidException.defensive().build("No annotations found on pod spec for job [%s]", from.getMetadata().getName());
    }
    String taskId = annotations.get(DruidK8sConstants.TASK_ID);
    if (taskId == null) {
      throw DruidException.defensive().build("No task_id annotation found on pod spec for job [%s]", from.getMetadata().getName());
    }
    return new K8sTaskId(taskRunnerConfig.getK8sTaskPodNamePrefix(), taskId);
  }

  private Collection<EnvVar> getEnv(Task task) throws IOException
  {
    List<EnvVar> envVars = Lists.newArrayList(
        new EnvVarBuilder()
            .withName(DruidK8sConstants.TASK_DIR_ENV)
            .withValue(Paths.get(taskConfig.getBaseDir()).resolve(task.getId()).toString())
            .build(),
        new EnvVarBuilder()
            .withName("druid_indexer_task_baseTaskDir")
            .withValue(taskConfig.getBaseDir())
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.TASK_ID_ENV)
            .withValue(task.getId())
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.LOAD_BROADCAST_DATASOURCE_MODE_ENV)
            .withValue(task.getBroadcastDatasourceLoadingSpec().getMode().toString())
            .build(),
        new EnvVarBuilder()
            .withName(DruidK8sConstants.LOAD_BROADCAST_SEGMENTS_ENV)
            .withValue(Boolean.toString(task.getBroadcastDatasourceLoadingSpec().getMode().needsBroadcastSegments()))
            .build(),
        // Lets the peon tag its metrics with the template it runs under. Sourced from the annotation
        // rather than passed by value so it always reflects the template actually applied to the pod.
        new EnvVarBuilder()
            .withName(DruidK8sConstants.POD_TEMPLATE_ENV)
            .withValueFrom(new EnvVarSourceBuilder().withFieldRef(new ObjectFieldSelector(
                null,
                StringUtils.format("metadata.annotations['%s']", DruidK8sConstants.TASK_JOB_TEMPLATE)
            )).build()).build()
    );
    if (!shouldUseDeepStorageForTaskPayload(task)) {
      envVars.add(new EnvVarBuilder()
          .withName(DruidK8sConstants.TASK_JSON_ENV)
          .withValueFrom(new EnvVarSourceBuilder().withFieldRef(new ObjectFieldSelector(
              null,
              StringUtils.format("metadata.annotations['%s']", DruidK8sConstants.TASK)
          )).build()).build()
      );
    }
    return envVars;
  }

  private Map<String, String> getPodLabels(KubernetesTaskRunnerConfig config, Task task)
  {
    return getJobLabels(config, task);
  }

  private Map<String, String> getPodTemplateAnnotations(Task task) throws IOException
  {
    ImmutableMap.Builder<String, String> podTemplateAnnotationBuilder = ImmutableMap.<String, String>builder()
        .put(DruidK8sConstants.TLS_ENABLED, String.valueOf(node.isEnableTlsPort()))
        .put(DruidK8sConstants.TASK_ID, task.getId())
        .put(DruidK8sConstants.TASK_TYPE, task.getType())
        .put(DruidK8sConstants.TASK_GROUP_ID, task.getGroupId())
        .put(DruidK8sConstants.TASK_DATASOURCE, task.getDataSource());

    if (!shouldUseDeepStorageForTaskPayload(task)) {
      podTemplateAnnotationBuilder
          .put(DruidK8sConstants.TASK, Base64Compression.compressBase64(mapper.writeValueAsString(task)));
    }
    return podTemplateAnnotationBuilder.build();
  }

  private Map<String, String> getJobLabels(KubernetesTaskRunnerConfig config, Task task)
  {
    Preconditions.checkNotNull(config.getNamespace(), "When using Custom Pod Templates, druid.indexer.runner.namespace cannot be null.");
    String overlordNamespace = config.getOverlordNamespace().isEmpty() ? config.getNamespace() : config.getOverlordNamespace();

    return ImmutableMap.<String, String>builder()
        .putAll(config.getLabels())
        .put(DruidK8sConstants.LABEL_KEY, "true")
        .put(DruidK8sConstants.OVERLORD_NAMESPACE_KEY, overlordNamespace)
        .put(getDruidLabel(DruidK8sConstants.TASK_ID), KubernetesOverlordUtils.convertTaskIdToK8sLabel(task.getId()))
        .put(getDruidLabel(DruidK8sConstants.TASK_TYPE), KubernetesOverlordUtils.convertStringToK8sLabel(task.getType()))
        .put(getDruidLabel(DruidK8sConstants.TASK_GROUP_ID), KubernetesOverlordUtils.convertTaskIdToK8sLabel(task.getGroupId()))
        .put(getDruidLabel(DruidK8sConstants.TASK_DATASOURCE), KubernetesOverlordUtils.convertStringToK8sLabel(task.getDataSource()))
        .build();
  }

  private Map<String, String> getJobAnnotations(KubernetesTaskRunnerConfig config, Task task)
  {
    return ImmutableMap.<String, String>builder()
                       .putAll(config.getAnnotations())
                       .put(DruidK8sConstants.TASK_ID, task.getId())
                       .put(DruidK8sConstants.TASK_TYPE, task.getType())
                       .put(DruidK8sConstants.TASK_GROUP_ID, task.getGroupId())
                       .put(DruidK8sConstants.TASK_DATASOURCE, task.getDataSource())
                       .build();
  }

  private String getDruidLabel(String baseLabel)
  {
    return DruidK8sConstants.DRUID_LABEL_PREFIX + baseLabel;
  }

  @Override
  public boolean shouldUseDeepStorageForTaskPayload(Task task) throws IOException
  {
    String compressedTaskPayload = Base64Compression.compressBase64(mapper.writeValueAsString(task));
    return compressedTaskPayload.length() > DruidK8sConstants.MAX_ENV_VARIABLE_KBS;
  }
}
