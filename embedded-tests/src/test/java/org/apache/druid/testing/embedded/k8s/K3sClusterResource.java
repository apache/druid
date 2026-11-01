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

package org.apache.druid.testing.embedded.k8s;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestFolder;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.druid.testing.embedded.docker.DruidContainerResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.utils.ITRetryUtil;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link TestcontainerResource} to run a K3s cluster which can launch Druid pods.
 */
public class K3sClusterResource extends TestcontainerResource<K3sContainer>
{
  private static final Logger log = new Logger(K3sClusterResource.class);

  private static final String K3S_IMAGE_NAME = "rancher/k3s:v1.35.0-k3s1";

  public static final String DRUID_NAMESPACE = "druid";
  private static final String NAMESPACE_MANIFEST = "manifests/druid-namespace.yaml";
  private static final String DEFAULT_SERVICE_MANIFEST = "manifests/druid-service.yaml";

  private static final String COMMON_CONFIG_MAP = "druid-common-props";
  private static final String SERVICE_CONFIG_MAP = "druid-%s-props";

  public static final long POD_READY_TIMEOUT_SECONDS = 300;

  private final List<File> manifestFiles = new ArrayList<>();
  private final List<K3sDruidService> services = new ArrayList<>();

  private EmbeddedDruidCluster cluster;
  private KubernetesClient client;
  private String druidImageName;
  private String druidManifestTemplate = DEFAULT_SERVICE_MANIFEST;

  private final Closer closer = Closer.create();

  public K3sClusterResource()
  {
    // Add the namespace manifest
    addManifestResource(NAMESPACE_MANIFEST);
  }

  public K3sClusterResource addManifestResource(String manifestResourceName)
  {
    manifestFiles.add(Resources.getFileForResource(manifestResourceName));
    return this;
  }

  public K3sClusterResource addService(K3sDruidService service)
  {
    services.add(service);
    return this;
  }

  public K3sClusterResource usingDruidImage(String druidImageName)
  {
    this.druidImageName = druidImageName;
    return this;
  }

  /**
   * Uses the Docker test image specified by the system property
   * {@link DruidContainerResource#PROPERTY_TEST_IMAGE} for the Druid pods.
   */
  public K3sClusterResource usingDruidTestImage()
  {
    return usingDruidImage(DruidContainerResource.getTestDruidImageName());
  }

  /**
   * Uses the given resource template to create manifests for Druid services.
   */
  public K3sClusterResource usingDruidManifestTemplate(String resourceName)
  {
    this.druidManifestTemplate = resourceName;
    return this;
  }

  @Override
  protected K3sContainer createContainer()
  {
    Objects.requireNonNull(druidImageName, "No Druid image specified");
    final K3sContainer container = new K3sContainer(DockerImageName.parse(K3S_IMAGE_NAME));

    final List<String> portBindings = new ArrayList<>();
    for (K3sDruidService service : services) {
      for (int port : service.getCommand().getExposedPorts()) {
        container.addExposedPorts(port);

        // Bind the ports statically (rather than using a mapped port) so that this
        // Druid service is discoverable with the Druid service discovery
        portBindings.add(port + ":" + port);
      }
      int servicePort = service.getServicePort();
      container.addExposedPorts(servicePort);
      portBindings.add(servicePort + ":" + servicePort);
    }

    container.setPortBindings(portBindings);
    return container;
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
    initKubernetesClient();
    loadLocalDockerImageIntoContainer(druidImageName, cluster.getTestFolder());
    manifestFiles.forEach(this::applyManifest);
    initializeDruidServices(cluster);
    waitUntilPodsAreReady(DRUID_NAMESPACE);
    waitUntilServicesAreHealthy();
  }

  protected void initializeDruidServices(EmbeddedDruidCluster cluster)
  {
    createConfigMapForCommonProperties(cluster);
    for (K3sDruidService druidService : services) {
      final String serviceConfigMap = StringUtils.format(SERVICE_CONFIG_MAP, druidService.getName());
      applyConfigMap(
          newConfigMap(serviceConfigMap, druidService.getProperties(), "runtime.properties")
      );
      applyManifestYaml(druidService, createManifestYaml(druidService));
    }
  }

  protected void waitUntilServicesAreHealthy()
  {
    try {
      services.forEach(this::waitUntilServiceIsHealthy);
    }
    catch (Exception e) {
      dumpPodLogs(DRUID_NAMESPACE);
      throw e;
    }
  }

  protected List<K3sDruidService> getServices()
  {
    return services;
  }

  private void createConfigMapForCommonProperties(EmbeddedDruidCluster cluster)
  {
    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    commonProperties.remove("druid.extensions.modulesForEmbeddedTests");
    applyConfigMap(
        newConfigMap(COMMON_CONFIG_MAP, commonProperties, "common.runtime.properties")
    );
  }

  private void initKubernetesClient()
  {
    client = new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(getContainer().getKubeConfigYaml()))
        .build();
    closer.register(client);
  }

  protected void waitUntilPodsAreReady(String namespace)
  {
    if (DRUID_NAMESPACE.equals(namespace) && !services.isEmpty()) {
      try {
        ITRetryUtil.retryUntil(
            () -> client.pods().inNamespace(namespace).list().getItems().size() >= services.size(),
            true,
            1_000L,
            60,
            StringUtils.format("All [%d] service pods created in namespace [%s]", services.size(), namespace)
        );
      }
      catch (Exception e) {
        log.warn(e, "Did not observe [%d] pods in namespace [%s]", services.size(), namespace);
      }
    }
    client.pods().inNamespace(namespace).resources().forEach(this::waitUntilPodIsReady);
  }

  /**
   * Exposes the fabric8 {@link KubernetesClient} for tests that need to interact
   * with the cluster directly (e.g. discover task-launched peon pods).
   */
  public KubernetesClient getKubernetesClient()
  {
    return client;
  }

  @Override
  public void stop()
  {
    try {
      closer.close();
    }
    catch (Exception e) {
      log.error(e, "Could not close resources");
    }
    super.stop();
  }

  /**
   * Loads the given Docker image from the host Docker to the container. If the
   * image does not exist in the host Docker, the image will be pulled by the
   * K3s container itself.
   */
  private void loadLocalDockerImageIntoContainer(String localImageName, TestFolder testFolder)
  {
    ensureRunning();

    final File tempDir = testFolder.getOrCreateFolder("druid-k3s-image");
    final File tarFile = new File(tempDir, "druid-image.tar");

    final DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();

    try (
        final ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient
            .Builder()
            .dockerHost(config.getDockerHost())
            .build();
        final DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient);
        final FileOutputStream tarOutputStream = new FileOutputStream(tarFile);
        final InputStream imageInputStream = dockerClient.saveImageCmd(localImageName).exec();
    ) {
      if (doesImageExistInHostDocker(localImageName, dockerClient)) {
        log.info("Transfering image[%s] from host Docker to K3s container.", localImageName);
      } else {
        log.info("Image[%s] will be pulled by K3s container as it does not exist host Docker.", localImageName);
        return;
      }

      imageInputStream.transferTo(tarOutputStream);
      log.info("Saved Docker image[%s] to tar[%s].", localImageName, tarFile);

      final String imagePathInContainer = "/tmp/druid-image.tar";
      getContainer().copyFileToContainer(
          MountableFile.forHostPath(tarFile.getAbsolutePath()),
          imagePathInContainer
      );

      getContainer().execInContainer("ctr", "-n", "k8s.io", "images", "import", imagePathInContainer);
      log.info("Image[%s] loaded into K3s containerd", localImageName);

      getContainer().execInContainer("rm", imagePathInContainer);
      FileUtils.deleteDirectory(tempDir);
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to load local Docker image[%s]", localImageName);
    }
  }

  private boolean doesImageExistInHostDocker(String localImageName, DockerClient dockerClient)
  {
    try {
      dockerClient.inspectImageCmd(localImageName).exec();
      return true;
    }
    catch (NotFoundException e) {
      return false;
    }
  }

  private void applyConfigMap(ConfigMap configMap)
  {
    client.configMaps()
          .inNamespace(DRUID_NAMESPACE)
          .resource(configMap)
          .serverSideApply();
  }

  /**
   * Creates the manifest YAML for the given Druid service.
   */
  protected String createManifestYaml(K3sDruidService service)
  {
    return service.createManifestYaml(druidManifestTemplate, druidImageName);
  }

  /**
   * Applies the given service manifest YAML to the K3s cluster.
   */
  protected void applyManifestYaml(K3sDruidService service, String manifestYaml)
  {
    log.info("Applying manifest for service[%s]: %s", service.getName(), manifestYaml);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(manifestYaml.getBytes(StandardCharsets.UTF_8))) {
      client.load(bis).inNamespace(DRUID_NAMESPACE).serverSideApply();
      log.info("Applied manifest for service[%s]", service.getName());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Applies a YAML manifest file to the K3s container.
   */
  private void applyManifest(File manifest)
  {
    try (FileInputStream fis = new FileInputStream(manifest)) {
      client.load(fis).inNamespace(DRUID_NAMESPACE).serverSideApply();
      log.info("Applied manifest file[%s]", manifest);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits for the given pod to be ready.
   */
  private void waitUntilPodIsReady(PodResource pod)
  {
    try {
      pod.waitUntilCondition(
          p -> p.getStatus() != null &&
               p.getStatus().getConditions() != null &&
               p.getStatus().getConditions().stream().anyMatch(
                   c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus())
               ),
          POD_READY_TIMEOUT_SECONDS,
          TimeUnit.SECONDS
      );
    }
    catch (KubernetesClientTimeoutException e) {
      dumpPodLogs(DRUID_NAMESPACE);
      final String diagnostics = buildPodDiagnosticsOnTimeout(pod);
      throw new ISE("%s", diagnostics);
    }
  }

  private String buildPodDiagnosticsOnTimeout(final PodResource podResource)
  {
    Pod pod = null;
    try {
      pod = podResource.get();
    }
    catch (Exception e) {
      log.warn(e, "Could not get pod resource");
    }

    final String podName = pod != null && pod.getMetadata() != null
                           ? pod.getMetadata().getName()
                           : null;
    final String namespace = pod != null && pod.getMetadata() != null
                             ? pod.getMetadata().getNamespace()
                             : DRUID_NAMESPACE;

    List<Event> events = null;
    if (podName != null && client != null) {
      try {
        final ObjectReference objectRef = new ObjectReferenceBuilder()
            .withApiVersion("v1")
            .withKind("Pod")
            .withName(podName)
            .withNamespace(namespace)
            .build();
        events = client.v1()
                       .events()
                       .inNamespace(namespace)
                       .withInvolvedObject(objectRef)
                       .list()
                       .getItems();
      }
      catch (Exception e) {
        log.warn(e, "Could not fetch events for pod [%s]", podName);
      }
    }

    String logTail = null;
    try {
      logTail = podResource.tailingLines(100).getLog();
    }
    catch (Exception e) {
      log.warn(e, "Could not fetch log tail for pod [%s]", podName);
    }

    return formatPodDiagnostics(pod, events, logTail);
  }

  public static String formatPodDiagnostics(
      @Nullable final Pod pod,
      @Nullable final List<Event> events,
      @Nullable final String logTail
  )
  {
    final StringBuilder diagnostics = new StringBuilder();
    if (pod == null || pod.getMetadata() == null) {
      diagnostics.append("Timed out waiting for pod to be ready: Pod metadata is unavailable (pod may have been deleted).");
      if (events != null && !events.isEmpty()) {
        appendEvents(diagnostics, events);
      }
      return diagnostics.toString();
    }

    final String podName = pod.getMetadata().getName();
    final String namespace = pod.getMetadata().getNamespace();

    diagnostics.append(StringUtils.format(
        "Timed out waiting for pod [%s] in namespace [%s] to be ready.",
        podName,
        namespace
    ));

    final PodStatus status = pod.getStatus();
    if (status != null) {
      diagnostics.append(StringUtils.format("\nPhase: [%s]", status.getPhase()));
      if (status.getReason() != null || status.getMessage() != null) {
        diagnostics.append(StringUtils.format(
            ", Reason: [%s], Message: [%s]",
            status.getReason(),
            status.getMessage()
        ));
      }

      final List<PodCondition> conditions = status.getConditions();
      if (conditions != null && !conditions.isEmpty()) {
        diagnostics.append("\nConditions:");
        for (final PodCondition condition : conditions) {
          diagnostics.append(StringUtils.format(
              "\n  - Type: [%s], Status: [%s], Reason: [%s], Message: [%s]",
              condition.getType(),
              condition.getStatus(),
              condition.getReason(),
              condition.getMessage()
          ));
        }
      }

      final List<ContainerStatus> containerStatuses = status.getContainerStatuses();
      if (containerStatuses != null && !containerStatuses.isEmpty()) {
        diagnostics.append("\nContainer Statuses:");
        for (final ContainerStatus containerStatus : containerStatuses) {
          diagnostics.append(StringUtils.format(
              "\n  - Container: [%s], Ready: [%s], RestartCount: [%d]",
              containerStatus.getName(),
              containerStatus.getReady(),
              containerStatus.getRestartCount()
          ));
          final ContainerState state = containerStatus.getState();
          if (state != null) {
            if (state.getWaiting() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Waiting(reason=[%s], message=[%s])",
                  state.getWaiting().getReason(),
                  state.getWaiting().getMessage()
              ));
            } else if (state.getTerminated() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Terminated(exitCode=[%d], reason=[%s], message=[%s])",
                  state.getTerminated().getExitCode(),
                  state.getTerminated().getReason(),
                  state.getTerminated().getMessage()
              ));
            } else if (state.getRunning() != null) {
              diagnostics.append(StringUtils.format(
                  ", State: Running(startedAt=[%s])",
                  state.getRunning().getStartedAt()
              ));
            }
          }
          final ContainerState lastState = containerStatus.getLastState();
          if (lastState != null && lastState.getTerminated() != null) {
            diagnostics.append(StringUtils.format(
                ", LastState: Terminated(exitCode=[%d], reason=[%s], message=[%s])",
                lastState.getTerminated().getExitCode(),
                lastState.getTerminated().getReason(),
                lastState.getTerminated().getMessage()
            ));
          }
        }
      }
    }

    if (events != null && !events.isEmpty()) {
      appendEvents(diagnostics, events);
    }

    if (logTail != null && !logTail.isBlank()) {
      diagnostics.append("\nLog Tail (last lines):\n").append(logTail);
    }

    return diagnostics.toString();
  }

  private static void appendEvents(final StringBuilder diagnostics, final List<Event> events)
  {
    diagnostics.append("\nEvents:");
    for (final Event event : events) {
      diagnostics.append(StringUtils.format(
          "\n  - [%s] %s: %s (count: [%d], lastObserved: [%s])",
          event.getType(),
          event.getReason(),
          event.getMessage(),
          event.getCount() != null ? event.getCount() : 1,
          event.getLastTimestamp()
      ));
    }
  }

  public void dumpPodLogs(final String namespace)
  {
    if (client == null) {
      return;
    }
    final String testClassName = cluster != null && cluster.getTestClassName() != null
                                 ? cluster.getTestClassName()
                                 : "k3s";
    final File logDir = new File(new File("druid-container-logs", testClassName), "k3s-pods");
    try {
      FileUtils.mkdirp(logDir);
    }
    catch (Exception e) {
      log.warn(e, "Failed to create pod log directory [%s]", logDir.getAbsolutePath());
    }

    try {
      for (final Pod pod : client.pods().inNamespace(namespace).list().getItems()) {
        if (pod.getMetadata() == null || pod.getMetadata().getName() == null) {
          continue;
        }
        final String podName = pod.getMetadata().getName();
        final File podLogFile = new File(logDir, podName + ".log");
        try {
          final String podLogs = client.pods().inNamespace(namespace).withName(podName).getLog();
          if (podLogs != null) {
            Files.writeString(podLogFile.toPath(), podLogs, StandardCharsets.UTF_8);
            log.info("Dumped pod [%s] logs to [%s]", podName, podLogFile.getAbsolutePath());
          }
        }
        catch (Exception e) {
          log.warn(e, "Could not dump logs for pod [%s]", podName);
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Could not list pods in namespace [%s] for dumping logs", namespace);
    }
  }

  /**
   * Polls the health check endpoint of the given service until it is healthy.
   */
  private void waitUntilServiceIsHealthy(K3sDruidService service)
  {
    final URL url;
    try {
      url = new URL(service.getHealthCheckUrl());
    }
    catch (Exception e) {
      throw new ISE(e, "Could not construct URL for service[%s]", service.getName());
    }

    ITRetryUtil.retryUntilEquals(
        () -> {
          byte[] resp = url
              .openConnection()
              .getInputStream()
              .readAllBytes();
          String body = new String(resp, StandardCharsets.UTF_8);
          return body.contains("true");
        },
        true,
        1_000L,
        100,
        StringUtils.format("Service[%s] is healthy", service.getName())
    );
  }

  /**
   * Creates a new {@link ConfigMap} that can be applied to the K3s cluster.
   */
  private static ConfigMap newConfigMap(String name, Properties properties, String fileName)
  {
    try {
      // Serialize the properties
      StringWriter writer = new StringWriter();
      properties.store(writer, null);

      final ConfigMap configMap = new ConfigMap();
      ObjectMeta meta = new ObjectMeta();
      meta.setName(name);
      meta.setNamespace(DRUID_NAMESPACE);
      configMap.setMetadata(meta);
      configMap.setData(Map.of(fileName, writer.toString()));

      final String configMapYaml = Serialization.asYaml(configMap);
      log.info("Created config map[%s]: %s", name, configMapYaml);

      return configMap;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not write config map[%s]", name);
    }
  }
}
