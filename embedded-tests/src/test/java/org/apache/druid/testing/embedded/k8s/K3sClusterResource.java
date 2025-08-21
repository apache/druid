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
import io.fabric8.kubernetes.api.model.ObjectMeta;
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
import org.apache.druid.testing.tools.ITRetryUtil;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * {@link TestcontainerResource} to run a K3s cluster which can launch Druid pods.
 */
public class K3sClusterResource extends TestcontainerResource<K3sContainer>
{
  private static final Logger log = new Logger(K3sClusterResource.class);

  private static final String K3S_IMAGE_NAME = "rancher/k3s:v1.28.8-k3s1";

  public static final String DRUID_NAMESPACE = "druid";
  private static final String NAMESPACE_MANIFEST = "manifests/druid-namespace.yaml";

  private static final String COMMON_CONFIG_MAP = "druid-common-props";
  private static final String SERVICE_CONFIG_MAP = "druid-%s-props";

  public static final long POD_READY_TIMEOUT_SECONDS = 300;

  private final List<File> manifestFiles = new ArrayList<>();
  private final List<K3sDruidService> services = new ArrayList<>();

  private KubernetesClient client;
  private String druidImageName;

  private final Closer closer = Closer.create();

  public K3sClusterResource()
  {
    // Add the namespace manifest
    manifestFiles.add(Resources.getFileForResource(NAMESPACE_MANIFEST));
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
  public K3sClusterResource usingTestImage()
  {
    return usingDruidImage(DruidContainerResource.getTestDruidImageName());
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
    }

    container.setPortBindings(portBindings);
    return container;
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    client = new KubernetesClientBuilder()
        .withConfig(Config.fromKubeconfig(getContainer().getKubeConfigYaml()))
        .build();
    closer.register(client);

    loadLocalDockerImageIntoContainer(druidImageName, cluster.getTestFolder());

    manifestFiles.forEach(this::applyManifest);

    // Create common config map
    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    commonProperties.remove("druid.extensions.modulesForEmbeddedTests");
    applyConfigMap(
        newConfigMap(COMMON_CONFIG_MAP, commonProperties, "common.runtime.properties")
    );

    // Create config maps and manifests for each service
    for (K3sDruidService druidService : services) {
      final String serviceConfigMap = StringUtils.format(SERVICE_CONFIG_MAP, druidService.getName());
      applyConfigMap(
          newConfigMap(serviceConfigMap, druidService.getProperties(), "runtime.properties")
      );
      applyManifest(druidService);
    }

    // Wait for all pods to be ready and services to be healthy
    client.pods().inNamespace(DRUID_NAMESPACE).resources().forEach(this::waitUntilPodIsReady);
    services.forEach(this::waitUntilServiceIsHealthy);
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
      log.info("Docker image[%s] saved to tar[%s].", localImageName, tarFile);

      getContainer().copyFileToContainer(
          MountableFile.forHostPath(tarFile.getAbsolutePath()),
          "/tmp/druid-image.tar"
      );

      getContainer().execInContainer("ctr", "-n", "k8s.io", "images", "import", "/tmp/druid-image.tar");
      log.info("Image[%s] loaded into K3s containerd", localImageName);

      getContainer().execInContainer("rm", "/tmp/druid-image.tar");

      FileUtils.deleteDirectory(tempDir);
    }
    catch (Exception e) {
      throw new ISE("Failed to load local Docker image[%s]" + localImageName, e);
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
   * Creates and applies the manifest for the given Druid service.
   */
  private void applyManifest(K3sDruidService service)
  {
    final String manifestYaml = service.createManifestYaml(druidImageName);
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
      throw new ISE("Timed out waiting for pod[%s] to be ready", pod.get().getMetadata().getName());
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
