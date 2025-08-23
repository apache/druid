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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.docker.DruidContainerResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class K3sClusterWithOperatorResource extends K3sClusterResource
{
  private static final Logger log = new Logger(K3sClusterWithOperatorResource.class);
  private static final String RBAC_MANIFEST = "manifests/druid-operator-rbac.yaml";
  private static final String OPERATOR_NAMESPACE_MANIFEST = "manifests/druid-operator-namespace.yaml";
  private static final String OPERATOR_NAMESPACE = "druid-operator-system";
  private static final String HELM_RELEASE_NAME = "druid-operator";
  private static final String HELM_REPO_NAME = "datainfra";
  private static final String HELM_REPO_URL = "https://charts.datainfra.io";
  private static final String HELM_CHART_NAME = "datainfra/druid-operator";
  private static final String HELM_VERSION = "v3.13.1";
  private static final String HELM_PLATFORM = "linux-amd64";
  private static final String HELM_MOUNT_PATH = "/usr/local/bin/helm";


  public K3sClusterWithOperatorResource()
  {
    super();
    manifestFiles.add(Resources.getFileForResource(RBAC_MANIFEST));
    manifestFiles.add(Resources.getFileForResource(OPERATOR_NAMESPACE_MANIFEST));
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    super.loadImageAndApplyClusterManifests(cluster);
    installHelm(cluster);
    setupOperatorWithHelm();
    super.waitUntilPodsAreReady(OPERATOR_NAMESPACE);
    Properties clusterCommon = super.injectClusterCommonPropertiesToConfigMap(cluster);
    Properties yamlCommonProperties = getCombinedCommonProperties(clusterCommon);
    initializeDruidServices(yamlCommonProperties);
  }

  private void initializeDruidServices(Properties properties)
  {
    for (K3sDruidService druidService : getServices()) {
      applyManifest(druidService.withCommonProperties(properties));
    }

    waitUntilPodsAreReady(DRUID_NAMESPACE);
    super.waitUntilServicesAreHealthy();
  }

 private Properties getCombinedCommonProperties(Properties properties)
  {
    Properties defaults = new Properties();
    defaults.setProperty("druid.metadata.storage.connector.createTables", "true");
    defaults.putAll(properties);
    return defaults;
  }

  /**
   * Installs Helm binary in the K3s cluster.
   */
  private void installHelm(EmbeddedDruidCluster cluster)
  {
    try {
      File helmBinary = downloadHelmBinary(cluster);
      this.getContainer().copyFileToContainer(
          MountableFile.forHostPath(helmBinary.getAbsolutePath()),
          HELM_MOUNT_PATH
      );
      this.getContainer().execInContainer("chmod", "+x", HELM_MOUNT_PATH);
      log.info("Helm binary installed to /usr/local/bin/helm");
    }
    catch (Exception e) {
      log.error(e, "Failed to download or install Helm binary");
      throw new RuntimeException("Helm installation failed", e);
    }
  }

  private File downloadHelmBinary(EmbeddedDruidCluster cluster) throws Exception
  {
    String helmUrl = StringUtils.format(
        "https://get.helm.sh/helm-%s-%s.tar.gz",
        HELM_VERSION,
        HELM_PLATFORM
    );
    log.debug("Downloading Helm from: %s", helmUrl);

    File helmFolder = cluster.getTestFolder().getOrCreateFolder("helm");
    File tarFile = new File(helmFolder, "helm.tar.gz");
    File helmBinary = new File(helmFolder, "helm");

    if (helmBinary.exists() && helmBinary.canExecute()) {
      log.debug("Helm binary already exists: %s", helmBinary.getAbsolutePath());
      return helmBinary;
    }

    HttpClient client = HttpClient.newBuilder()
                                  .connectTimeout(Duration.ofSeconds(30))
                                  .build();

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(URI.create(helmUrl))
                                     .timeout(Duration.ofSeconds(120))
                                     .build();

    HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());

    if (response.statusCode() != 200) {
      throw new RuntimeException("Failed to download Helm. Status: " + response.statusCode());
    }

    try (InputStream inputStream = response.body();
         FileOutputStream outputStream = new FileOutputStream(tarFile)) {
      inputStream.transferTo(outputStream);
    }

    extractTarGz(tarFile, helmFolder, HELM_PLATFORM + "/helm", "helm");

    Set<PosixFilePermission> permissions = Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE,
        PosixFilePermission.GROUP_READ,
        PosixFilePermission.GROUP_EXECUTE,
        PosixFilePermission.OTHERS_READ,
        PosixFilePermission.OTHERS_EXECUTE
    );

    try {
      Files.setPosixFilePermissions(helmBinary.toPath(), permissions);
    }
    catch (IOException e) {
      helmBinary.setExecutable(true);
    }

    tarFile.delete();
    log.info("Helm binary downloaded and extracted to: %s", helmBinary.getAbsolutePath());
    return helmBinary;
  }

  /**
   * Extract a specific file from a tar.gz archive.
   */
  private void extractTarGz(File tarGzFile, File destFolder, String sourceEntryPath, String destFileName)
      throws IOException
  {
    try (FileInputStream fis = new FileInputStream(tarGzFile);
         BufferedInputStream bis = new BufferedInputStream(fis);
         GZIPInputStream gis = new GZIPInputStream(bis);
         TarArchiveInputStream tais = new TarArchiveInputStream(gis)) {

      TarArchiveEntry entry;
      while ((entry = tais.getNextTarEntry()) != null) {
        if (entry.getName().equals(sourceEntryPath)) {
          File destFile = new File(destFolder, destFileName);
          try (FileOutputStream fos = new FileOutputStream(destFile)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = tais.read(buffer)) != -1) {
              fos.write(buffer, 0, len);
            }
          }
          break;
        }
      }
    }
  }

  /**
   * Updates helm repository and installs the druid operator chart in the K3s cluster.
   */
  private void setupOperatorWithHelm()
  {
    try {
      executeHelmCommand("repo", "add", HELM_REPO_NAME, HELM_REPO_URL);
      executeHelmCommand("repo", "update");
      executeHelmCommand(
          "install",
          HELM_RELEASE_NAME,
          HELM_CHART_NAME,
          "--namespace", OPERATOR_NAMESPACE,
          "--create-namespace",
          "--set", "env.WATCH_NAMESPACE=" + DRUID_NAMESPACE,
          "--wait",
          "--timeout", "3m"
      );
    }
    catch (Exception e) {
      log.error("Failed to set up Druid Operator with Helm: %s", e.getMessage());
      throw new RuntimeException("Failed to execute helm command", e);
    }
  }

  /**
   * Executes a Helm command in the K3s cluster container.
   */
  private void executeHelmCommand(String... args) throws Exception
  {
    String[] fullCommand = new String[args.length + 3];
    fullCommand[0] = "sh";
    fullCommand[1] = "-c";
    fullCommand[2] = "export KUBECONFIG=/etc/rancher/k3s/k3s.yaml && helm " + String.join(" ", args);

    try {
      Container.ExecResult result = getContainer().execInContainer(fullCommand);

      if (result.getExitCode() != 0) {
        log.error("Helm command failed with exit code: %d", result.getExitCode());
        if (!result.getStderr().trim().isEmpty()) {
          log.error("Error: %s", result.getStderr().trim());
        }
        throw new RuntimeException("Helm command failed: " + String.join(" ", args));
      }
    }
    catch (Exception e) {
      log.error("Exception executing helm command: %s", e.getMessage());
      throw e;
    }
  }

  @Override
  public void applyManifest(K3sDruidService service)
  {
    String manifestYaml = service.createManifestYaml(druidImageName);
    manifestYaml = StringUtils.replace(
        manifestYaml,
        "${commonRuntimeProperties}",
        buildPropertiesString(service.getCommonProperties(), 4)
    );
    manifestYaml = StringUtils.replace(
        manifestYaml,
        "${nodeRuntimeProperties}",
        buildPropertiesString(service.getRuntimeProperties(), 8)
    );
    super.loadYamlInCluster(service, manifestYaml);
  }

  /**
   * Builds a properties string to be used in the manifest.yaml file supporting a uniform indentation.
   */
  private String buildPropertiesString(Properties properties, int indentationSpaces)
  {
    StringBuilder builder = new StringBuilder();
    String indentation = " ".repeat(indentationSpaces);
    for (String key : properties.stringPropertyNames()) {
      builder.append(indentation).append(key).append("=").append(properties.getProperty(key)).append("\n");
    }
    return builder.toString();
  }
}
