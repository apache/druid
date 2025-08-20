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

package org.apache.druid.k8s.simulate;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestFolder;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.testcontainers.containers.BindMode.READ_WRITE;

/**
 * A K3s container for use in embedded tests.
 */
public class K3SResource extends TestcontainerResource<K3sContainer>
{
  private static final Logger log = new Logger(K3SResource.class);

  private static final String K3S_IMAGE = "rancher/k3s:v1.28.8-k3s1";
  private static final String HELM_VERSION = "v3.13.1";
  private static final String HELM_PLATFORM = "linux-amd64";
  
  private EmbeddedDruidCluster cluster;
  private KubernetesClient client;
  private TestFolder testFolder;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
    this.testFolder = new TestFolder();
    try {
      this.testFolder.start();
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to initialize TestFolder", e);
    }
  }


  @Override
  protected K3sContainer createContainer()
  {
    try {
      if (testFolder == null) {
        testFolder = new TestFolder();
        testFolder.start();
      }
      
      File helmBinary = downloadHelm();
      
      File storageDir = testFolder.getOrCreateFolder("druid-storage");
      testFolder.getOrCreateFolder("druid-storage/segments");
      testFolder.getOrCreateFolder("druid-storage/segment-cache");
      testFolder.getOrCreateFolder("druid-storage/metadata");
      testFolder.getOrCreateFolder("druid-storage/indexing-logs");
      
      Integer[] exposedPortRange = generatePortRange(30080, 30100);
      Integer[] allPorts = new Integer[exposedPortRange.length + 1];
      allPorts[0] = 6443;
      System.arraycopy(exposedPortRange, 0, allPorts, 1, exposedPortRange.length);
      
      K3sContainer container = new K3sContainer(DockerImageName.parse(K3S_IMAGE))
          .withExposedPorts(allPorts)
          .withCopyFileToContainer(
              MountableFile.forHostPath(helmBinary.getAbsolutePath()),
              "/usr/local/bin/helm"
          )
          .withFileSystemBind(
              storageDir.getAbsolutePath(),
              "/druid/data",
              READ_WRITE
          )
          .withCreateContainerCmdModifier(cmd -> {
            cmd.withPlatform("linux/amd64");
          });
      container.start();
      
      container.execInContainer("chmod", "+x", "/usr/local/bin/helm");
      
      try {
        container.execInContainer("helm", "version");
      }
      catch (IOException | InterruptedException e) {
        throw new RuntimeException("Helm verification failed", e);
      }

      this.client = new KubernetesClientBuilder()
          .withConfig(Config.fromKubeconfig(container.getKubeConfigYaml()))
          .build();
      return container;
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to create K3S container with Helm", e);
    }
  }

  @Override
  public void stop()
  {
    try {
      super.stop();
    }
    finally {
      if (testFolder != null) {
        try {
          testFolder.stop();
        }
        catch (Exception e) {
          log.error("Failed to cleanup TestFolder: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Download Helm binary and save to TestFolder.
   */
  private File downloadHelm() throws Exception
  {
    String helmUrl = StringUtils.format(
        "https://get.helm.sh/helm-%s-%s.tar.gz",
        HELM_VERSION,
        HELM_PLATFORM
    );
    log.debug("Downloading Helm from: %s", helmUrl);

    File helmFolder = testFolder.getOrCreateFolder("helm");
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
  private void extractTarGz(File tarGzFile, File destFolder, String sourceEntryPath, String destFileName) throws IOException
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
   * Generate array of ports from start to end (inclusive).
   */
  private Integer[] generatePortRange(int start, int end)
  {
    Integer[] ports = new Integer[end - start + 1];
    for (int i = 0; i < ports.length; i++) {
      ports[i] = start + i;
    }
    return ports;
  }

  public KubernetesClient getClient()
  {
    return client;
  }

  public K3sContainer getK3sContainer()
  {
    return getContainer();
  }

  /**
   * Load a local Docker image into the K3s container.
   * @param localImageName the name of the local Docker image (e.g., "my-druid:latest")
   */
  public void loadLocalImage(String localImageName)
  {
    try {
      Path tempDir = FileUtils.createTempDir("druid-k3s-image-").toPath();
      Path tarFile = tempDir.resolve("druid-image.tar");

      DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
      ApacheDockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
          .dockerHost(config.getDockerHost())
          .build();
      DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient);

      try {
        dockerClient.inspectImageCmd(localImageName).exec();
        log.debug("Docker image found: %s", localImageName);
      } catch (Exception e) {
        log.error("Docker image does not exist: %s", localImageName);
        throw new RuntimeException("Docker image does not exist: " + localImageName, e);
      }

      try (FileOutputStream fos = new FileOutputStream(tarFile.toFile());
           InputStream imageStream = dockerClient.saveImageCmd(localImageName).exec()) {
        imageStream.transferTo(fos);
        log.debug("Docker image saved to: %s", tarFile);
      }

      getContainer().copyFileToContainer(MountableFile.forHostPath(tarFile), "/tmp/druid-image.tar");
      log.debug("Tar file copied to K3s container");

      getContainer().execInContainer("ctr", "-n", "k8s.io", "images", "import", "/tmp/druid-image.tar");
      log.info("Image loaded into K3s containerd: %s", localImageName);

      getContainer().execInContainer("rm", "/tmp/druid-image.tar");
      Files.deleteIfExists(tarFile);
      Files.deleteIfExists(tempDir);
      
      dockerClient.close();
      httpClient.close();
    }
    catch (Exception e) {
      log.error(e, "Failed to load local Docker image: %s", localImageName);
      throw new RuntimeException("Failed to load local Docker image: " + localImageName, e);
    }
  }

  public TestFolder getTestFolder()
  {
    return testFolder;
  }
}
