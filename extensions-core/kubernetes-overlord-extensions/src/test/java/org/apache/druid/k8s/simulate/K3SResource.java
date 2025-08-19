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

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestFolder;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
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
import java.util.Locale;
import java.util.Set;

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
              testFolder.getOrCreateFolder("druid-storage").getAbsolutePath(),
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
    String helmUrl = String.format(
        Locale.ENGLISH,
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

    ProcessBuilder extractProcess = new ProcessBuilder(
        "tar", "-xzf", tarFile.getAbsolutePath(), 
        "-C", helmFolder.getAbsolutePath(),
        "--strip-components=1", 
        HELM_PLATFORM + "/helm"
    );
    
    Process extract = extractProcess.start();
    int exitCode = extract.waitFor();
    
    if (exitCode != 0) {
      String error = new String(extract.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException("Failed to extract Helm binary. Exit code: " + exitCode + ", Error: " + error);
    }
    
    // Make executable
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
      String containerId = getContainer().getContainerId();
      String tarPath = "/tmp/druid-image-" + System.currentTimeMillis() + ".tar";

      ProcessBuilder checkProcess = new ProcessBuilder("docker", "image", "inspect", localImageName);
      Process check = checkProcess.start();
      int checkExitCode = check.waitFor();
      
      if (checkExitCode != 0) {
        String checkError = new String(check.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        log.error("Docker image inspect failed with error: %s", checkError);
        throw new RuntimeException("Docker image does not exist: " + localImageName);
      }
      
      ProcessBuilder saveProcess = new ProcessBuilder("docker", "save", "-o", tarPath, localImageName);
      Process save = saveProcess.start();
      int saveExitCode = save.waitFor();
      
      if (saveExitCode != 0) {
        String saveError = new String(save.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        log.error("Docker save failed with error: %s", saveError);
        throw new RuntimeException("Failed to save Docker image: " + localImageName);
      }

      ProcessBuilder copyProcess = new ProcessBuilder("docker", "cp", tarPath, containerId + ":/tmp/druid-image.tar");
      Process copy = copyProcess.start();
      int copyExitCode = copy.waitFor();
      
      if (copyExitCode != 0) {
        String copyError = new String(copy.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        log.error("Docker cp failed with error: %s", copyError);
        throw new RuntimeException("Failed to copy tar file to K3s container");
      }

      ProcessBuilder loadProcess = new ProcessBuilder("docker", "exec", containerId, "ctr", "-n", "k8s.io", "images", "import", "/tmp/druid-image.tar");
      Process load = loadProcess.start();
      int loadExitCode = load.waitFor();
      
      String loadOutput = new String(load.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      String loadError = new String(load.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      
      if (!loadOutput.isEmpty()) {
        log.debug("Image Load output: %s", loadOutput);
      }
      if (!loadError.isEmpty()) {
        log.error("Image Load error: %s", loadError);
      }
      
      if (loadExitCode != 0) {
        throw new RuntimeException("Failed to load image into K3s containerd. Exit code: " + loadExitCode);
      }

      ProcessBuilder cleanupProcess = new ProcessBuilder("rm", tarPath);
      cleanupProcess.start().waitFor();
      ProcessBuilder cleanupK3sProcess = new ProcessBuilder("docker", "exec", containerId, "rm", "/tmp/druid-image.tar");
      cleanupK3sProcess.start().waitFor();
    }
    catch (Exception e) {
      log.error("Failed to load local Docker image: %s", localImageName);
      e.printStackTrace();
      throw new RuntimeException("Failed to load local Docker image: " + localImageName, e);
    }
  }

  public TestFolder getTestFolder()
  {
    return testFolder;
  }
}
