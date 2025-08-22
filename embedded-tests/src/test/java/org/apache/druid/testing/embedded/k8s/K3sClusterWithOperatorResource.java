package org.apache.druid.testing.embedded.k8s;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestFolder;
import org.apache.druid.testing.embedded.docker.DruidContainerResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.testcontainers.containers.Container;
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

  public static final String KEY_NODE = "node";
  public static final String KEY_DRUID_SERVICE = "druidServiceType";
  public static final String KEY_HEALTH_PATH = "healthPath";
  public static final String KEY_READINESS_PROBE_PATH = "readinessProbePath";
  public static final String KEY_SHARED_STORAGE_DIR = "sharedStorageDir";
  public static final String KEY_METADATA = "metadataName";



  public K3sClusterWithOperatorResource()
  {
    super();
    manifestFiles.add(Resources.getFileForResource(RBAC_MANIFEST));
    manifestFiles.add(Resources.getFileForResource(OPERATOR_NAMESPACE_MANIFEST));
  }

  public K3sClusterWithOperatorResource usingTestImage()
  {
    return usingDruidImage(DruidContainerResource.getTestDruidImageName());
  }

  public K3sClusterWithOperatorResource usingDruidImage(String druidImageName)
  {
    this.druidImageName = druidImageName;
    return this;
  }

  @Override
  public K3sClusterWithOperatorResource addService(K3sDruidService service)
  {
    services.add(service);
    return this;
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

    installHelm(cluster);
    setupOperatorWithHelm();

    client.pods().inNamespace(OPERATOR_NAMESPACE).resources().forEach(this::waitUntilPodIsReady);

    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    commonProperties.remove("druid.extensions.modulesForEmbeddedTests");
    applyConfigMap(
        newConfigMap(COMMON_CONFIG_MAP, commonProperties, "common.runtime.properties")
    );

    initializeDruidTestFolders(cluster.getTestFolder());

    String commonPropertiesString = prepareCommonPropertiesString(commonProperties);
    
    for (K3sDruidService druidService : services) {
      applyManifest(druidService.withCommonProperties(commonPropertiesString));
    }

    client.pods().inNamespace(DRUID_NAMESPACE).resources().forEach(this::waitUntilPodIsReady);
    services.forEach(this::waitUntilServiceIsHealthy);
  }

  private void initializeDruidTestFolders(TestFolder testFolder) {
    testFolder.getOrCreateFolder("druid-storage");
    testFolder.getOrCreateFolder("druid-storage/segments");
    testFolder.getOrCreateFolder("druid-storage/segment-cache");
    testFolder.getOrCreateFolder("druid-storage/metadata");
    testFolder.getOrCreateFolder("druid-storage/indexing-logs");
  }

  private String prepareCommonPropertiesString(Properties properties) {
    StringBuilder sb = new StringBuilder();
    sb.append("druid.zk.service.enabled=false\n");
    sb.append("    druid.discovery.type=k8s\n");
    sb.append("    druid.discovery.k8s.clusterIdentifier=druid-it\n");
    sb.append("    druid.serverview.type=http\n");
    sb.append("    druid.coordinator.loadqueuepeon.type=http\n");
    sb.append("    druid.indexer.runner.type=httpRemote\n");
    sb.append("    druid.metadata.storage.type=derby\n");
    sb.append("    druid.metadata.storage.connector.connectURI=jdbc:derby://localhost:1527/var/druid/metadata.db;create=true\n");
    sb.append("    druid.metadata.storage.connector.host=localhost\n");
    sb.append("    druid.metadata.storage.connector.port=1527\n");
    sb.append("    druid.metadata.storage.connector.createTables=true\n");
    sb.append("    druid.storage.type=local\n");
    sb.append("    druid.storage.storageDirectory=/druid/data/segments\n");
    sb.append("    druid.selectors.indexing.serviceName=druid/overlord\n");
    sb.append("    druid.selectors.coordinator.serviceName=druid/coordinator\n");
    sb.append("    druid.indexer.logs.type=file\n");
    sb.append("    druid.indexer.logs.directory=/druid/data/indexing-logs\n");
    
    for (String key : properties.stringPropertyNames()) {
      sb.append("    ").append(key).append("=").append(properties.getProperty(key)).append("\n");
    }
    return sb.toString();
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
}
