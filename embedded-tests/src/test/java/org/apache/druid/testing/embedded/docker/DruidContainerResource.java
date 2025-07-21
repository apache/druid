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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * {@link TestcontainerResource} to run Druid services.
 * Currently, only core extensions can be used out-of-the-box with these containers
 * such as {@code druid-s3-extensions} or {@code postgresql-metadata-storage},
 * simply by adding them to {@code druid.extensions.loadList}.
 * <p>
 * {@link DruidContainers} should be used only for testing backward compatiblity
 * or a Docker-specific feature. For all other testing needs, use plain old
 * {@code EmbeddedDruidServer} as they are much faster, allow easy debugging and
 * do not require downloading any images.
 */
public class DruidContainerResource extends TestcontainerResource<DruidContainer>
{
  /**
   * Java system property to specify the name of the Docker test image.
   */
  public static final String PROPERTY_TEST_IMAGE = "druid.testing.docker.image";

  private static final Logger log = new Logger(DruidContainerResource.class);

  /**
   * Forbidden server properties that may be used by EmbeddedDruidServers but
   * interfere with the functioning of DruidContainer-based services.
   */
  private static final Set<String> FORBIDDEN_PROPERTIES = Set.of(
      "druid.extensions.modulesForEmbeddedTests",
      "druid.emitter"
  );

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;
  private final DruidCommand command;
  private final Map<String, String> properties = new HashMap<>();
  private final List<Integer> taskPorts = new ArrayList<>();

  private int servicePort;
  private DockerImageName imageName;
  private EmbeddedDruidCluster cluster;

  private String containerDirectory;

  private MountedDir indexerLogsDeepStorageDirectory;
  private MountedDir serviceLogsDirectory;
  private MountedDir segmentDeepStorageDirectory;

  DruidContainerResource(DruidCommand command)
  {
    this.name = StringUtils.format(
        "container_%s_%d",
        command.getName(),
        SERVER_ID.incrementAndGet()
    );
    this.command = command;

    Integer[] exposedPorts = command.getExposedPorts();
    servicePort = exposedPorts[0];
    taskPorts.addAll(Arrays.asList(exposedPorts).subList(1, exposedPorts.length));
  }

  public DruidContainerResource withPort(int port)
  {
    this.servicePort = port;
    return this;
  }

  /**
   * Used to bind the task ports on middle managers.
   */
  public DruidContainerResource withTaskPorts(int startPort, int workerCapacity)
  {
    taskPorts.clear();
    for (int i = 0; i < workerCapacity; ++i) {
      taskPorts.add(startPort + i);
    }
    addProperty("druid.worker.capacity", String.valueOf(workerCapacity));
    addProperty("druid.indexer.runner.startPort", String.valueOf(startPort));
    addProperty("druid.indexer.runner.endPort", String.valueOf(startPort + workerCapacity));

    return this;
  }

  public DruidContainerResource withImage(DockerImageName imageName)
  {
    this.imageName = imageName;
    return this;
  }

  /**
   * Uses the Docker test image specified by the system property
   * {@link #PROPERTY_TEST_IMAGE} for this container.
   */
  public DruidContainerResource withTestImage()
  {
    String imageName = Objects.requireNonNull(
        System.getProperty(PROPERTY_TEST_IMAGE),
        StringUtils.format("System property[%s] is not set", PROPERTY_TEST_IMAGE)
    );
    return withImage(DockerImageName.parse(imageName));
  }

  public DruidContainerResource addProperty(String key, String value)
  {
    properties.put(key, value);
    return this;
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;

    // Mount directories used by the entire cluster (including embedded servers)
    this.segmentDeepStorageDirectory = new MountedDir(
        "/tmp/druid/deep-store",
        cluster.getTestFolder().getOrCreateFolder("deep-store").getAbsolutePath()
    );
    this.indexerLogsDeepStorageDirectory = new MountedDir(
        "/tmp/druid/indexer-logs",
        cluster.getTestFolder().getOrCreateFolder("indexer-logs").getAbsolutePath()
    );

    // Mount directories used by this container for easier debugging with service logs
    // this.containerDirectory = cluster.getTestFolder().getOrCreateFolder(name).getAbsolutePath();
    this.containerDirectory = new File("/tmp/embedded-tests/" + cluster.getId(), name).getAbsolutePath();
    this.serviceLogsDirectory = new MountedDir(
        "/opt/druid/log",
        containerDirectory + "/log"
    );

    // Create the log directory upfront to avoid permission issues
    createLogDirectory(new File(containerDirectory, "log"));
  }

  @Override
  protected DruidContainer createContainer()
  {
    final List<Integer> exposedPorts = new ArrayList<>(taskPorts);
    exposedPorts.add(servicePort);
    log.info(
        "Starting Druid container[%s] on port[%d] with mounted directory[%s] and exposed ports[%s].",
        name, servicePort, containerDirectory, exposedPorts
    );

    final DruidContainer container = new DruidContainer(command, imageName)
        .withExposedPorts(exposedPorts.toArray(new Integer[0]))
        .withCommonProperties(getCommonProperties())
        .withServiceProperties(getServerProperties())
        .withFileSystemBind(
            segmentDeepStorageDirectory.hostPath,
            segmentDeepStorageDirectory.containerPath,
            BindMode.READ_WRITE
        )
        .withFileSystemBind(
            indexerLogsDeepStorageDirectory.hostPath,
            indexerLogsDeepStorageDirectory.containerPath,
            BindMode.READ_WRITE
        )
        .withFileSystemBind(
            serviceLogsDirectory.hostPath,
            serviceLogsDirectory.containerPath,
            BindMode.READ_WRITE
        )
        .withEnv(
            Map.of(
                "DRUID_SET_HOST_IP", "0",
                "DRUID_SET_HOST", "0"
            )
        )
        .waitingFor(Wait.forHttp("/status/health").forPort(servicePort));

    // Bind the ports statically (rather than using a mapped port) to the same
    // value used in `druid.plaintextPort` to make this node discoverable
    // by other services (both embedded and dockerized).
    List<String> portBindings = exposedPorts.stream().map(
        port -> StringUtils.format("%d:%d", port, port)
    ).collect(Collectors.toList());
    container.setPortBindings(portBindings);

    return container;
  }

  /**
   * Writes the common properties to a file.
   *
   * @return Name of the file.
   */
  private Properties getCommonProperties()
  {
    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    FORBIDDEN_PROPERTIES.forEach(commonProperties::remove);

    commonProperties.setProperty(
        "druid.storage.storageDirectory",
        segmentDeepStorageDirectory.containerPath
    );
    commonProperties.setProperty(
        "druid.indexer.logs.directory",
        indexerLogsDeepStorageDirectory.containerPath
    );

    log.info(
        "Writing common properties for Druid container[%s]: [%s]",
        name, commonProperties
    );

    return commonProperties;
  }

  /**
   * Writes the server properties to a file.
   *
   * @return Name of the file.
   */
  private Properties getServerProperties()
  {
    FORBIDDEN_PROPERTIES.forEach(properties::remove);
    addProperty("druid.host", hostMachine());
    addProperty("druid.plaintextPort", String.valueOf(servicePort));

    final Properties serverProperties = new Properties();
    serverProperties.putAll(properties);

    log.info(
        "Writing runtime properties for Druid container[%s]: [%s]",
        name, serverProperties
    );

    return serverProperties;
  }

  private static void createLogDirectory(File dir)
  {
    try {
      FileUtils.mkdirp(dir);
      Files.setPosixFilePermissions(dir.toPath(), PosixFilePermissions.fromString("rwxrwxrwx"));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Hostname for the host machine running the containers. Using this hostname
   * instead of "localhost" allows all the Druid containers to talk to each
   * other and also other EmbeddedDruidServers.
   */
  private static String hostMachine()
  {
    return DruidNode.getDefaultHost();
  }

  @Override
  public String toString()
  {
    return name;
  }

  private static class MountedDir
  {
    final String hostPath;
    final String containerPath;

    MountedDir(String containerPath, String hostPath)
    {
      this.hostPath = hostPath;
      this.containerPath = containerPath;
    }
  }
}
