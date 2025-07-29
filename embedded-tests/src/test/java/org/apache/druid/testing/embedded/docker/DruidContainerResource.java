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
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.MountedDir;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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

  private DockerImageName imageName;
  private EmbeddedDruidCluster cluster;

  private File containerDirectory;

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
    addProperty("druid.host", EmbeddedDruidCluster.getDefaultHost());
  }

  public DruidContainerResource usingImage(DockerImageName imageName)
  {
    this.imageName = imageName;
    return this;
  }

  /**
   * Uses the Docker test image specified by the system property
   * {@link #PROPERTY_TEST_IMAGE} for this container.
   */
  public DruidContainerResource usingTestImage()
  {
    String imageName = Objects.requireNonNull(
        System.getProperty(PROPERTY_TEST_IMAGE),
        StringUtils.format("System property[%s] is not set", PROPERTY_TEST_IMAGE)
    );
    return usingImage(DockerImageName.parse(imageName));
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
        new File("/druid/deep-store"),
        cluster.getTestFolder().getOrCreateFolder("deep-store")
    );
    this.indexerLogsDeepStorageDirectory = new MountedDir(
        new File("/druid/indexer-logs"),
        cluster.getTestFolder().getOrCreateFolder("indexer-logs")
    );

    // Mount directories used by this container for easier debugging with service logs
    this.containerDirectory = cluster.getTestFolder().getOrCreateFolder(name);

    final File logDirectory = new File(containerDirectory, "log");
    this.serviceLogsDirectory = new MountedDir(new File("/opt/druid/log"), logDirectory);

    // Create the log directory upfront to avoid permission issues
    createLogDirectory(logDirectory);
  }

  @Override
  protected DruidContainer createContainer()
  {
    final DruidContainer container = new DruidContainer(command, imageName)
        .withFileSystemBind(segmentDeepStorageDirectory)
        .withFileSystemBind(indexerLogsDeepStorageDirectory)
        .withFileSystemBind(serviceLogsDirectory)
        .withEnv(
            Map.of(
                "DRUID_SET_HOST_IP", "0",
                "DRUID_SET_HOST", "0"
            )
        );

    log.info(
        "Starting Druid container[%s] with mounted directory[%s] and exposed ports[%s].",
        name, containerDirectory, Arrays.toString(command.getExposedPorts())
    );

    setCommonProperties(container);
    setServerProperties(container);

    return container;
  }

  private void setCommonProperties(DruidContainer container)
  {
    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    FORBIDDEN_PROPERTIES.forEach(commonProperties::remove);

    commonProperties.setProperty(
        "druid.storage.storageDirectory",
        segmentDeepStorageDirectory.containerFile().getAbsolutePath()
    );
    commonProperties.setProperty(
        "druid.indexer.logs.directory",
        indexerLogsDeepStorageDirectory.containerFile().getAbsolutePath()
    );

    log.info(
        "Writing common properties for Druid container[%s]: [%s]",
        name, commonProperties
    );

    for (String key : commonProperties.stringPropertyNames()) {
      container.withCommonProperty(key, commonProperties.getProperty(key));
    }
  }

  private void setServerProperties(DruidContainer container)
  {
    FORBIDDEN_PROPERTIES.forEach(properties::remove);
    log.info(
        "Writing runtime properties for Druid container[%s]: [%s]",
        name, properties
    );

    properties.forEach(container::withServiceProperty);
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

  @Override
  public String toString()
  {
    return name;
  }

}
