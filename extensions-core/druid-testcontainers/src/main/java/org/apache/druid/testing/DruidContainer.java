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

package org.apache.druid.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.StringWriter;
import java.util.Properties;

/**
 * Testcontainer for running Apache Druid services.
 * <p>
 * Usage:
 * <pre>
 *
 * </pre>
 * Dependencies of Druid services:
 * <ul>
 * <li>All Druid services need to be able to access </li>
 * </ul>
 */
public class DruidContainer extends GenericContainer<DruidContainer>
{
  /**
   * Standard images for {@link DruidContainer}.
   */
  public static class Image
  {
    public static final DockerImageName APACHE_31 = DockerImageName.parse("apache/druid:31.0.2");
    public static final DockerImageName APACHE_32 = DockerImageName.parse("apache/druid:32.0.1");
    public static final DockerImageName APACHE_33 = DockerImageName.parse("apache/druid:33.0.0");
  }

  private static final String COMMON_PROPERTIES_PATH = "/tmp/druid_conf_common.runtime.properties";
  private static final String SERVICE_PROPERTIES_PATH = "/tmp/druid_conf_runtime.properties";

  private final Properties commonProperties = new Properties();
  private final Properties serviceProperties = new Properties();

  /**
   * Creates a new {@link DruidContainer} which uses the given image name.
   *
   * @param command   Druid command to run. e.g. "coordinator", "overlord".
   * @param imageName Name of the Druid image to use
   * @see DruidCommand for standard Druid commands.
   * @see Image for standard Druid images
   */
  public DruidContainer(String command, String imageName)
  {
    this(DruidCommand.valueOf(command), DockerImageName.parse(imageName));
  }

  /**
   * Creates a new {@link DruidContainer} which uses the given image name.
   *
   * @param command   Druid command to run. e.g. "coordinator", "overlord".
   * @param imageName Name of the Druid image to use
   * @see DruidCommand for standard Druid commands.
   * @see Image for standard Druid images
   */
  public DruidContainer(DruidCommand command, DockerImageName imageName)
  {
    super(imageName);

    setCommand(command.getName());
    withExposedPorts(command.getExposedPorts());
    withEnv("DRUID_CONFIG_COMMON", COMMON_PROPERTIES_PATH);
    withEnv("DRUID_CONFIG_" + command.getName(), SERVICE_PROPERTIES_PATH);

    serviceProperties.putAll(command.getDefaultProperties());
  }

  /**
   * Sets the common properties to be used for the service running on this container.
   * The properties are written out to the file {@link #COMMON_PROPERTIES_PATH}.
   */
  public DruidContainer withCommonProperties(Properties commonProperties)
  {
    this.commonProperties.putAll(commonProperties);
    return this;
  }

  /**
   * Sets the runtime properties to be used for the service running on this container.
   * The properties are written out to the file {@link #SERVICE_PROPERTIES_PATH}.
   */
  public DruidContainer withServiceProperties(Properties serviceProperties)
  {
    this.serviceProperties.putAll(serviceProperties);
    return this;
  }

  @Override
  protected void containerIsCreated(String containerId)
  {
    copyFileToContainer(Transferable.of(toString(commonProperties), 0777), COMMON_PROPERTIES_PATH);
    copyFileToContainer(Transferable.of(toString(serviceProperties), 0777), SERVICE_PROPERTIES_PATH);
  }

  private static String toString(Properties properties)
  {
    try (StringWriter writer = new StringWriter()) {
      properties.store(writer, "Druid Runtime Properties");
      return writer.toString();
    }
    catch (Exception e) {
      throw new RuntimeException("Could not serialize Druid runtime properties", e);
    }
  }
}
