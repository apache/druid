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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.embedded.EmbeddedHostname;
import org.apache.druid.testing.embedded.indexing.Resources;

import java.nio.file.Files;
import java.util.Locale;
import java.util.Properties;

/**
 * Represents a single Druid service to be run inside a Kubernetes cluster using
 * {@link K3sClusterResource}.
 */
public class K3sDruidService
{
  private static final String KEY_COMMON_RUNTIME_PROPERTIES = "${commonRuntimeProperties}";
  private static final String KEY_NODE_RUNTIME_PROPERTIES = "${nodeRuntimeProperties}";

  private final DruidCommand command;
  private final Properties properties;
  private final Properties commonProperties;
  private Integer druidPort;
  private String manifestTemplate = "manifests/druid-service.yaml";

  public K3sDruidService(DruidCommand command)
  {
    this.command = command;
    this.properties = new Properties();
    this.commonProperties = new Properties();
    this.druidPort = command.getExposedPorts()[0];

    addProperty("druid.host", EmbeddedHostname.containerFriendly().toString());
    command.getDefaultProperties().forEach(properties::setProperty);
  }

  public K3sDruidService usingManifestTemplate(String manifestTemplate)
  {
    this.manifestTemplate = manifestTemplate;
    return this;
  }

  public K3sDruidService withCommonProperties(Properties commonProperties)
  {
    this.commonProperties.putAll(commonProperties);
    return this;
  }

  public K3sDruidService withDruidPort(Integer druidPort)
  {
    this.druidPort = druidPort;
    return this;
  }

  public Integer getDruidPort()
  {
    return druidPort;
  }

  public String getName()
  {
    return command.getName().toLowerCase(Locale.ROOT);
  }

  public DruidCommand getCommand()
  {
    return command;
  }

  /**
   * Creates a manifest YAML String for this service.
   */
  public String createManifestYaml(String druidImage)
  {
    try {
      final String template = Files.readString(
          Resources.getFileForResource(manifestTemplate).toPath()
      );

      String manifest = StringUtils.replace(template, "${service}", getName());
      manifest = StringUtils.replace(manifest, "${command}", command.getName());
      manifest = StringUtils.replace(manifest, "${port}", String.valueOf(druidPort));
      manifest = StringUtils.replace(manifest, "${image}", druidImage);
      manifest = StringUtils.replace(manifest, "${serviceFolder}", getServicePropsFolder());
      manifest = StringUtils.replace(manifest, KEY_COMMON_RUNTIME_PROPERTIES, buildPropertiesString(commonProperties, 4));
      manifest = StringUtils.replace(manifest, KEY_NODE_RUNTIME_PROPERTIES, buildPropertiesString(properties, 8));
      return manifest;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not create manifest for service[%s]", command);
    }
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

  public Properties getProperties()
  {
    return properties;
  }

  public K3sDruidService addProperty(String key, String value)
  {
    properties.setProperty(key, value);
    return this;
  }

  public String getHealthCheckUrl()
  {
    return StringUtils.format(
        "http://%s:%s/status/health",
        EmbeddedHostname.containerFriendly().toString(),
        isGovernedByOperator ? command.getExposedOperatorPort() : command.getExposedPorts()[0]
    );
  }

  private String getServicePropsFolder()
  {
    final DruidCommand.Server server = (DruidCommand.Server) command;
    switch (server) {
      case COORDINATOR:
      case OVERLORD:
        return "master/coordinator-overlord";
      case ROUTER:
        return "query/router";
      case BROKER:
        return "query/broker";
      case HISTORICAL:
        return "data/historical";
      case MIDDLE_MANAGER:
        return "data/middleManager";
      default:
        throw new IAE("Unsupported command[%s]", server);
    }
  }

  public String getServiceDiscoveryPath()
  {
    return EmbeddedHostname.containerFriendly().toString() + ":" + druidPort;
  }
}
