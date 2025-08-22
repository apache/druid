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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Represents a single Druid service to be run inside a Kubernetes cluster using
 * {@link K3sClusterResource}.
 */
public class K3sDruidService
{
  private final DruidCommand command;
  private final Properties properties;
  private boolean isGovernedByOperator = false;
  private String manifestTemplate = "manifests/druid-service.yaml";
  private Map<String, String> operatorVariablesTemplate = Map.of();

  public K3sDruidService(DruidCommand command)
  {
    this.command = command;
    this.properties = new Properties();

    // Don't set druid.host here - it will be set when operator variables are available
    command.getDefaultProperties().forEach(properties::setProperty);
  }

  public K3sDruidService governWithOperator()
  {
    this.operatorVariablesTemplate = new HashMap<>(command.getOperatorConfiguration());
    this.isGovernedByOperator = true;
    this.manifestTemplate = "manifests/druid-service-operator.yaml";
    return this;
  }

  public K3sDruidService withCommonProperties(String commonProperties)
  {
    this.operatorVariablesTemplate = new HashMap<>(this.operatorVariablesTemplate);
    this.operatorVariablesTemplate.put("commonRuntimeProperties", commonProperties);
    return this;
  }

  public Map<String, String> getOperatorVariablesTemplate()
  {
    return operatorVariablesTemplate;
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
      String port = isGovernedByOperator
                    ? String.valueOf(command.getExposedOperatorPort())
                    : String.valueOf(command.getExposedPorts()[0]);
      manifest = StringUtils.replace(manifest, "${port}", port);
      manifest = StringUtils.replace(manifest, "${image}", druidImage);
      manifest = StringUtils.replace(manifest, "${serviceFolder}", getServicePropsFolder());

      if (isGovernedByOperator) {
        manifest = replaceOperatorVariables(manifest);
      }
      return manifest;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not create manifest for service[%s]", command);
    }
  }

  private String replaceOperatorVariables(String manifest)
  {
    String hostname = EmbeddedHostname.containerFriendly().toString();
    this.properties.setProperty("druid.host", hostname);

    String nodePropertiesString = prepareNodePropertiesString(this.properties);
    manifest = StringUtils.replace(manifest, "${nodeRuntimeProperties}", nodePropertiesString);

    for (Map.Entry<String, String> entry : operatorVariablesTemplate.entrySet()) {
      manifest = StringUtils.replace(manifest, "${" + entry.getKey() + "}", entry.getValue());
    }
    return manifest;
  }

  private String prepareNodePropertiesString(Properties nodeProperties)
  {
    StringBuilder sb = new StringBuilder();
    for (String key : nodeProperties.stringPropertyNames()) {
      sb.append("        ").append(key).append("=").append(nodeProperties.getProperty(key)).append("\n");
    }
    return sb.toString().trim(); // Remove trailing newline
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
    return EmbeddedHostname.containerFriendly().toString() + ":" + command.getExposedOperatorPort();
  }
}
