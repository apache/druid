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
  private static final String MANIFEST_TEMPLATE = "manifests/druid-service.yaml";

  /**
   * Use the Druid 34 image until the K3s tests are modified to load the latest
   * image built by CI into the K3s container.
   */
  private static final String DRUID_IMAGE = "apache/druid:34.0.0";

  private final DruidCommand command;
  private final Properties properties;

  public K3sDruidService(DruidCommand command)
  {
    this.command = command;
    this.properties = new Properties();

    addProperty("druid.host", EmbeddedHostname.containerFriendly().toString());
    command.getDefaultProperties().forEach(properties::setProperty);
  }

  public String getName()
  {
    return command.getName().toLowerCase(Locale.ROOT);
  }

  public DruidCommand getCommand()
  {
    return command;
  }

  public String createManifestYaml()
  {
    try {
      final String template = Files.readString(
          Resources.getFileForResource(MANIFEST_TEMPLATE).toPath()
      );

      String manifest = StringUtils.replace(template, "${service}", getName());
      manifest = StringUtils.replace(manifest, "${command}", command.getName());
      manifest = StringUtils.replace(manifest, "${port}", String.valueOf(command.getExposedPorts()[0]));
      manifest = StringUtils.replace(manifest, "${image}", DRUID_IMAGE);
      manifest = StringUtils.replace(manifest, "${serviceFolder}", getServicePropsFolder());

      return manifest;
    }
    catch (Exception e) {
      throw new ISE(e, "Could not create manifest for service[%s]", command);
    }
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
        command.getExposedPorts()[0]
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
}
