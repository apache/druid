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

package org.apache.druid.testsEx.config;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RegExUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResolvedMetastore extends ResolvedService
{
  // Set to be 1 sec. longer than the setting in the
  // docker-compose.yaml file:
  // druid_manager_segments_pollDuration=PT5S
  public static final int DEFAULT_METASTORE_INIT_DELAY_SEC = 6;

  private final String driver;
  private final String connectURI;
  private final String user;
  private final String password;
  private final Map<String, Object> properties;
  private final int initDelaySec;
  private List<MetastoreStmt> initStmts;

  public ResolvedMetastore(ResolvedConfig root, MetastoreConfig config, ClusterConfig clusterConfig)
  {
    super(root, config, "metastore");
    this.driver = config.driver();
    if (config.connectURI() != null) {
      ResolvedInstance instance = instance();
      this.connectURI = RegExUtils.replaceAll(
          RegExUtils.replaceAll(
              config.connectURI(),
              "<port>",
              Integer.toString(instance.clientPort())),
          "<host>",
          instance.clientHost());
    } else {
      this.connectURI = null;
    }
    this.user = config.user();
    this.password = config.password();
    if (config.properties() == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = config.properties();
    }

    this.initDelaySec = clusterConfig.metastoreInitDelaySec() > 0
        ? clusterConfig.metastoreInitDelaySec()
        : DEFAULT_METASTORE_INIT_DELAY_SEC;
    this.initStmts = clusterConfig.metastoreInit();
  }

  public String driver()
  {
    return driver;
  }

  public String connectURI()
  {
    return connectURI;
  }

  public String user()
  {
    return user;
  }

  public String password()
  {
    return password;
  }

  public Map<String, Object> properties()
  {
    return properties;
  }

  /**
   * Create the properties Guice needs to create the connector config.
   *
   * @see <a href="https://druid.apache.org/docs/0.23.0/development/extensions-core/mysql.html#setting-up-mysql">
   * Setting up MySQL</a>
   */
  public Map<String, Object> toProperties()
  {
    final String base = MetadataStorageConnectorConfig.PROPERTY_BASE;
    Map<String, Object> properties = new HashMap<>();
    TestConfigs.putProperty(properties, "druid.metadata.mysql.driver.driverClassName", driver);
    TestConfigs.putProperty(properties, "druid.metadata.storage.type", "mysql");
    TestConfigs.putProperty(properties, base, "connectURI", connectURI);
    TestConfigs.putProperty(properties, base, "user", user);
    TestConfigs.putProperty(properties, base, "password", password);
    TestConfigs.putProperty(properties, base, "dbcp", this.properties);
    return properties;
  }

  public List<MetastoreStmt> initStmts()
  {
    return initStmts;
  }

  public int initDelaySec()
  {
    return initDelaySec;
  }
}
