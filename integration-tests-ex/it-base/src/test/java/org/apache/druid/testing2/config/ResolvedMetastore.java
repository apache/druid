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

package org.apache.druid.testing2.config;

import org.apache.commons.lang3.RegExUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig;

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

  public MetadataStorageConnectorConfig toMetadataConfig()
  {
    return MetadataStorageConnectorConfig.create(
        connectURI,
        user,
        password,
        properties);
  }

  public MySQLConnectorDriverConfig toDriverConfig()
  {
    return MySQLConnectorDriverConfig.create(driver);
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
