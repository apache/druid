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

package org.apache.druid.testing.embedded.psql;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLMetadataStorageModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Resource that creates a PostgreSQL metadata store.
 */
public class PostgreSQLMetadataResource extends TestcontainerResource<PostgreSQLContainer<?>>
{
  private static final String DATABASE_NAME = "druid_test";
  private static final String USERNAME = "sally";
  private static final String PASSWORD = "diurd";

  private String connectURI;

  @Override
  protected PostgreSQLContainer<?> createContainer()
  {
    return new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName(DATABASE_NAME)
        .withUsername(USERNAME)
        .withPassword(PASSWORD);
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    connectURI = createConnectURI(cluster);
    cluster.addExtension(PostgreSQLMetadataStorageModule.class);
    cluster.addCommonProperty("druid.metadata.storage.type", PostgreSQLMetadataStorageModule.TYPE);
    cluster.addCommonProperty("druid.metadata.storage.connector.connectURI", createConnectURI(cluster));
    cluster.addCommonProperty("druid.metadata.storage.connector.user", getUsername());
    cluster.addCommonProperty("druid.metadata.storage.connector.password", getPassword());
  }

  public String getDatabaseName()
  {
    return DATABASE_NAME;
  }

  public String getUsername()
  {
    return USERNAME;
  }

  public String getPassword()
  {
    return PASSWORD;
  }

  public String getConnectURI()
  {
    ensureRunning();
    return connectURI;
  }

  public String createConnectURI(EmbeddedDruidCluster cluster)
  {
    ensureRunning();
    return StringUtils.format(
        "jdbc:postgresql://%s:%d/%s",
        cluster.getEmbeddedHostname(),
        getContainer().getMappedPort(5432),
        DATABASE_NAME
    );
  }
}
