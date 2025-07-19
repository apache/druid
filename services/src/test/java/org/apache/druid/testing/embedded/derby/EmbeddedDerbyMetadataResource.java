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

package org.apache.druid.testing.embedded.derby;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorage;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

/**
 * Derby metadata store that runs in the test JVM but functions as a standalone process.
 * Clients can connect to it on the exposed port {@code 1527}. This resource should
 * be used only when running {@code DruidContainers}. If all servers are running
 * in embedded mode, use {@link InMemoryDerbyResource} instead.
 */
public class EmbeddedDerbyMetadataResource implements EmbeddedResource
{
  private static final int PORT = 1527;
  private static final String CONNECT_URI = "jdbc:derby://%s:%d/druid;create=true";

  private DerbyMetadataStorage storage;
  private MetadataStorageConnectorConfig connectorConfig;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    // Create the database in the TestFolder
    System.setProperty(
        "derby.system.home",
        cluster.getTestFolder().getOrCreateFolder("derby").getAbsolutePath()
    );

    connectorConfig = initConnectorConfig(cluster);
    storage = new DerbyMetadataStorage(connectorConfig);
  }

  @Override
  public void start() throws Exception
  {
    storage.start();
  }

  @Override
  public void stop() throws Exception
  {
    storage.stop();
    System.clearProperty("derby.system.home");
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster.addCommonProperty(
        "druid.metadata.storage.connector.connectURI",
        connectorConfig.getConnectURI()
    );
  }

  private MetadataStorageConnectorConfig initConnectorConfig(EmbeddedDruidCluster cluster)
  {
    return new MetadataStorageConnectorConfig()
    {
      @Override
      public String getHost()
      {
        return cluster.getEmbeddedServiceHostname();
      }

      @Override
      public int getPort()
      {
        return PORT;
      }

      @Override
      public String getConnectURI()
      {
        return StringUtils.format(CONNECT_URI, getHost(), getPort());
      }
    };
  }
}
