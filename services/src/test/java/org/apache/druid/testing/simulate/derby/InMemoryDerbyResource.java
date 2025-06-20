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

package org.apache.druid.testing.simulate.derby;

import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedResource;

/**
 * Resource to run an embedded Derby metadata store in memory.
 */
public class InMemoryDerbyResource implements EmbeddedResource
{
  private final EmbeddedDruidCluster cluster;
  private final TestDerbyConnector.DerbyConnectorRule dbRule;

  public InMemoryDerbyResource(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
    this.dbRule = new TestDerbyConnector.DerbyConnectorRule();
  }

  @Override
  public void start()
  {
    dbRule.before();

    final TestDerbyConnector connector = dbRule.getConnector();
    cluster.addCommonProperty("druid.metadata.storage.type", InMemoryDerbyModule.TYPE);
    cluster.addCommonProperty("druid.metadata.storage.tables.base", connector.getMetadataTablesConfig().getBase());
    cluster.addCommonProperty("druid.metadata.storage.connector.connectURI", connector.getJdbcUri());
  }

  @Override
  public void stop()
  {
    dbRule.after();
  }
}
