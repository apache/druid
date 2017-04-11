/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.server.DruidNode;

import java.util.List;

@Command(
    name = "metadata-init",
    description = "Initialize Metadata Storage"
)
public class CreateTables extends GuiceRunnable
{
  @Option(name = "--connectURI", description = "Database JDBC connection string", required = true)
  private String connectURI;

  @Option(name = "--user", description = "Database username", required = true)
  private String user;

  @Option(name = "--password", description = "Database password", required = true)
  private String password;

  @Option(name = "--base", description = "Base table name")
  private String base;

  private static final Logger log = new Logger(CreateTables.class);

  public CreateTables()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(MetadataStorageConnectorConfig.class), new MetadataStorageConnectorConfig()
                {
                  @Override
                  public String getConnectURI()
                  {
                    return connectURI;
                  }

                  @Override
                  public String getUser()
                  {
                    return user;
                  }

                  @Override
                  public String getPassword()
                  {
                    return password;
                  }
                }
            );
            JsonConfigProvider.bindInstance(
                binder, Key.get(MetadataStorageTablesConfig.class), MetadataStorageTablesConfig.fromBase(base)
            );
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("tools", "localhost", -1)
            );
          }
        }
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    MetadataStorageConnector dbConnector = injector.getInstance(MetadataStorageConnector.class);
    dbConnector.createDataSourceTable();
    dbConnector.createPendingSegmentsTable();
    dbConnector.createSegmentTable();
    dbConnector.createRulesTable();
    dbConnector.createConfigTable();
    dbConnector.createTaskTables();
    dbConnector.createAuditTable();
    dbConnector.createSupervisorsTable();
  }
}
