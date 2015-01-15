/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
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
    dbConnector.createSegmentTable();
    dbConnector.createRulesTable();
    dbConnector.createConfigTable();
    dbConnector.createTaskTables();
  }
}
