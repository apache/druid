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

package io.druid.security.basic.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.cli.GuiceRunnable;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.db.BasicSecurityStorageConnector;
import io.druid.server.DruidNode;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;

import java.util.List;

@Command(
    name = "authorization-init",
    description = "Initialize Authorization Storage"
)
public class CreateAuthorizationTables extends GuiceRunnable
{
  private static final String DEFAULT_ADMIN_NAME = "admin";
  private static final String DEFAULT_ADMIN_ROLE = "admin";
  private static final String DEFAULT_ADMIN_PASS = "druid";

  private static final String DEFAULT_SYSTEM_USER_NAME = "druid_system";
  private static final String DEFAULT_SYSTEM_USER_ROLE = "druid_system";
  private static final String DEFAULT_SYSTEM_USER_PASS = "druid";

  @Option(name = "--connectURI", description = "Database JDBC connection string", required = true)
  private String connectURI;

  @Option(name = "--user", description = "Database username", required = true)
  private String user;

  @Option(name = "--password", description = "Database password", required = true)
  private String password;

  @Option(name = "--admin-user", description = "Name of default admin to be created.", required = false)
  private String adminUser;

  @Option(name = "--admin-password", description = "Password of default admin to be created.", required = false)
  private String adminPassword;

  @Option(name = "--admin-role", description = "Role of default admin to be created.", required = false)
  private String adminRole;

  @Option(name = "--system-user", description = "Name of internal system user to be created.", required = false)
  private String systemUser;

  @Option(name = "--system-password", description = "Password of internal system user to be created.", required = false)
  private String systemPassword;

  @Option(name = "--system-role", description = "Role of internal system user to be created.", required = false)
  private String systemRole;

  @Option(name = "--base", description = "Base table name")
  private String base;

  private static final Logger log = new Logger(CreateAuthorizationTables.class);

  public CreateAuthorizationTables()
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
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("tools", "localhost", -1, null, true, false)
            );
          }
        }
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    BasicSecurityStorageConnector dbConnector = injector.getInstance(BasicSecurityStorageConnector.class);
    ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);

    dbConnector.createUserTable();
    dbConnector.createRoleTable();
    dbConnector.createPermissionTable();
    dbConnector.createUserRoleTable();
    dbConnector.createUserCredentialsTable();

    setupDefaultAdmin(dbConnector, jsonMapper);
    setupInternalDruidSystemUser(dbConnector, jsonMapper);
  }

  private void setupInternalDruidSystemUser(BasicSecurityStorageConnector dbConnector, ObjectMapper jsonMapper)
  {
    if (systemUser == null) {
      systemUser = DEFAULT_SYSTEM_USER_NAME;
    }

    if (systemPassword == null) {
      systemPassword = DEFAULT_SYSTEM_USER_PASS;
    }

    if (systemRole == null) {
      systemRole = DEFAULT_SYSTEM_USER_ROLE;
    }

    dbConnector.createUser(systemUser);
    dbConnector.createRole(systemRole);
    dbConnector.assignRole(systemUser, systemRole);

    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    List<ResourceAction> resActs = Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);

    for (ResourceAction resAct : resActs) {
      dbConnector.addPermission(systemRole, resAct);
    }

    dbConnector.setUserCredentials(systemUser, systemPassword.toCharArray());
  }

  private void setupDefaultAdmin(BasicSecurityStorageConnector dbConnector, ObjectMapper jsonMapper)
  {
    if (adminUser == null) {
      adminUser = DEFAULT_ADMIN_NAME;
    }

    if (adminPassword == null) {
      adminPassword = DEFAULT_ADMIN_PASS;
    }

    if (adminRole == null) {
      adminRole = DEFAULT_ADMIN_ROLE;
    }

    dbConnector.createUser(adminUser);
    dbConnector.createRole(adminRole);
    dbConnector.assignRole(adminUser, adminRole);

    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    List<ResourceAction> resActs = Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);

    for (ResourceAction resAct : resActs) {
      dbConnector.addPermission(adminRole, resAct);
    }

    dbConnector.setUserCredentials(adminUser, adminPassword.toCharArray());
  }
}
