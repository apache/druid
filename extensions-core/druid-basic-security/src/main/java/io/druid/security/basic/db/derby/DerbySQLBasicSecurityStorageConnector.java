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

package io.druid.security.basic.db.derby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorage;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.BasicAuthConfig;
import io.druid.security.basic.db.SQLBasicSecurityStorageConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

@ManageLifecycle
public class DerbySQLBasicSecurityStorageConnector extends SQLBasicSecurityStorageConnector
{
  private static final Logger log = new Logger(DerbySQLBasicSecurityStorageConnector.class);

  private final DBI dbi;
  private final MetadataStorage storage;

  @Inject
  public DerbySQLBasicSecurityStorageConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<BasicAuthConfig> basicAuthConfigSupplier,
      ObjectMapper jsonMapper
  )
  {
    super(config, basicAuthConfigSupplier, jsonMapper);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");

    this.dbi = new DBI(datasource);
    this.storage = storage;
    log.info("Derby connector instantiated with metadata storage [%s].", this.storage.getClass().getName());
  }

  public DerbySQLBasicSecurityStorageConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<BasicAuthConfig> basicAuthConfigSupplier,
      ObjectMapper jsonMapper,
      DBI dbi
  )
  {
    super(config, basicAuthConfigSupplier, jsonMapper);
    this.dbi = dbi;
    this.storage = storage;
  }


  @Override
  @LifecycleStart
  public void start()
  {
    storage.start();
    super.start();
  }

  @Override
  public void createRoleTable()
  {
    createTable(
        ROLES,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name)\n"
                + ")",
                ROLES
            )
        )
    );
  }

  @Override
  public void createUserTable()
  {
    createTable(
        USERS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name)\n"
                + ")",
                USERS
            )
        )
    );
  }

  @Override
  public void createPermissionTable()
  {
    createTable(
        PERMISSIONS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
                + "  resource_json BLOB(1024) NOT NULL,\n"
                + "  role_name VARCHAR(255) NOT NULL, \n"
                + "  PRIMARY KEY (id),\n"
                + "  FOREIGN KEY (role_name) REFERENCES roles(name) ON DELETE CASCADE\n"
                + ")",
                PERMISSIONS
            )
        )
    );
  }

  @Override
  public void createUserCredentialsTable()
  {
    createTable(
        USER_CREDENTIALS,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL, \n"
                + "  salt BLOB(32) NOT NULL, \n"
                + "  hash BLOB(64) NOT NULL, \n"
                + "  iterations INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (user_name), \n"
                + "  FOREIGN KEY (user_name) REFERENCES users(name) ON DELETE CASCADE\n"
                + ")",
                USER_CREDENTIALS
            )
        )
    );
  }


  @Override
  public boolean tableExists(Handle handle, String tableName)
  {
    return !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                  .bind("tableName", StringUtils.toUpperCase(tableName))
                  .list()
                  .isEmpty();
  }

  @Override
  public String getValidationQuery()
  {
    return "VALUES 1";
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }
}
