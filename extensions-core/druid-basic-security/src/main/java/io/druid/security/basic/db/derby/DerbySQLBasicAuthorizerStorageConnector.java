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
import com.google.inject.Injector;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorage;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.db.SQLBasicAuthorizerStorageConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

@ManageLifecycle
public class DerbySQLBasicAuthorizerStorageConnector extends SQLBasicAuthorizerStorageConnector
{
  private static final Logger log = new Logger(DerbySQLBasicAuthorizerStorageConnector.class);

  private final DBI dbi;
  private final MetadataStorage storage;

  @Inject
  public DerbySQLBasicAuthorizerStorageConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector,
      ObjectMapper jsonMapper
  )
  {
    super(config, injector, jsonMapper);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");

    this.dbi = new DBI(datasource);
    this.storage = storage;
    log.info("Derby connector instantiated with metadata storage [%s].", this.storage.getClass().getName());
  }

  public DerbySQLBasicAuthorizerStorageConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector,
      ObjectMapper jsonMapper,
      DBI dbi
  )
  {
    super(config, injector, jsonMapper);
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
  public void createRoleTable(String dbPrefix)
  {
    final String roleTableName = getPrefixedTableName(dbPrefix, ROLES);

    createTable(
        roleTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name)\n"
                + ")",
                roleTableName
            )
        )
    );
  }

  @Override
  public void createUserTable(String dbPrefix)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);

    createTable(
        userTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name)\n"
                + ")",
                userTableName
            )
        )
    );
  }

  @Override
  public void createPermissionTable(String dbPrefix)
  {
    final String permissionTableName = getPrefixedTableName(dbPrefix, PERMISSIONS);
    final String roleTableName = getPrefixedTableName(dbPrefix, ROLES);

    createTable(
        permissionTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),\n"
                + "  resource_json BLOB(1024) NOT NULL,\n"
                + "  role_name VARCHAR(255) NOT NULL, \n"
                + "  PRIMARY KEY (id),\n"
                + "  FOREIGN KEY (role_name) REFERENCES %2$s(name) ON DELETE CASCADE\n"
                + ")",
                permissionTableName,
                roleTableName
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
