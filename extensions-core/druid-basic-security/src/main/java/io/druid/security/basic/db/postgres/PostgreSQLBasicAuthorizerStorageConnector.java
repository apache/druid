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

package io.druid.security.basic.db.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.db.SQLBasicAuthorizerStorageConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

public class PostgreSQLBasicAuthorizerStorageConnector extends SQLBasicAuthorizerStorageConnector
{
  private static final Logger log = new Logger(PostgreSQLBasicAuthorizerStorageConnector.class);

  private final DBI dbi;

  @Inject
  public PostgreSQLBasicAuthorizerStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector,
      ObjectMapper jsonMapper
  )
  {
    super(config, injector, jsonMapper);

    final BasicDataSource datasource = getDatasource();
    // PostgreSQL driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.postgresql.Driver");

    this.dbi = new DBI(datasource);

    log.info("Configured PostgreSQL as security storage");
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename ILIKE :tableName"
    )
                  .bind("tableName", tableName)
                  .map(StringMapper.FIRST)
                  .list()
                  .isEmpty();
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  public void createPermissionTable(String dbPrefix)
  {
    final String roleTableName = getPrefixedTableName(dbPrefix, ROLES);
    final String permissionTableName = getPrefixedTableName(dbPrefix, PERMISSIONS);

    createTable(
        permissionTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id SERIAL NOT NULL,\n"
                + "  resource_json BYTEA NOT NULL,\n"
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
}
