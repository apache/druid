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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.db.SQLBasicAuthenticatorStorageConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.StringMapper;

public class PostgreSQLBasicAuthenticatorStorageConnector extends SQLBasicAuthenticatorStorageConnector
{
  private static final Logger log = new Logger(PostgreSQLBasicAuthenticatorStorageConnector.class);

  private final DBI dbi;

  @Inject
  public PostgreSQLBasicAuthenticatorStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector
  )
  {
    super(config, injector);

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
  public void createUserCredentialsTable(String dbPrefix)
  {
    final String userTableName = getPrefixedTableName(dbPrefix, USERS);
    final String credentialsTableName = getPrefixedTableName(dbPrefix, USER_CREDENTIALS);

    createTable(
        credentialsTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL, \n"
                + "  salt BYTEA NOT NULL, \n"
                + "  hash BYTEA NOT NULL, \n"
                + "  iterations INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (user_name),\n"
                + "  FOREIGN KEY (user_name) REFERENCES %2$s(name) ON DELETE CASCADE\n"
                + ")",
                credentialsTableName,
                userTableName
            )
        )
    );
  }
}
