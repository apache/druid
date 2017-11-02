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

package io.druid.security.basic.db.mysql;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.db.SQLBasicAuthenticatorStorageConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.BooleanMapper;

public class MySQLBasicAuthenticatorStorageConnector extends SQLBasicAuthenticatorStorageConnector
{
  private static final Logger log = new Logger(MySQLBasicAuthenticatorStorageConnector.class);

  private final DBI dbi;

  @Inject
  public MySQLBasicAuthenticatorStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Injector injector
  )
  {
    super(config, injector);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("com.mysql.jdbc.Driver");

    // use double-quotes for quoting columns, so we can write SQL that works with most databases
    datasource.setConnectionInitSqls(ImmutableList.of("SET sql_mode='ANSI_QUOTES'"));

    this.dbi = new DBI(datasource);
    log.info("Configured MySQL as security storage");
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
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                userTableName
            )
        )
    );
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
                + "  salt BLOB(32) NOT NULL, \n"
                + "  hash BLOB(64) NOT NULL, \n"
                + "  iterations INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (user_name), \n"
                + "  FOREIGN KEY (user_name) REFERENCES %2$s(name) ON DELETE CASCADE\n"
                + ")",
                credentialsTableName,
                userTableName
            )
        )
    );
  }


  @Override
  public boolean tableExists(Handle handle, String tableName)
  {
    // ensure database defaults to utf8, otherwise bail
    boolean isUtf8 = handle
        .createQuery("SELECT @@character_set_database = 'utf8'")
        .map(BooleanMapper.FIRST)
        .first();

    if (!isUtf8) {
      throw new ISE(
          "Database default character set is not UTF-8." + System.lineSeparator()
          + "  Druid requires its MySQL database to be created using UTF-8 as default character set."
      );
    }

    return !handle.createQuery("SHOW tables LIKE :tableName")
                  .bind("tableName", tableName)
                  .list()
                  .isEmpty();
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }
}
