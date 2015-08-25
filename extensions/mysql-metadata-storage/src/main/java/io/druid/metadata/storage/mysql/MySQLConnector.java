/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.metadata.storage.mysql;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.mysql.jdbc.exceptions.MySQLTransientException;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;

public class MySQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(MySQLConnector.class);
  private static final String PAYLOAD_TYPE = "LONGBLOB";
  private static final String SERIAL_TYPE = "BIGINT(20) AUTO_INCREMENT";

  private final DBI dbi;

  @Inject
  public MySQLConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    // MySQL driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("com.mysql.jdbc.Driver");

    // use double-quotes for quoting columns, so we can write SQL that works with most databases
    datasource.setConnectionInitSqls(ImmutableList.of("SET sql_mode='ANSI_QUOTES'"));

    this.dbi = new DBI(datasource);

    log.info("Configured MySQL as metadata storage");
  }

  @Override
  protected String getPayloadType()
  {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public boolean tableExists(Handle handle, String tableName)
  {
    // ensure database defaults to utf8, otherwise bail
    boolean isUtf8 = handle
                         .createQuery("SHOW VARIABLES where variable_name = 'character_set_database' and value = 'utf8'")
                         .list()
                         .size() == 1;

    if (!isUtf8) {
      throw new ISE(
          "Database default character set is not UTF-8." + System.lineSeparator()
          + "  Druid requires its MySQL database to be created using UTF-8 as default character set."
          + " If you are upgrading from Druid 0.6.x, please make all tables have been converted to utf8 and change the database default."
          + " For more information on how to convert and set the default, please refer to section on updating from 0.6.x in the Druid 0.7.1 release notes."
      );
    }

    return !handle.createQuery("SHOW tables LIKE :tableName")
                  .bind("tableName", tableName)
                  .list()
                  .isEmpty();
  }

  @Override
  protected boolean connectorIsTransientException(Throwable e)
  {
    return e instanceof MySQLTransientException
           || (e instanceof SQLException && ((SQLException) e).getErrorCode() == 1317 /* ER_QUERY_INTERRUPTED */);
  }

  @Override
  public Void insertOrUpdate(
      final String tableName,
      final String keyColumn,
      final String valueColumn,
      final String key,
      final byte[] value
  ) throws Exception
  {
    return getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(
                String.format(
                    "INSERT INTO %1$s (%2$s, %3$s) VALUES (:key, :value) ON DUPLICATE KEY UPDATE %3$s = :value",
                    tableName,
                    keyColumn,
                    valueColumn
                )
            )
                  .bind("key", key)
                  .bind("value", value)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public DBI getDBI() { return dbi; }
}
