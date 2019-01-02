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

package org.apache.druid.extensions.watermarking.storage.sql;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.mysql.jdbc.exceptions.MySQLTransientException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.extensions.watermarking.WatermarkKeeperConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.BooleanMapper;

import javax.inject.Inject;
import java.sql.SQLException;

// this is mostly straight gaffled from MySQLConnector implementation
public class MySqlWatermarkStore extends SqlWatermarkStore
{
  private static final Logger log = new Logger(MySqlWatermarkStore.class);
  private static final String PAYLOAD_TYPE = "LONGBLOB";
  private static final String SERIAL_TYPE = "BIGINT(20) AUTO_INCREMENT";
  private static final String QUOTE_STRING = "`";

  private final DBI dbi;

  @Inject
  public MySqlWatermarkStore(
      Supplier<SqlWatermarkStoreConfig> connectorConfigSupplier,
      WatermarkKeeperConfig keeperConfig
  )
  {
    super(connectorConfigSupplier, keeperConfig);

    final BasicDataSource datasource = getDatasource();
    // MySQL driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("com.mysql.jdbc.Driver");

    // use double-quotes for quoting columns, so we can write SQL that works with most databases
    datasource.setConnectionInitSqls(ImmutableList.of("SET sql_mode='ANSI_QUOTES'"));

    this.dbi = new DBI(datasource);

    log.info("Configured MySQL as timeline watermark metadata storage");
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
  public String getQuoteString()
  {
    return QUOTE_STRING;
  }

  @Override
  protected int getStreamingFetchSize()
  {
    // this is MySQL's way of indicating you want results streamed back
    // see http://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html
    return Integer.MIN_VALUE;
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
  )
  {
    return getDBI().withHandle(
        handle -> {
          handle.createStatement(
              StringUtils.format(
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
    );
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }
}
