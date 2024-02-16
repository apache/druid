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

package org.apache.druid.metadata.storage.derby;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

@ManageLifecycle
public class DerbyConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(DerbyConnector.class);
  private static final String SERIAL_TYPE = "BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
  private static final String QUOTE_STRING = "\\\"";
  private final DBI dbi;
  private final MetadataStorage storage;

  @Inject
  public DerbyConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(config, dbTables, centralizedDatasourceSchemaConfig);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");

    this.dbi = new DBI(datasource);
    this.storage = storage;
    log.info("Derby connector instantiated with metadata storage [%s].", this.storage.getClass().getName());
  }

  public DerbyConnector(
      MetadataStorage storage,
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      DBI dbi,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(config, dbTables, centralizedDatasourceSchemaConfig);
    this.dbi = dbi;
    this.storage = storage;
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
  public String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public String getQuoteString()
  {
    return QUOTE_STRING;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  public int getStreamingFetchSize()
  {
    // Derby only supports fetch size of 1
    return 1;
  }

  @Override
  public String getValidationQuery()
  {
    return "VALUES 1";
  }

  @Override
  public String limitClause(int limit)
  {
    return String.format(Locale.ENGLISH, "FETCH NEXT %d ROWS ONLY", limit);
  }

  @Override
  public void exportTable(
      String tableName,
      String outputPath
  )
  {
    retryWithHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle)
          {
            handle.createStatement(
                StringUtils.format(
                    "CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, '%s', '%s', null, null, null)",
                    tableName,
                    outputPath
                )
            ).execute();
            return null;
          }
        }
    );
  }

  /**
   * Get the ResultSet for indexInfo for given table
   *
   * @param databaseMetaData DatabaseMetaData
   * @param tableName        Name of table
   * @return ResultSet with index info
   */
  @Override
  public ResultSet getIndexInfo(DatabaseMetaData databaseMetaData, String tableName) throws SQLException
  {
    return databaseMetaData.getIndexInfo(
        null,
        null,
        StringUtils.toUpperCase(tableName),  // tableName needs to be uppercase in derby default setting
        false,
        false
    );
  }
  /**
   * Interrogate table metadata and return true or false depending on the existance of the indicated column
   *
   * public visibility because DerbyConnector needs to override thanks to uppercase table and column names.
   *
   * @param tableName The table being interrogated
   * @param columnName The column being looked for
   * @return boolean indicating the existence of the column in question
   */
  @Override
  public boolean tableHasColumn(String tableName, String columnName)
  {
    return getDBI().withHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle)
          {
            try {
              if (tableExists(handle, tableName)) {
                DatabaseMetaData dbMetaData = handle.getConnection().getMetaData();
                ResultSet columns = dbMetaData.getColumns(
                    null,
                    null,
                    tableName.toUpperCase(Locale.ENGLISH),
                    columnName.toUpperCase(Locale.ENGLISH)
                );
                return columns.next();
              } else {
                return false;
              }
            }
            catch (SQLException e) {
              return false;
            }
          }
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    log.info("Starting DerbyConnector...");
    storage.start();
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping DerbyConnector...");
    storage.stop();
  }
}
