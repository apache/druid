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

package io.druid.metadata.storage.derby;

import com.google.common.base.Supplier;
import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class DerbyConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(DerbyConnector.class);
  private static final String SERIAL_TYPE = "BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
  private final DBI dbi;

  @Inject
  public DerbyConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");

    this.dbi = new DBI(datasource);

    log.info("Configured Derby as metadata storage");
  }

  public DerbyConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      DBI dbi
  )
  {
    super(config, dbTables);
    this.dbi = dbi;
  }

  @Override
  public boolean tableExists(Handle handle, String tableName)
  {
    return !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                  .bind("tableName", tableName.toUpperCase())
                  .list()
                  .isEmpty();
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public DBI getDBI() { return dbi; }

  @Override
  protected int getStreamingFetchSize()
  {
    // Derby only supports fetch size of 1
    return 1;
  }

  @Override
  public String getValidationQuery() { return "VALUES 1"; }
}
