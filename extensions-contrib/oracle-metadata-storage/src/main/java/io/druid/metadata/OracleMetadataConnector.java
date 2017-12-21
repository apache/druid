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

package io.druid.metadata;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

public class OracleMetadataConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(OracleMetadataConnector.class);
  private static final String PAYLOAD_TYPE = "BLOB";
  private static final String SERIAL_TYPE = "NUMBER";
  private static final String QUOTE_STRING = "\"";

  private final DBI dbi;

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final Supplier<MetadataStorageTablesConfig> tablesConfigSupplier;

  @Inject
  public OracleMetadataConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    super(config, dbTables);

    this.config = config;
    this.tablesConfigSupplier = dbTables;

    final BasicDataSource datasource = getDatasource();
    // Oracle driver is classloader isolated as part of the extension
    // so we need to help JDBC find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("oracle.jdbc.OracleDriver");

    this.dbi = new DBI(datasource);

    log.info("Configured Oracle as metadata storage");
  }

  @Override
  public String getValidationQuery()
  {
    return "SELECT 1 FROM DUAL";
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
  public int getStreamingFetchSize()
  {
    return 100;
  }

  @Override
  public boolean tableExists(Handle handle, String tableName)
  {
    return !handle.createQuery("SELECT * FROM user_tables WHERE table_name = :tableName")
                  .bind("tableName", StringUtils.toUpperCase(tableName))
                  .list()
                  .isEmpty();
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  private void createAuditTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format("CREATE TABLE %1$s (\n"
                               + "  id %2$s NOT NULL,\n"
                               + "  audit_key VARCHAR2(255) NOT NULL,\n"
                               + "  type VARCHAR2(255) NOT NULL,\n"
                               + "  author VARCHAR2(255) NOT NULL,\n"
                               + "  %4$scomment%4$s VARCHAR2(2048) NOT NULL,\n"
                               + "  created_date VARCHAR2(255) NOT NULL,\n"
                               + "  payload %3$s NOT NULL,\n"
                               + "  PRIMARY KEY(id)\n"
                               + ")", tableName, getSerialType(), getPayloadType(), getQuoteString()),
            StringUtils.format("CREATE SEQUENCE %1$s_seq MINVALUE 1 START WITH 1 INCREMENT BY 1 NOCACHE", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_key_time ON %1$s(audit_key, created_date)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_type_time ON %1$s(type, created_date)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_audit_time ON %1$s(created_date)", tableName)
        )
    );
  }

  @Override
  public void createAuditTable()
  {
    if (this.config.get().isCreateTables()) {
      this.createAuditTable(this.tablesConfigSupplier.get().getAuditTable());
    }

  }

  @Override
  public void createPendingSegmentsTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(StringUtils.format("CREATE TABLE %1$s (\n"
                                            + "  id VARCHAR2(255) NOT NULL,\n"
                                            + "  dataSource VARCHAR2(255) NOT NULL,\n"
                                            + "  created_date VARCHAR2(255) NOT NULL,\n"
                                            + "  %3$sstart%3$s VARCHAR2(255) NOT NULL,\n"
                                            + "  %3$send%3$s VARCHAR2(255) NOT NULL,\n"
                                            + "  sequence_name VARCHAR2(255) NOT NULL,\n"
                                            + "  sequence_prev_id VARCHAR2(255),\n"
                                            + "  sequence_name_prev_id_sha1 VARCHAR2(255) NOT NULL,\n"
                                            + "  payload %2$s NOT NULL,\n"
                                            + "  PRIMARY KEY (id),\n"
                                            + "  UNIQUE (sequence_name_prev_id_sha1)\n"
                                            + ")", tableName, getPayloadType(), getQuoteString()))
    );
  }

  @Override
  public void createDataSourceTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(StringUtils.format("CREATE TABLE %1$s (\n"
                                            + "  dataSource VARCHAR2(255) NOT NULL,\n"
                                            + "  created_date VARCHAR2(255) NOT NULL,\n"
                                            + "  commit_metadata_payload %2$s NOT NULL,\n"
                                            + "  commit_metadata_sha1 VARCHAR2(255) NOT NULL,\n"
                                            + "  PRIMARY KEY (dataSource)\n"
                                            + ")", tableName, getPayloadType()))
    );
  }

  @Override
  public void createSegmentTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format("CREATE TABLE %1$s (\n"
                               + "  id VARCHAR2(255) NOT NULL,\n"
                               + "  dataSource VARCHAR2(255) NOT NULL,\n"
                               + "  created_date VARCHAR2(255) NOT NULL,\n"
                               + "  %3$sstart%3$s VARCHAR2(255) NOT NULL,\n"
                               + "  %3$send%3$s VARCHAR2(255) NOT NULL,\n"
                               + "  partitioned VARCHAR2(10) NOT NULL,\n"
                               + "  version VARCHAR2(255) NOT NULL,\n"
                               + "  used VARCHAR2(10) NOT NULL,\n"
                               + "  payload %2$s NOT NULL,\n"
                               + "  PRIMARY KEY (id)\n"
                               + ")", tableName, getPayloadType(), getQuoteString()),
            StringUtils.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName),
            StringUtils.format("CREATE INDEX idx_%1$s_used ON %1$s(used)", tableName)
        )
    );
  }

  @Override
  public void createRulesTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format("CREATE TABLE %1$s (\n"
                               + "  id VARCHAR2(255) NOT NULL,\n"
                               + "  dataSource VARCHAR2(255) NOT NULL,\n"
                               + "  version VARCHAR2(255) NOT NULL,\n"
                               + "  payload %2$s NOT NULL,\n"
                               + "  PRIMARY KEY (id)\n"
                               + ")", tableName, getPayloadType()),
            StringUtils.format("CREATE INDEX idx_%1$s_datasource ON %1$s(dataSource)", tableName)
        )
    );
  }

  @Override
  public void createConfigTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(StringUtils.format("CREATE TABLE %1$s (\n"
                                            + "  name VARCHAR2(255) NOT NULL,\n"
                                            + "  payload %2$s NOT NULL,\n"
                                            + "  PRIMARY KEY(name)\n"
                                            + ")", tableName, getPayloadType()))
    );
  }

  @Override
  public void createEntryTable(final String tableName)
  {
    createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format("CREATE TABLE %1$s (\n"
                               + "  id VARCHAR2(255) NOT NULL,\n"
                               + "  created_date VARCHAR2(255) NOT NULL,\n"
                               + "  datasource VARCHAR2(255) NOT NULL,\n"
                               + "  payload %2$s NOT NULL,\n"
                               + "  status_payload %2$s NOT NULL,\n"
                               + "  active VARCHAR2(10) DEFAULT 'false' NOT NULL,\n"
                               + "  PRIMARY KEY (id)\n"
                               + ")", tableName, getPayloadType()),
            StringUtils.format(
                "CREATE INDEX idx_%1$s_active_cdate ON %1$s(active, created_date)",
                tableName
            )
        )
    );
  }

  @Override
  public void createLogTable(final String tableName, final String entryTypeName)
  {
    createTable(tableName, ImmutableList.of(
        StringUtils.format("CREATE TABLE %1$s (\n"
                           + "  id %2$s NOT NULL,\n"
                           + "  %4$s_id VARCHAR2(255) DEFAULT NULL,\n"
                           + "  log_payload %3$s,\n"
                           + "  PRIMARY KEY (id)\n"
                           + ")",
                           tableName, getSerialType(), getPayloadType(), entryTypeName
        ),
        StringUtils.format("CREATE SEQUENCE %1$s_seq MINVALUE 1 START WITH 1 INCREMENT BY 1 NOCACHE", tableName),
        StringUtils.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
    ));
  }

  @Override
  public void createLockTable(final String tableName, final String entryTypeName)
  {
    createTable(tableName, ImmutableList.of(
        StringUtils.format("CREATE TABLE %1$s (\n"
                           + "  id %2$s NOT NULL,\n"
                           + "  %4$s_id VARCHAR2(255) DEFAULT NULL,\n"
                           + "  lock_payload %3$s,\n"
                           + "  PRIMARY KEY (id)\n"
                           + ")",
                           tableName, getSerialType(), getPayloadType(), entryTypeName
        ),
        StringUtils.format("CREATE SEQUENCE %1$s_seq MINVALUE 1 START WITH 1 INCREMENT BY 1 NOCACHE", tableName),
        StringUtils.format("CREATE INDEX idx_%1$s_%2$s_id ON %1$s(%2$s_id)", tableName, entryTypeName)
    ));
  }

  @Override
  public void createSupervisorsTable(final String tableName)
  {
    createTable(tableName, ImmutableList.of(
        StringUtils.format("CREATE TABLE %1$s (\n"
                           + "  id %2$s NOT NULL,\n"
                           + "  spec_id VARCHAR2(255) NOT NULL,\n"
                           + "  created_date VARCHAR2(255) NOT NULL,\n"
                           + "  payload %3$s NOT NULL,\n"
                           + "  PRIMARY KEY (id)\n" + ")",
                           tableName, getSerialType(), getPayloadType()
        ),
        StringUtils.format("CREATE SEQUENCE %1$s_seq MINVALUE 1 START WITH 1 INCREMENT BY 1 NOCACHE", tableName),
        StringUtils.format("CREATE INDEX idx_%1$s_spec_id ON %1$s(spec_id)", tableName)
    ));
  }
}
