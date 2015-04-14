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

package io.druid.metadata.storage.mssql;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;

import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;

import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.StringMapper;

public class MsSQLConnector extends SQLMetadataConnector
{
  private static final Logger log = new Logger(MsSQLConnector.class);
  private static final String PAYLOAD_TYPE = "BYTEA";
  private static final String SERIAL_TYPE = "BIGSERIAL";

  private final DBI dbi;

  @Inject
  public MsSQLConnector(Supplier<MetadataStorageConnectorConfig> config, Supplier<MetadataStorageTablesConfig> dbTables)
  {
    super(config, dbTables);

    final BasicDataSource datasource = getDatasource();
    // MSSQL driver is classloader isolated as part of the extension
    // so we need to help jtds find the driver
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("net.sourceforge.jtds.jdbc.Driver");

    this.dbi = new DBI(datasource);
  }

  @Override
  protected String getPayloadType() {
    return PAYLOAD_TYPE;
  }

  @Override
  protected String getSerialType()
  {
    return SERIAL_TYPE;
  }

  @Override
  public boolean tableExists(final Handle handle, final String tableName)
  {
    return !handle.createQuery(
    		"select * from SYS.TABLES where name = :tableName")
                 .bind("tableName", tableName)
                 .map(StringMapper.FIRST)
                 .list()
                 .isEmpty();
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
                      "begin tran \n" +
                      "if exists (select * from %1$s with (updlock,serializable) where %2$s = :key)\n" +
                      "begin \n"+
                      "update %1$s set %3$s = :value where %2$s = :key \n" +
                      "end \n" +
                      "else \n" +
                      "begin \n" +
                      "insert %1$s (%2$s, %3$s) values (:key, :value) \n" +
                      "end \n" +
                      "commit tran",
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
