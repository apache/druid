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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.Rule;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

public class SqlTestUtils
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private final TestDerbyFirehoseConnector derbyFirehoseConnector;
  private final TestDerbyConnector derbyConnector;

  public SqlTestUtils(TestDerbyConnector derbyConnector)
  {
    this.derbyConnector = derbyConnector;
    this.derbyFirehoseConnector = new SqlTestUtils.TestDerbyFirehoseConnector(
        new MetadataStorageConnectorConfig(),
        derbyConnector.getDBI()
    );
  }

  private static class TestDerbyFirehoseConnector extends SQLFirehoseDatabaseConnector
  {
    private final DBI dbi;

    private TestDerbyFirehoseConnector(
        @JsonProperty("connectorConfig") MetadataStorageConnectorConfig metadataStorageConnectorConfig, DBI dbi
    )
    {
      final BasicDataSource datasource = getDatasource(metadataStorageConnectorConfig);
      datasource.setDriverClassLoader(getClass().getClassLoader());
      datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");
      this.dbi = dbi;
    }

    @Override
    public DBI getDBI()
    {
      return dbi;
    }
  }

  public void createAndUpdateTable(final String tableName, int numEntries)
  {
    derbyConnector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  timestamp varchar(255) NOT NULL,\n"
                + "  a VARCHAR(255) NOT NULL,\n"
                + "  b VARCHAR(255) NOT NULL\n"
                + ")",
                tableName
            )
        )
    );

    derbyConnector.getDBI().withHandle(
        (handle) -> {
          Batch batch = handle.createBatch();
          for (int i = 0; i < numEntries; i++) {
            String timestampSt = StringUtils.format("2011-01-12T00:0%s:00.000Z", i);
            batch.add(StringUtils.format("INSERT INTO %1$s (timestamp, a, b) VALUES ('%2$s', '%3$s', '%4$s')",
                                         tableName, timestampSt,
                                         i, i
            ));
          }
          batch.execute();
          return null;
        }
    );
  }

  public void dropTable(final String tableName)
  {
    derbyConnector.getDBI().withHandle(
        (HandleCallback<Void>) handle -> {
          handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                .execute();
          return null;
        }
    );
  }

  public TestDerbyFirehoseConnector getDerbyFirehoseConnector()
  {
    return derbyFirehoseConnector;
  }
}
