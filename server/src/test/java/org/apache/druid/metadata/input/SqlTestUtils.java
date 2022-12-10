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
import com.google.common.collect.ImmutableSet;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.junit.Rule;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
      final BasicDataSource datasource = getDatasource(
          metadataStorageConnectorConfig,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("user", "create");
            }
          }
      );
      datasource.setDriverClassLoader(getClass().getClassLoader());
      datasource.setDriverClassName("org.apache.derby.jdbc.ClientDriver");
      this.dbi = dbi;
    }

    @Override
    public DBI getDBI()
    {
      return dbi;
    }

    @Override
    public Set<String> findPropertyKeysFromConnectURL(String connectUri, boolean allowUnknown)
    {
      return ImmutableSet.of("user", "create");
    }
  }

  public List<InputRow> createTableWithRows(final String tableName, int numEntries)
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

    final List<InputRow> rowsToCreate = IntStream.range(0, numEntries).mapToObj(i -> {
      final String timestamp = StringUtils.format("2011-01-12T00:%02d:00.000Z", i);
      final Map<String, Object> event = new LinkedHashMap<>();
      event.put("a", "a " + i);
      event.put("b", "b" + i);
      event.put("timestamp", timestamp);
      return new MapBasedInputRow(DateTimes.of(timestamp), Arrays.asList("timestamp", "a", "b"), event);
    }).collect(Collectors.toList());

    derbyConnector.getDBI().withHandle(
        (handle) -> {
          Batch batch = handle.createBatch();
          for (InputRow row : rowsToCreate) {
            batch.add(StringUtils.format(
                "INSERT INTO %1$s (timestamp, a, b) VALUES ('%2$s', '%3$s', '%4$s')",
                tableName, row.getTimestamp(), row.getDimension("a").get(0), row.getDimension("b").get(0)
            ));
          }
          batch.execute();
          return null;
        }
    );

    return rowsToCreate;
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

  /**
   * Builds a {@code SELECT timestamp, a, b FROM tableName} query for each of
   * the given tables.
   */
  public static List<String> selectFrom(String... tableNames)
  {
    final List<String> selects = new ArrayList<>();
    for (String tableName : tableNames) {
      selects.add(StringUtils.format("SELECT timestamp, a, b FROM %s", tableName));
    }
    return selects;
  }
}
