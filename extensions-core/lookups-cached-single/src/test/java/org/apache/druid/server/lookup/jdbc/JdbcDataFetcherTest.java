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

package org.apache.druid.server.lookup.jdbc;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.lookup.DataFetcher;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcDataFetcherTest extends InitializedNullHandlingTest
{
  private static final String TABLE_NAME = "tableName";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String VALUE_COLUMN = "valueColumn";

  @Nested
  public class FetchTest
  {
    private TestDerbyConnector derbyConnector;
    private MetadataStorageConnectorConfig derbyConnectorConfig;

    private Handle handle;

    private JdbcDataFetcher jdbcDataFetcher;

    private static final Map<String, String> LOOKUP_MAP = ImmutableMap.of(
        "foo", "bar",
        "bad", "bar",
        "how about that", "foo",
        "empty string", ""
    );

    @BeforeEach
    public void setUp()
    {
      derbyConnector = new TestDerbyConnector();
      derbyConnector.createDatabase();
      derbyConnectorConfig = new MetadataStorageConnectorConfig()
      {
        @Override
        public String getConnectURI()
        {
          return derbyConnector.getJdbcUri();
        }
      };
      jdbcDataFetcher = new JdbcDataFetcher(
          derbyConnectorConfig,
          "tableName",
          "keyColumn",
          "valueColumn",
          100,
          new JdbcAccessSecurityConfig()
      );

      handle = derbyConnector.getDBI().open();
      Assertions.assertEquals(
          0,
          handle.createStatement(
              StringUtils.format(
                  "CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64))",
                  TABLE_NAME,
                  KEY_COLUMN,
                  VALUE_COLUMN
              )
          ).setQueryTimeout(1).execute()
      );
      handle.createStatement(StringUtils.format("TRUNCATE TABLE %s", TABLE_NAME)).setQueryTimeout(1).execute();

      for (Map.Entry<String, String> entry : LOOKUP_MAP.entrySet()) {
        insertValues(entry.getKey(), entry.getValue(), handle);
      }
      handle.commit();
    }

    @AfterEach
    public void tearDown()
    {
      handle.createStatement("DROP TABLE " + TABLE_NAME).setQueryTimeout(1).execute();
      handle.close();
      derbyConnector.tearDown();
    }

    @Test
    public void testFetch()
    {
      Assertions.assertEquals(null, jdbcDataFetcher.fetch("baz"), "null check");
      assertMapLookup(LOOKUP_MAP, jdbcDataFetcher);
    }

    @Test
    public void testFetchAll()
    {
      ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
      jdbcDataFetcher.fetchAll().forEach(mapBuilder::put);
      Assertions.assertEquals(LOOKUP_MAP, mapBuilder.build(), "maps should match");
    }

    @Test
    public void testFetchKeys()
    {
      ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
      jdbcDataFetcher.fetch(LOOKUP_MAP.keySet()).forEach(mapBuilder::put);
      Assertions.assertEquals(LOOKUP_MAP, mapBuilder.build());
    }

    @Test
    public void testReverseFetch()
    {
      Assertions.assertEquals(
          Sets.newHashSet("foo", "bad"),
          Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys("bar")),
          "reverse lookup should match"
      );
      Assertions.assertEquals(
          Sets.newHashSet("how about that"),
          Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys("foo")),
          "reverse lookup should match"
      );
      Assertions.assertEquals(
          Sets.newHashSet("empty string"),
          Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys("")),
          "reverse lookup should match"
      );
      Assertions.assertEquals(
          Collections.emptyList(),
          jdbcDataFetcher.reverseFetchKeys("does't exist"),
          "reverse lookup of none existing value should be empty list"
      );
    }

    @Test
    public void testSerDesr() throws IOException
    {
      final JdbcAccessSecurityConfig securityConfig = new JdbcAccessSecurityConfig();
      JdbcDataFetcher jdbcDataFetcher = new JdbcDataFetcher(
          new MetadataStorageConnectorConfig(),
          "table",
          "keyColumn",
          "ValueColumn",
          100,
          securityConfig
      );
      DefaultObjectMapper mapper = new DefaultObjectMapper();
      mapper.setInjectableValues(new Std().addValue(JdbcAccessSecurityConfig.class, securityConfig));
      String jdbcDataFetcherSer = mapper.writeValueAsString(jdbcDataFetcher);
      Assertions.assertEquals(jdbcDataFetcher, mapper.readerFor(DataFetcher.class).readValue(jdbcDataFetcherSer));
    }

    @SuppressWarnings("SameParameterValue")
    private void assertMapLookup(Map<String, String> map, DataFetcher dataFetcher)
    {
      for (Map.Entry<String, String> entry : map.entrySet()) {
        String key = entry.getKey();
        String val = entry.getValue();
        Assertions.assertEquals(val, dataFetcher.fetch(key), "non-null check");
      }
    }

    private void insertValues(final String key, final String val, Handle handle)
    {
      final String query;
      handle.createStatement(
          StringUtils.format("DELETE FROM %s WHERE %s='%s'", TABLE_NAME, KEY_COLUMN, key)
      ).setQueryTimeout(1).execute();
      query = StringUtils.format(
          "INSERT INTO %s (%s, %s) VALUES ('%s', '%s')",
          TABLE_NAME,
          KEY_COLUMN, VALUE_COLUMN,
          key, val
      );
      Assertions.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
      handle.commit();
    }
  }

  @Nested
  public class MissingJdbcJarTest
  {
    private static final MetadataStorageConnectorConfig MISSING_METADATA_STORAGE_CONNECTOR_CONFIG =
        createMissingMetadataStorageConnectorConfig();
    private static final String KEY = "key";

    private JdbcDataFetcher target;

    @BeforeEach
    public void setUp()
    {
      target = new JdbcDataFetcher(
          MISSING_METADATA_STORAGE_CONNECTOR_CONFIG,
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          null,
          new JdbcAccessSecurityConfig()
      );
    }

    @Test
    public void testFetchAll()
    {
      test(() -> target.fetchAll());
    }

    @Test
    public void testFetch()
    {
      test(() -> target.fetch(Collections.singleton(KEY)));
    }

    @Test
    public void testFetchKeys()
    {
      test(() -> target.fetch(Collections.singleton(KEY)));
    }

    @Test
    public void testReverseFetch()
    {
      test(() -> target.reverseFetchKeys(KEY));
    }

    private void test(Runnable runnable)
    {
      Throwable exception = assertThrows(IllegalStateException.class, () ->

        runnable.run());
      assertTrue(exception.getMessage().contains("JDBC driver JAR files missing in the classpath"));
    }

    @SuppressWarnings("SameParameterValue")
    private static MetadataStorageConnectorConfig createMissingMetadataStorageConnectorConfig()
    {
      String type = "mydb";
      String json = "{\"connectURI\":\"jdbc:" + type + "://localhost:3306/\"}";
      try {
        return new ObjectMapper().readValue(json, MetadataStorageConnectorConfig.class);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
