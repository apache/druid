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

package io.druid.server.lookup.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.lookup.DataFetcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class JdbcDataFetcherTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  Handle handle;
  
  private JdbcDataFetcher jdbcDataFetcher;
  private final String tableName = "tableName";
  private final String keyColumn = "keyColumn";
  private final String valueColumn = "valueColumn";
  
  

  private static final Map<String, String> lookupMap = ImmutableMap.of(
      "foo", "bar",
      "bad", "bar",
      "how about that", "foo",
      "empty string", ""
  );

  @Before
  public void setUp() throws InterruptedException
  {
    jdbcDataFetcher = new JdbcDataFetcher(derbyConnectorRule.getMetadataConnectorConfig(), "tableName", "keyColumn", "valueColumn",
                                          100);

    handle = derbyConnectorRule.getConnector().getDBI().open();
    Assert.assertEquals(
        0,
        handle.createStatement(
            StringUtils.format(
                "CREATE TABLE %s (%s VARCHAR(64), %s VARCHAR(64))",
                tableName,
                keyColumn,
                valueColumn
            )
        ).setQueryTimeout(1).execute()
    );
    handle.createStatement(StringUtils.format("TRUNCATE TABLE %s", tableName)).setQueryTimeout(1).execute();

    for (Map.Entry<String, String> entry : lookupMap.entrySet()) {
      insertValues(entry.getKey(), entry.getValue(), handle);
    }
    handle.commit();
  }

  @After
  public void tearDown()
  {
    handle.createStatement("DROP TABLE " + tableName).setQueryTimeout(1).execute();
    handle.close();
  }

  @Test
  public void testFetch() throws InterruptedException
  {
    Assert.assertEquals("null check", null, jdbcDataFetcher.fetch("baz"));
    assertMapLookup(lookupMap, jdbcDataFetcher);
  }

  @Test
  public void testFetchAll()
  {
    ImmutableMap.Builder<String,String> mapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry: jdbcDataFetcher.fetchAll()
         ) {
      mapBuilder.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals("maps should match", lookupMap, mapBuilder.build());
  }

  @Test
  public void testFetchKeys()
  {
    ImmutableMap.Builder<String,String> mapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry: jdbcDataFetcher.fetch(lookupMap.keySet())
        ) {
      mapBuilder.put(entry.getKey(), entry.getValue());
    }

    Assert.assertEquals(lookupMap, mapBuilder.build());
  }

  @Test
  public void testReverseFetch() throws InterruptedException
  {
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("foo", "bad"),
        Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys("bar"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("how about that"),
        Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys("foo"))
    );
    Assert.assertEquals(
        "reverse lookup should match",
        Sets.newHashSet("empty string"),
        Sets.newHashSet(jdbcDataFetcher.reverseFetchKeys(""))
    );
    Assert.assertEquals(
        "reverse lookup of none existing value should be empty list",
        Collections.EMPTY_LIST,
        jdbcDataFetcher.reverseFetchKeys("does't exist")
    );
  }

  @Test
  public void testSerDesr() throws IOException
  {
    JdbcDataFetcher jdbcDataFetcher = new JdbcDataFetcher(new MetadataStorageConnectorConfig(), "table", "keyColumn", "ValueColumn",
                                                          100);
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    String jdbcDataFetcherSer = mapper.writeValueAsString(jdbcDataFetcher);
    Assert.assertEquals(jdbcDataFetcher, mapper.reader(DataFetcher.class).readValue(jdbcDataFetcherSer));
  }

  private void assertMapLookup(Map<String, String> map, DataFetcher dataFetcher)
  {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      Assert.assertEquals("non-null check", val, dataFetcher.fetch(key));
    }
  }

  private void insertValues(final String key, final String val, Handle handle)
  {
    final String query;
    handle.createStatement(
        StringUtils.format("DELETE FROM %s WHERE %s='%s'", tableName, keyColumn, key)
    ).setQueryTimeout(1).execute();
    query = StringUtils.format(
        "INSERT INTO %s (%s, %s) VALUES ('%s', '%s')",
        tableName,
        keyColumn, valueColumn,
        key, val
    );
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
  }
}
