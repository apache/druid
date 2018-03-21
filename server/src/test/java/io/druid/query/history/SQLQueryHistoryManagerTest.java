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

package io.druid.query.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DirectDruidClient;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.TestDerbyConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SQLQueryHistoryManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private QueryHistoryManager queryHistoryManager;

  private final ObjectMapper jsonMapper;
  private final Map<String, Object> defaultContext;


  public SQLQueryHistoryManagerTest()
  {
    jsonMapper = new DefaultObjectMapper();
    defaultContext = new HashMap<>();
    defaultContext.put(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE);
    defaultContext.put(DirectDruidClient.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());
  }

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    connector.createQueryHistoryTable();
    queryHistoryManager = new SQLQueryHistoryManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        jsonMapper,
        new QueryHistoryConfig(true)
    );
  }

  @Test(timeout = 10_000L)
  public void testAddEntry()
  {
    QueryHistoryEntry entry = QueryHistoryEntry.builder()
        .queryID("ID_1")
        .type(QueryHistoryEntry.TYPE_BROKER_TIME)
        .payload(ImmutableMap.of("TEST_KEY", "TEST_VALUE").toString())
        .build();
    queryHistoryManager.addEntry(entry);
    List<QueryHistoryEntry> entries = queryHistoryManager.fetchQueryHistory();
    Assert.assertEquals(1, entries.size());
    Assert.assertEquals(entry, entries.get(0));
  }

}
