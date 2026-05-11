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

package org.apache.druid.server.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.QueryContext;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.QueryBlocklistRule;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class BrokerDynamicConfigTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"queryBlocklist\": [\n"
                     + "    {\n"
                     + "      \"ruleName\": \"block-wikipedia\",\n"
                     + "      \"dataSources\": [\"wikipedia\"]\n"
                     + "    }\n"
                     + "  ]\n"
                     + "}\n";

    BrokerDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                BrokerDynamicConfig.class
            )
        ),
        BrokerDynamicConfig.class
    );

    List<QueryBlocklistRule> expectedBlocklist = ImmutableList.of(
        new QueryBlocklistRule("block-wikipedia", ImmutableSet.of("wikipedia"), null, null)
    );

    Assert.assertEquals(expectedBlocklist, actual.getQueryBlocklist());
  }

  @Test
  public void testSerdeWithNullBlocklist() throws Exception
  {
    String jsonStr = "{}";

    BrokerDynamicConfig actual = mapper.readValue(jsonStr, BrokerDynamicConfig.class);
    // When no blocklist is provided, it defaults to an empty list
    Assert.assertNotNull(actual.getQueryBlocklist());
    Assert.assertTrue(actual.getQueryBlocklist().isEmpty());
  }

  @Test
  public void testSerdeWithEmptyBlocklist() throws Exception
  {
    String jsonStr = "{\"queryBlocklist\": []}";

    BrokerDynamicConfig actual = mapper.readValue(jsonStr, BrokerDynamicConfig.class);
    Assert.assertNotNull(actual.getQueryBlocklist());
    Assert.assertTrue(actual.getQueryBlocklist().isEmpty());
  }

  @Test
  public void testSerdeWithComplexBlocklist() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"queryBlocklist\": [\n"
                     + "    {\n"
                     + "      \"ruleName\": \"block-scan-queries\",\n"
                     + "      \"queryTypes\": [\"scan\"]\n"
                     + "    },\n"
                     + "    {\n"
                     + "      \"ruleName\": \"block-context\",\n"
                     + "      \"contextMatches\": {\"priority\": \"0\"}\n"
                     + "    }\n"
                     + "  ]\n"
                     + "}\n";

    BrokerDynamicConfig actual = mapper.readValue(jsonStr, BrokerDynamicConfig.class);

    Assert.assertNotNull(actual.getQueryBlocklist());
    Assert.assertEquals(2, actual.getQueryBlocklist().size());

    QueryBlocklistRule rule1 = actual.getQueryBlocklist().get(0);
    Assert.assertEquals("block-scan-queries", rule1.getRuleName());
    Assert.assertEquals(ImmutableSet.of("scan"), rule1.getQueryTypes());

    QueryBlocklistRule rule2 = actual.getQueryBlocklist().get(1);
    Assert.assertEquals("block-context", rule2.getRuleName());
    Assert.assertEquals(ImmutableMap.of("priority", "0"), rule2.getContextMatches());
  }

  @Test
  public void testSerdeWithQueryContext() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"queryContext\": {\n"
                     + "    \"priority\": 10,\n"
                     + "    \"useCache\": false\n"
                     + "  }\n"
                     + "}\n";

    BrokerDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, BrokerDynamicConfig.class)),
        BrokerDynamicConfig.class
    );

    Assert.assertEquals(QueryContext.of(ImmutableMap.of("priority", 10, "useCache", false)), actual.getQueryContext());
  }

  @Test
  public void testNullQueryContextDefaultsToEmptyMap() throws Exception
  {
    BrokerDynamicConfig actual = mapper.readValue("{}", BrokerDynamicConfig.class);
    Assert.assertNotNull(actual.getQueryContext());
    Assert.assertTrue(actual.getQueryContext().isEmpty());
  }

  @Test
  public void testSerdeWithPerSegmentTimeoutConfig() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"perSegmentTimeoutConfig\": {\n"
                     + "    \"my_large_ds\": {\"perSegmentTimeoutMs\": 5000, \"monitorOnly\": true},\n"
                     + "    \"my_other_ds\": {\"perSegmentTimeoutMs\": 3000}\n"
                     + "  }\n"
                     + "}\n";

    BrokerDynamicConfig actual = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, BrokerDynamicConfig.class)),
        BrokerDynamicConfig.class
    );

    Map<String, PerSegmentTimeoutConfig> expected = ImmutableMap.of(
        "my_large_ds", new PerSegmentTimeoutConfig(5000, true),
        "my_other_ds", new PerSegmentTimeoutConfig(3000, null)
    );
    Assert.assertEquals(expected, actual.getPerSegmentTimeoutConfig());
  }

  @Test
  public void testNullPerSegmentTimeoutConfigDefaultsToEmptyMap() throws Exception
  {
    BrokerDynamicConfig actual = mapper.readValue("{}", BrokerDynamicConfig.class);
    Assert.assertNotNull(actual.getPerSegmentTimeoutConfig());
    Assert.assertTrue(actual.getPerSegmentTimeoutConfig().isEmpty());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(BrokerDynamicConfig.class)
                  .usingGetClass()
                  .verify();
  }
}
